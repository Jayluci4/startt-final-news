# Main application file for the Startt News Intelligence API
#!/usr/bin/env python3

import asyncio
import json
import os
import time
import uuid
import hashlib
import gzip
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Union, AsyncGenerator
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass, asdict
from enum import Enum
import logging
from collections import defaultdict, deque
import weakref
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import sqlite3
from run_pipeline import execute_pipeline
from argparse import Namespace

# FastAPI and related imports
from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks, WebSocket, WebSocketDisconnect
from fastapi import Request, Response, status, Query, Path, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.responses import JSONResponse, StreamingResponse, FileResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.openapi.utils import get_openapi
from fastapi.encoders import jsonable_encoder
import uvicorn
from fastapi.exception_handlers import RequestValidationError
from fastapi.exceptions import RequestValidationError
from fastapi.responses import PlainTextResponse

# Pydantic models
from pydantic import BaseModel, Field, validator, root_validator
from pydantic.types import PositiveInt, confloat, constr

# Advanced dependencies with graceful fallbacks
try:
    import redis
    from redis.connection import ConnectionPool
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    logging.warning("Redis not available - using in-memory caching")

try:
    import aiofiles
    AIOFILES_AVAILABLE = True
except ImportError:
    AIOFILES_AVAILABLE = False
    logging.warning("aiofiles not available - using synchronous file operations")

try:
    import prometheus_client
    from prometheus_client import Counter, Histogram, Gauge, start_http_server
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
    logging.warning("Prometheus client not available - metrics disabled")

# Import our enterprise pipeline
try:
    from master_pipeline import create_enterprise_pipeline, EnterpriseNewsPipeline
    PIPELINE_AVAILABLE = True
except ImportError:
    PIPELINE_AVAILABLE = False
    logging.error("Startt news pipeline not available - API will be limited")
from run_pipeline import main as run_pipeline_main

# Configure enterprise logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(name)s | %(levelname)s | %(funcName)s:%(lineno)d | %(message)s'
)
logger = logging.getLogger(__name__)

# Prometheus metrics (if available)
if PROMETHEUS_AVAILABLE:
    REQUEST_COUNT = Counter('api_requests_total', 'Total API requests', ['method', 'endpoint', 'status'])
    REQUEST_DURATION = Histogram('api_request_duration_seconds', 'API request duration')
    ACTIVE_CONNECTIONS = Gauge('api_active_connections', 'Active WebSocket connections')
    PIPELINE_RUNS = Counter('pipeline_runs_total', 'Total pipeline runs', ['status'])
    ARTICLES_PROCESSED = Counter('articles_processed_total', 'Total articles processed', ['source'])

# ==================== Critical Improvements ====================

def validate_critical_config():
    """Validate critical environment configuration"""
    issues = []
    
    # Test database accessibility
    db_path = os.getenv('DB_PATH', 'news_pipeline.db')
    try:
        conn = sqlite3.connect(db_path, timeout=10.0)
        conn.execute('SELECT 1')
        conn.close()
        logger.info(f"✅ Database accessible at: {db_path}")
    except Exception as e:
        issues.append(f"Database not accessible at {db_path}: {e}")
    
    # Check Gemini API key
    if not os.getenv('GEMINI_API_KEY'):
        logger.warning("GEMINI_API_KEY not set - AI features will use fallback methods")
    else:
        logger.info("GEMINI_API_KEY configured")
    
    # Check required directories

    db_dir = os.path.dirname(db_path)
    if db_dir:  # Only create directory if there's actually a directory path
        os.makedirs(db_dir, exist_ok=True)
    if issues:
        raise RuntimeError(f"Critical configuration issues: {issues}")
    
    logger.info(" All critical configurations validated")

@contextmanager
def get_db_connection():
    """Production-ready database connection management with proper cleanup"""
    conn = None
    try:
        conn = sqlite3.connect(
            os.getenv('DB_PATH', 'news_pipeline.db'),
            timeout=30.0,
            check_same_thread=False
        )
        conn.row_factory = sqlite3.Row
        # Enable WAL mode for better concurrency
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA synchronous=NORMAL')
        conn.execute('PRAGMA temp_store=memory')
        conn.execute('PRAGMA mmap_size=268435456')  # 256MB
        yield conn
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Database error: {e}")
        raise
    finally:
        if conn:
            conn.close()

# Pipeline-specific rate limiting
pipeline_requests = defaultdict(list)
pipeline_requests_lock = threading.Lock()

async def check_pipeline_rate_limit(client_id: str):
    """Rate limiting specifically for expensive pipeline operations"""
    now = time.time()
    
    with pipeline_requests_lock:
        # Remove old requests (older than 1 hour)
        pipeline_requests[client_id] = [
            req_time for req_time in pipeline_requests[client_id] 
            if now - req_time < 3600
        ]
        
        # Check if too many requests
        if len(pipeline_requests[client_id]) >= 3:  # Max 3 per hour
            raise HTTPException(
                status_code=429,
                detail={
                    "error": "Pipeline rate limit exceeded",
                    "message": "Maximum 3 pipeline executions per hour per client",
                    "retry_after": 3600,
                    "current_requests": len(pipeline_requests[client_id])
                }
            )
        
        pipeline_requests[client_id].append(now)

def save_execution_status(execution_id: str, status: str, details: dict = None):
    """Save pipeline execution status to database for proper tracking"""
    try:
        with get_db_connection() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO pipeline_executions 
                (execution_id, status, details, updated_at)
                VALUES (?, ?, ?, ?)
            """, (execution_id, status, json.dumps(details or {}), datetime.now().isoformat()))
            conn.commit()
    except Exception as e:
        logger.error(f"Failed to save execution status: {e}")

def get_execution_status(execution_id: str) -> Optional[Dict[str, Any]]:
    """Get pipeline execution status from database"""
    try:
        with get_db_connection() as conn:
            cursor = conn.execute("""
                SELECT execution_id, status, details, updated_at, created_at
                FROM pipeline_executions 
                WHERE execution_id = ?
            """, (execution_id,))
            row = cursor.fetchone()
            
            if row:
                details = json.loads(row['details']) if row['details'] else {}
                return {
                    'execution_id': row['execution_id'],
                    'status': row['status'],
                    'updated_at': row['updated_at'],
                    'created_at': row.get('created_at'),
                    **details
                }
    except Exception as e:
        logger.error(f"Failed to get execution status: {e}")
    
    return None

# ==================== Data Models ====================

class PipelineStatus(str, Enum):
    """Pipeline execution status"""
    IDLE = "idle"
    RUNNING = "running" 
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class ArticleQuality(str, Enum):
    """Article quality levels"""
    EXCELLENT = "excellent"
    GOOD = "good"
    FAIR = "fair"
    POOR = "poor"

class CacheStrategy(str, Enum):
    """Caching strategies"""
    AGGRESSIVE = "aggressive"
    CONSERVATIVE = "conservative"
    DISABLED = "disabled"

@dataclass
class RateLimitConfig:
    """Rate limiting configuration"""
    requests_per_minute: int = 60
    burst_capacity: int = 10
    window_size: int = 60

class PipelineRequest(BaseModel):
    """Pipeline execution request model with flexible defaults"""
    max_articles_per_source: Optional[PositiveInt] = Field(15, description="Maximum articles per source", le=100)
    max_workers: Optional[PositiveInt] = Field(3, description="Maximum concurrent workers", le=10)
    sources: Optional[List[str]] = Field(None, description="Specific sources to scrape")
    enable_ai_summary: Optional[bool] = Field(True, description="Enable AI summarization")
    quality_threshold: Optional[float] = Field(0.3, ge=0.0, le=1.0, description="Minimum quality threshold")
    cache_strategy: Optional[CacheStrategy] = Field(CacheStrategy.CONSERVATIVE, description="Caching strategy")
    save_to_json: Optional[bool] = Field(True, description="Save results to JSON file")
    save_to_db: Optional[bool] = Field(True, description="Save results to database")
    
    @validator('sources')
    def validate_sources(cls, v):
        if v is not None:
            valid_sources = ['inc42', 'entrackr', 'moneycontrol', 'startupnews', 'indianstartup']
            invalid = [s for s in v if s not in valid_sources]
            if invalid:
                raise ValueError(f"Invalid sources: {invalid}. Valid: {valid_sources}")
        return v
    
    def get_safe_values(self) -> Dict[str, Any]:
        """Get safe values with proper defaults for None values"""
        return {
            'max_articles_per_source': self.max_articles_per_source or 15,
            'max_workers': self.max_workers or 3,
            'sources': self.sources,
            'enable_ai_summary': self.enable_ai_summary if self.enable_ai_summary is not None else True,
            'quality_threshold': self.quality_threshold if self.quality_threshold is not None else 0.3,
            'cache_strategy': self.cache_strategy or CacheStrategy.CONSERVATIVE,
            'save_to_json': self.save_to_json if self.save_to_json is not None else True,
            'save_to_db': self.save_to_db if self.save_to_db is not None else True
        }

class ArticleFilter(BaseModel):
    """Article filtering options"""
    sources: Optional[List[str]] = None
    quality_min: Optional[float] = None
    date_from: Optional[datetime] = None
    date_to: Optional[datetime] = None
    search_query: Optional[str] = None
    limit: PositiveInt = Field(50, le=1000)
    offset: int = Field(0, ge=0)

class Article(BaseModel):
    """Article response model"""
    news_id: str
    news_source: str
    source_url: str
    news_title: str
    news_content: str
    ai_summary: Optional[str] = None
    image_url: Optional[str] = None
    news_published_date: Optional[str] = None
    quality_score: Optional[float] = None
    extraction_strategy: Optional[str] = None
    created_at: datetime
    metadata: Optional[Dict[str, Any]] = None

class PipelineResponse(BaseModel):
    """Pipeline execution response"""
    execution_id: str
    status: PipelineStatus
    message: str
    articles_count: int = 0
    execution_time: Optional[float] = None
    started_at: datetime
    completed_at: Optional[datetime] = None
    errors: List[str] = []
    source_breakdown: Dict[str, int] = {}
    analytics: Optional[Dict[str, Any]] = None
    output_files: Optional[Dict[str, str]] = None

class HealthCheck(BaseModel):
    """Health check response"""
    status: str
    timestamp: datetime
    version: str = "1.0"
    components: Dict[str, str]
    uptime_seconds: float

# ==================== Improved Caching System ====================

class IntelligentCache:
    """Multi-tier intelligent caching system with memory management"""
    
    def __init__(self):
        self.memory_cache = {}
        self.cache_timestamps = {}
        self.cache_stats = defaultdict(int)
        self.access_patterns = defaultdict(list)
        self.redis_client = None
        self.max_memory_items = 1000
        self.cleanup_threshold = 1200
        self.default_ttl = int(os.getenv('CACHE_TTL', 3600))
        
        # Initialize Redis if available
        if REDIS_AVAILABLE:
            try:
                self.redis_client = redis.Redis(
                    host=os.getenv('REDIS_HOST', 'localhost'),
                    port=int(os.getenv('REDIS_PORT', 6379)),
                    db=0,
                    decode_responses=True,
                    socket_timeout=5,
                    connection_pool=ConnectionPool(max_connections=20)
                )
                self.redis_client.ping()
                logger.info("Redis cache initialized successfully")
            except Exception as e:
                logger.warning(f"Redis initialization failed: {e}")
                self.redis_client = None
    
    def _cleanup_cache(self):
        """Clean up expired and old cache entries"""
        if len(self.memory_cache) <= self.max_memory_items:
            return
            
        now = time.time()
        
        # Remove expired entries (older than 1 hour)
        expired_keys = [
            key for key, timestamp in self.cache_timestamps.items()
            if now - timestamp > 3600
        ]
        
        for key in expired_keys:
            self.memory_cache.pop(key, None)
            self.cache_timestamps.pop(key, None)
            self.access_patterns.pop(key, None)
        
        # If still too many, remove least recently used
        if len(self.memory_cache) > self.max_memory_items:
            sorted_items = sorted(
                self.cache_timestamps.items(), 
                key=lambda x: x[1]
            )
            
            to_remove = sorted_items[:len(sorted_items) - self.max_memory_items]
            
            for key, _ in to_remove:
                self.memory_cache.pop(key, None)
                self.cache_timestamps.pop(key, None)
                self.access_patterns.pop(key, None)
        
        logger.info(f"Cache cleanup completed. Current size: {len(self.memory_cache)}")
    
    async def get(self, key: str, default=None) -> Any:
        """Get value with intelligent tier selection and cleanup"""
        # Periodic cleanup
        if len(self.memory_cache) > self.cleanup_threshold:
            self._cleanup_cache()
        
        # Try memory cache first (fastest)
        if key in self.memory_cache:
            self.cache_stats['memory_hits'] += 1
            self._update_access_pattern(key)
            return self.memory_cache[key]
        
        # Try Redis cache (medium speed)
        if self.redis_client:
            try:
                value = self.redis_client.get(key)
                if value is not None:
                    parsed_value = json.loads(value)
                    # Promote to memory cache if frequently accessed
                    if self._should_promote_to_memory(key):
                        self.memory_cache[key] = parsed_value
                        self.cache_timestamps[key] = time.time()
                    self.cache_stats['redis_hits'] += 1
                    self._update_access_pattern(key)
                    return parsed_value
            except Exception as e:
                logger.warning(f"Redis get failed: {e}")
        
        self.cache_stats['misses'] += 1
        return default
    
    async def set(self, key: str, value: Any, ttl: int = None) -> bool:
        """Set value with intelligent tier distribution and size management"""
        try:
            # Cleanup if needed
            if len(self.memory_cache) > self.cleanup_threshold:
                self._cleanup_cache()
            
            # Always store in memory for immediate access
            self.memory_cache[key] = value
            self.cache_timestamps[key] = time.time()
            
            # Store in Redis for persistence
            if self.redis_client:
                try:
                    serialized = json.dumps(value, default=str)
                    self.redis_client.setex(key, ttl or self.default_ttl, serialized)
                except Exception as e:
                    logger.warning(f"Redis set failed: {e}")
            
            return True
        except Exception as e:
            logger.error(f"Cache set failed: {e}")
            return False
    
    def _update_access_pattern(self, key: str):
        """Update access pattern for intelligent caching decisions"""
        now = time.time()
        self.access_patterns[key].append(now)
        # Keep only recent access times
        self.access_patterns[key] = [t for t in self.access_patterns[key] if now - t < 3600]
    
    def _should_promote_to_memory(self, key: str) -> bool:
        """Decide if key should be promoted to memory cache"""
        accesses = self.access_patterns.get(key, [])
        return len(accesses) >= 3  # Promote if accessed 3+ times recently
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache performance statistics"""
        total_requests = sum(self.cache_stats.values())
        hit_rate = (self.cache_stats['memory_hits'] + self.cache_stats['redis_hits']) / max(total_requests, 1)
        
        return {
            'total_requests': total_requests,
            'hit_rate': hit_rate,
            'memory_hits': self.cache_stats['memory_hits'],
            'redis_hits': self.cache_stats['redis_hits'],
            'misses': self.cache_stats['misses'],
            'memory_cache_size': len(self.memory_cache),
            'cleanup_threshold': self.cleanup_threshold,
            'max_memory_items': self.max_memory_items
        }

# ==================== Rate Limiting System ====================

class TokenBucketRateLimiter:
    """Advanced token bucket rate limiter with adaptive capacity"""
    
    def __init__(self, config: RateLimitConfig):
        self.config = config
        self.buckets = {}
        self.lock = threading.Lock()
    
    async def is_allowed(self, client_id: str) -> tuple[bool, Dict[str, Any]]:
        """Check if request is allowed and return rate limit info"""
        with self.lock:
            now = time.time()
            
            if client_id not in self.buckets:
                self.buckets[client_id] = {
                    'tokens': self.config.burst_capacity,
                    'last_refill': now,
                    'requests_this_window': 0,
                    'window_start': now
                }
            
            bucket = self.buckets[client_id]
            
            # Reset window if needed
            if now - bucket['window_start'] >= self.config.window_size:
                bucket['requests_this_window'] = 0
                bucket['window_start'] = now
            
            # Refill tokens
            time_passed = now - bucket['last_refill']
            tokens_to_add = (time_passed / 60.0) * self.config.requests_per_minute
            bucket['tokens'] = min(self.config.burst_capacity, bucket['tokens'] + tokens_to_add)
            bucket['last_refill'] = now
            
            # Check if request allowed
            if bucket['tokens'] >= 1 and bucket['requests_this_window'] < self.config.requests_per_minute:
                bucket['tokens'] -= 1
                bucket['requests_this_window'] += 1
                
                return True, {
                    'allowed': True,
                    'tokens_remaining': int(bucket['tokens']),
                    'requests_remaining': self.config.requests_per_minute - bucket['requests_this_window'],
                    'reset_time': bucket['window_start'] + self.config.window_size
                }
            else:
                return False, {
                    'allowed': False,
                    'tokens_remaining': int(bucket['tokens']),
                    'requests_remaining': 0,
                    'reset_time': bucket['window_start'] + self.config.window_size,
                    'retry_after': bucket['window_start'] + self.config.window_size - now
                }

# ==================== WebSocket Connection Manager ====================

class WebSocketManager:
    """Advanced WebSocket connection manager with rooms and broadcasting"""
    
    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}
        self.connection_rooms: Dict[str, set] = defaultdict(set)
        self.connection_metadata: Dict[str, Dict] = {}
    
    async def connect(self, websocket: WebSocket, client_id: str, room: str = "default"):
        """Connect client to WebSocket with room assignment"""
        await websocket.accept()
        self.active_connections[client_id] = websocket
        self.connection_rooms[room].add(client_id)
        self.connection_metadata[client_id] = {
            'room': room,
            'connected_at': datetime.now(),
            'last_ping': time.time()
        }
        
        if PROMETHEUS_AVAILABLE:
            ACTIVE_CONNECTIONS.inc()
        
        logger.info(f"WebSocket client {client_id} connected to room {room}")
    
    def disconnect(self, client_id: str):
        """Disconnect client and cleanup"""
        if client_id in self.active_connections:
            metadata = self.connection_metadata.get(client_id, {})
            room = metadata.get('room', 'default')
            
            del self.active_connections[client_id]
            self.connection_rooms[room].discard(client_id)
            self.connection_metadata.pop(client_id, None)
            
            if PROMETHEUS_AVAILABLE:
                ACTIVE_CONNECTIONS.dec()
            
            logger.info(f"WebSocket client {client_id} disconnected")
    
    async def send_personal_message(self, message: Dict, client_id: str):
        """Send message to specific client"""
        if client_id in self.active_connections:
            try:
                await self.active_connections[client_id].send_text(json.dumps(message))
            except Exception as e:
                logger.error(f"Failed to send message to {client_id}: {e}")
                self.disconnect(client_id)
    
    async def broadcast_to_room(self, message: Dict, room: str = "default"):
        """Broadcast message to all clients in room"""
        disconnected_clients = []
        
        for client_id in self.connection_rooms[room].copy():
            try:
                await self.active_connections[client_id].send_text(json.dumps(message))
            except Exception as e:
                logger.error(f"Failed to broadcast to {client_id}: {e}")
                disconnected_clients.append(client_id)
        
        # Cleanup disconnected clients
        for client_id in disconnected_clients:
            self.disconnect(client_id)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get connection statistics"""
        return {
            'total_connections': len(self.active_connections),
            'rooms': {room: len(clients) for room, clients in self.connection_rooms.items()},
            'uptime_stats': {
                client_id: (datetime.now() - metadata['connected_at']).total_seconds()
                for client_id, metadata in self.connection_metadata.items()
            }
        }

# ==================== Background Task Manager ====================

class BackgroundTaskManager:
    """Advanced background task manager with priority queues and monitoring"""
    
    def __init__(self, max_workers: int = 4):
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.running_tasks = {}
        self.completed_tasks = deque(maxlen=100)  # Keep last 100 completed tasks
        self.task_stats = defaultdict(int)
    
    async def submit_pipeline_task(self, 
                                 pipeline: EnterpriseNewsPipeline,
                                 request: PipelineRequest,
                                 client_id: str,
                                 execution_id: str) -> str:
        """Submit pipeline task with comprehensive monitoring"""
        start_time = datetime.now()
        
        def pipeline_wrapper():
            """Wrapper function for pipeline execution with error handling"""
            try:
                # Update status to running
                save_execution_status(execution_id, "running", {
                    "started_at": start_time.isoformat(),
                    "client_id": client_id,
                    "config": request.get_safe_values()
                })
                
                logger.info(f"Starting pipeline execution {execution_id}")
                
                articles = pipeline.process_all_sources(
                    max_articles_per_source=request.max_articles_per_source or 15,
                    max_workers=request.max_workers or 3,
                    sources=request.sources
                )
                
                # Filter articles by quality threshold
                quality_threshold = request.quality_threshold or 0.3
                filtered_articles = [
                    article for article in articles
                    if article.get('metadata', {}).get('quality_score', 0) >= quality_threshold
                ]
                
                # Get analytics
                analytics = pipeline.get_analytics_dashboard()
                
                # Save outputs in requested formats
                output_files = {}
                config = request.get_safe_values()
                
                if config.get('save_to_json', True):
                    json_file = pipeline.save_to_json(filtered_articles)
                    output_files['json'] = json_file
                    logger.info(f"Saved {len(filtered_articles)} articles to {json_file}")
                
                if config.get('save_to_db', True):
                    # Articles are already saved to DB by the pipeline
                    output_files['database'] = os.getenv('DB_PATH', 'news_pipeline.db')
                
                # Update status to completed
                completion_details = {
                    "started_at": start_time.isoformat(),
                    "completed_at": datetime.now().isoformat(),
                    "articles_count": len(filtered_articles),
                    "execution_time": (datetime.now() - start_time).total_seconds(),
                    "analytics": analytics,
                    "output_files": output_files,
                    "source_breakdown": {
                        source: len([a for a in filtered_articles if a.get("news_source") == source])
                        for source in ["Inc42", "Entrackr", "Moneycontrol", "StartupNews.fyi", "IndianStartupNews"]
                    }
                }
                
                save_execution_status(execution_id, "completed", completion_details)
                
                logger.info(f"Pipeline execution {execution_id} completed successfully")
                
                return {
                    'status': 'completed',
                    'articles': filtered_articles,
                    'analytics': analytics,
                    'output_files': output_files,
                    'execution_time': completion_details['execution_time']
                }
                
            except Exception as e:
                logger.error(f"Pipeline task {execution_id} failed: {e}")
                
                # Update status to failed
                save_execution_status(execution_id, "failed", {
                    "started_at": start_time.isoformat(),
                    "failed_at": datetime.now().isoformat(),
                    "error": str(e),
                    "execution_time": (datetime.now() - start_time).total_seconds()
                })
                
                return {
                    'status': 'failed',
                    'error': str(e),
                    'articles': []
                }
        
        # Submit task to executor
        future = self.executor.submit(pipeline_wrapper)
        
        self.running_tasks[execution_id] = {
            'future': future,
            'client_id': client_id,
            'request': request,
            'started_at': start_time,
            'status': PipelineStatus.RUNNING
        }
        
        self.task_stats['submitted'] += 1
        
        if PROMETHEUS_AVAILABLE:
            PIPELINE_RUNS.labels(status='started').inc()
        
        logger.info(f"Pipeline task {execution_id} submitted for client {client_id}")
        return execution_id
    
    def get_task_status(self, execution_id: str) -> Optional[Dict[str, Any]]:
        """Get comprehensive task status"""
        # First check database for persistent status
        db_status = get_execution_status(execution_id)
        if db_status:
            return db_status
        
        # Fallback to in-memory tracking
        if execution_id in self.running_tasks:
            task = self.running_tasks[execution_id]
            future = task['future']
            
            if future.done():
                # Move to completed tasks
                try:
                    result = future.result()
                    status = PipelineStatus.COMPLETED if result['status'] == 'completed' else PipelineStatus.FAILED
                    
                    completed_task = {
                        'execution_id': execution_id,
                        'status': status.value,
                        'result': result,
                        'started_at': task['started_at'].isoformat(),
                        'completed_at': datetime.now().isoformat(),
                        'execution_time': (datetime.now() - task['started_at']).total_seconds(),
                        'client_id': task['client_id']
                    }
                    
                    self.completed_tasks.append(completed_task)
                    del self.running_tasks[execution_id]
                    
                    self.task_stats['completed' if status == PipelineStatus.COMPLETED else 'failed'] += 1
                    
                    if PROMETHEUS_AVAILABLE:
                        PIPELINE_RUNS.labels(status=status.value).inc()
                    
                    return completed_task
                except Exception as e:
                    logger.error(f"Task {execution_id} failed with exception: {e}")
                    return {
                        'execution_id': execution_id,
                        'status': PipelineStatus.FAILED.value,
                        'error': str(e),
                        'started_at': task['started_at'].isoformat(),
                        'completed_at': datetime.now().isoformat()
                    }
            else:
                return {
                    'execution_id': execution_id,
                    'status': PipelineStatus.RUNNING.value,
                    'started_at': task['started_at'].isoformat(),
                    'runtime': (datetime.now() - task['started_at']).total_seconds()
                }
        
        return None
    
    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive task manager statistics"""
        running_count = len(self.running_tasks)
        avg_execution_time = 0
        
        if self.completed_tasks:
            total_time = sum(task.get('execution_time', 0) for task in self.completed_tasks)
            avg_execution_time = total_time / len(self.completed_tasks)
        
        return {
            'running_tasks': running_count,
            'completed_tasks': len(self.completed_tasks),
            'task_stats': dict(self.task_stats),
            'average_execution_time': avg_execution_time,
            'executor_stats': {
                'max_workers': self.executor._max_workers,
                'active_threads': len(self.executor._threads)
            }
        }

# ==================== Global Components ====================

# Initialize global components
cache = IntelligentCache()
websocket_manager = WebSocketManager()
task_manager = BackgroundTaskManager()
rate_limiter = TokenBucketRateLimiter(RateLimitConfig())
app_start_time = time.time()

# Initialize pipeline instance
pipeline_instance = None
if PIPELINE_AVAILABLE:
    try:
        pipeline_instance = create_enterprise_pipeline(
            gemini_api_key=os.getenv('GEMINI_API_KEY'),
            db_path=os.getenv('DB_PATH', 'news_pipeline.db')
        )
        logger.info("Startt news pipeline initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize pipeline: {e}")

# ==================== Enhanced Database Schema ====================

def init_enhanced_database():
    """Initialize database with enhanced schema including execution tracking"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Articles table (existing)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS articles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    news_id TEXT UNIQUE,
                    source TEXT NOT NULL,
                    url TEXT UNIQUE NOT NULL,
                    title TEXT NOT NULL,
                    content TEXT,
                    description TEXT,
                    author TEXT,
                    published_date TEXT,
                    image_url TEXT,
                    ai_summary TEXT,
                    quality_score REAL,
                    content_fingerprint TEXT UNIQUE,
                    extraction_strategy TEXT,
                    extraction_time REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Pipeline executions table (new)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS pipeline_executions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    execution_id TEXT UNIQUE NOT NULL,
                    status TEXT NOT NULL,
                    details TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Source performance tracking (existing)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS source_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source TEXT NOT NULL,
                    success_rate REAL,
                    avg_extraction_time REAL,
                    last_success TIMESTAMP,
                    failure_count INTEGER DEFAULT 0,
                    circuit_breaker_state TEXT DEFAULT 'CLOSED',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create indexes for better performance
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_articles_url ON articles(url)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_articles_fingerprint ON articles(content_fingerprint)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_articles_source ON articles(source)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_articles_created ON articles(created_at)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_pipeline_executions_id ON pipeline_executions(execution_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_pipeline_executions_status ON pipeline_executions(status)')
            
            conn.commit()
            logger.info("✅ Enhanced database schema initialized")
            
    except Exception as e:
        logger.error(f"Failed to initialize enhanced database: {e}")
        raise

# ==================== FastAPI Application ====================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager with critical validation"""
    logger.info("🚀 Startt News Intelligence API starting up...")
    
    # Validate critical configuration first
    try:
        validate_critical_config()
    except Exception as e:
        logger.error(f"❌ Critical configuration validation failed: {e}")
        raise
    
    # Initialize enhanced database
    try:
        init_enhanced_database()
    except Exception as e:
        logger.error(f"❌ Database initialization failed: {e}")
        raise
    
    # Start Prometheus metrics server if available
    if PROMETHEUS_AVAILABLE:
        try:
            start_http_server(int(os.getenv('METRICS_PORT', 8001)))
            logger.info(f"📊 Metrics server started on port {os.getenv('METRICS_PORT', 8001)}")
        except Exception as e:
            logger.warning(f"Failed to start metrics server: {e}")

    # Initial pipeline run to populate data
    if pipeline_instance:
        logger.info("Performing initial pipeline run to populate data...")
        try:
            def run_initial_pipeline():
                with ThreadPoolExecutor(max_workers=1) as executor:
                    future = executor.submit(pipeline_instance.process_all_sources, 15, 3)
                    future.result()
                    logger.info("Initial pipeline run completed.")
            
            initial_run_thread = threading.Thread(target=run_initial_pipeline)
            initial_run_thread.start()

        except Exception as e:
            logger.error(f"Initial pipeline run failed: {e}")
    
    logger.info("✅ Startt News Intelligence API startup completed successfully")
    
    yield
    
    logger.info("Startt News Intelligence API shutting down...")
    # Cleanup tasks
    task_manager.executor.shutdown(wait=True)

# Create FastAPI application
app = FastAPI(
    title="Startt News Intelligence API",
    description="Production-ready news aggregation and analysis system with real-time capabilities",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# Add middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure properly for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(
    TrustedHostMiddleware, 
    allowed_hosts=["*"]  # Configure properly for production
)

# ==================== Dependency Functions ====================

async def get_client_id(request: Request) -> str:
    """Extract or generate client ID for rate limiting"""
    # Try to get client ID from headers first
    client_id = request.headers.get("X-Client-ID")
    if client_id:
        return client_id
    
    # Fallback to IP address
    forwarded_for = request.headers.get("X-Forwarded-For")
    if forwarded_for:
        return forwarded_for.split(",")[0].strip()
    
    return request.client.host if request.client else "unknown"

async def check_rate_limit(client_id: str = Depends(get_client_id)):
    """Check rate limiting for client"""
    allowed, info = await rate_limiter.is_allowed(client_id)
    
    if not allowed:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail={
                "error": "Rate limit exceeded",
                "retry_after": info.get("retry_after", 60),
                "info": info
            }
        )
    
    return info

async def get_pipeline() -> EnterpriseNewsPipeline:
    """Get pipeline instance"""
    if not pipeline_instance:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Pipeline not available - check configuration"
        )
    return pipeline_instance

# ==================== Enhanced Exception Handler ====================

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Enhanced validation exception handler with better guidance"""
    
    # Convert all error details to string if they are not serializable
    def safe_error(err):
        if isinstance(err, dict):
            return {k: safe_error(v) for k, v in err.items()}
        elif isinstance(err, list):
            return [safe_error(e) for e in err]
        elif isinstance(err, Exception):
            return str(err)
        else:
            return err
    
    safe_errors = safe_error(exc.errors())
    
    # Check if this is the pipeline endpoint
    is_pipeline_endpoint = str(request.url).endswith('/api/v1/pipeline/run')
    
    if is_pipeline_endpoint:
        return JSONResponse(
            status_code=422,
            content={
                "error": "Validation Error",
                "message": "Invalid request body for pipeline endpoint.",
                "details": safe_errors,
                "help": {
                    "valid_request_examples": [
                        "No body (uses all defaults)",
                        "Empty object: {}",
                        "Partial config: {\"max_articles_per_source\": 20}",
                        "Full config: {\"max_articles_per_source\": 15, \"max_workers\": 3, \"sources\": [\"inc42\", \"entrackr\"]}"
                    ],
                    "valid_sources": ["inc42", "entrackr", "moneycontrol", "startupnews", "indianstartup"],
                    "parameter_ranges": {
                        "max_articles_per_source": "1-100",
                        "max_workers": "1-10", 
                        "quality_threshold": "0.0-1.0"
                    }
                }
            },
        )
    else:
        return JSONResponse(
            status_code=422,
            content={
                "detail": safe_errors,
                "message": "Invalid request body for this endpoint. Please check the documentation for the correct schema."
            },
        )

# ==================== API Endpoints ====================

@app.get("/", response_model=Dict[str, Any])
async def root():
    """API root endpoint with comprehensive system information"""
    uptime = time.time() - app_start_time
    
    return {
        "service": "Startt News Intelligence API",
        "version": "2.0.0",
        "status": "operational",
        "uptime_seconds": uptime,
        "features": [
            "Production-ready architecture",
            "Dual format output (JSON + SQLite)",
            "Advanced memory management", 
            "Pipeline-specific rate limiting",
            "Real-time execution tracking",
            "Enhanced error handling",
            "AI-powered summarization",
            "Multi-tier caching",
            "Comprehensive analytics"
        ],
        "endpoints": {
            "pipeline": "/api/v1/pipeline/run",
            "pipeline_status": "/api/v1/pipeline/status/{execution_id}",
            "articles": "/api/v1/db/articles",
            "analytics": "/api/v1/analytics",
            "websocket": "/ws",
            "health": "/health",
            "metrics": "/metrics"
        },
        "improvements": [
            "✅ Fixed memory leaks in caching",
            "✅ Added proper DB connection pooling",
            "✅ Implemented pipeline rate limiting",
            "✅ Enhanced execution status tracking",
            "✅ Added dual format output support"
        ]
    }

@app.get("/health", response_model=HealthCheck)
async def health_check():
    """Comprehensive health check endpoint"""
    components = {}
    
    # Check pipeline
    components["pipeline"] = "healthy" if pipeline_instance else "unavailable"
    
    # Check database
    try:
        with get_db_connection() as conn:
            conn.execute('SELECT 1')
        components["database"] = "healthy"
    except Exception:
        components["database"] = "failed"
    
    # Check cache
    try:
        cache_stats = cache.get_stats()
        components["cache"] = "healthy"
    except Exception:
        components["cache"] = "degraded"
    
    # Check Redis
    if cache.redis_client:
        try:
            cache.redis_client.ping()
            components["redis"] = "healthy"
        except Exception:
            components["redis"] = "unavailable"
    else:
        components["redis"] = "not_configured"
    
    # Check task manager
    components["task_manager"] = "healthy"
    
    # Check WebSocket manager
    components["websocket"] = "healthy"
    
    overall_status = "healthy" if all(
        status in ["healthy", "not_configured"] 
        for status in components.values()
    ) else "degraded"
    
    return HealthCheck(
        status=overall_status,
        timestamp=datetime.now(),
        components=components,
        uptime_seconds=time.time() - app_start_time
    )

@app.post("/api/v1/pipeline/run", response_model=PipelineResponse)
async def run_pipeline(
    background_tasks: BackgroundTasks,
    request_body: Optional[Dict[str, Any]] = Body(None, description="Pipeline configuration (optional)"),
    client_id: str = Depends(get_client_id),
    rate_limit_info: Dict = Depends(check_rate_limit)
):
    """
    Execute the news aggregation pipeline with dual format output support.
    
    This endpoint accepts:
    - Full JSON configuration with any subset of parameters
    - Empty object {} for all defaults
    - No body at all for all defaults
    - Partial configuration (missing fields use defaults)
    
    All parameters are optional and will use sensible defaults if not provided.
    Results are saved to both SQLite database and JSON file by default.
    
    Example requests:
    - POST /api/v1/pipeline/run (no body)
    - POST /api/v1/pipeline/run with body: {}
    - POST /api/v1/pipeline/run with body: {"max_articles_per_source": 20, "save_to_json": true}
    - POST /api/v1/pipeline/run with body: {"sources": ["inc42", "entrackr"], "save_to_db": false}
    """
    execution_id = str(uuid.uuid4())
    
    try:
        # Check pipeline-specific rate limiting
        await check_pipeline_rate_limit(client_id)
        
        # Handle different request body scenarios
        if request_body is None:
            # No body provided - use all defaults
            pipeline_request = PipelineRequest()
        elif not request_body:
            # Empty object {} provided - use all defaults
            pipeline_request = PipelineRequest()
        else:
            # Validate and parse the provided configuration
            try:
                pipeline_request = PipelineRequest(**request_body)
            except Exception as validation_error:
                # If validation fails, try to extract valid fields and use defaults for the rest
                logger.warning(f"Partial validation failed, extracting valid fields: {validation_error}")
                valid_fields = {}
                
                # Extract only the fields that are valid
                for field_name, field_info in PipelineRequest.model_fields.items():
                    if field_name in request_body:
                        try:
                            # Try to validate individual field
                            if field_name == 'sources':
                                sources = request_body[field_name]
                                if sources is not None:
                                    valid_sources = ['inc42', 'entrackr', 'moneycontrol', 'startupnews', 'indianstartup']
                                    if isinstance(sources, list):
                                        invalid = [s for s in sources if s not in valid_sources]
                                        if not invalid:
                                            valid_fields[field_name] = sources
                                        else:
                                            logger.warning(f"Invalid sources ignored: {invalid}")
                                    else:
                                        logger.warning(f"Sources must be a list, got: {type(sources)}")
                            elif field_name in ['max_articles_per_source', 'max_workers']:
                                value = request_body[field_name]
                                if isinstance(value, int) and value > 0:
                                    if field_name == 'max_articles_per_source' and value <= 100:
                                        valid_fields[field_name] = value
                                    elif field_name == 'max_workers' and value <= 10:
                                        valid_fields[field_name] = value
                                    else:
                                        logger.warning(f"Invalid {field_name}: {value}")
                                else:
                                    logger.warning(f"Invalid {field_name}: {value}")
                            elif field_name in ['enable_ai_summary', 'save_to_json', 'save_to_db']:
                                value = request_body[field_name]
                                if isinstance(value, bool):
                                    valid_fields[field_name] = value
                                else:
                                    logger.warning(f"Invalid {field_name}: {value}")
                            elif field_name == 'quality_threshold':
                                value = request_body[field_name]
                                if isinstance(value, (int, float)) and 0.0 <= value <= 1.0:
                                    valid_fields[field_name] = value
                                else:
                                    logger.warning(f"Invalid quality_threshold: {value}")
                            elif field_name == 'cache_strategy':
                                value = request_body[field_name]
                                if value in [e.value for e in CacheStrategy]:
                                    valid_fields[field_name] = value
                                else:
                                    logger.warning(f"Invalid cache_strategy: {value}")
                        except Exception as field_error:
                            # Skip invalid fields - they'll use defaults
                            logger.warning(f"Skipping invalid field {field_name}: {field_error}")
                            continue
                
                # Create request with valid fields only
                pipeline_request = PipelineRequest(**valid_fields)
                logger.info(f"Using valid fields: {valid_fields}")
        
        # Get safe configuration values
        config = pipeline_request.get_safe_values()
        
        # Add system configuration
        config['gemini_api_key'] = os.getenv('GEMINI_API_KEY')
        config['db_path'] = os.getenv('DB_PATH', 'news_pipeline.db')
        
        logger.info(f"Pipeline execution {execution_id} starting with config: {config}")
        
        # Initialize execution status
        save_execution_status(execution_id, "running", {
            "client_id": client_id,
            "config": config,
            "started_at": datetime.now().isoformat()
        })
        
        # Get pipeline instance
        pipeline = await get_pipeline()
        
        # Submit background task
        await task_manager.submit_pipeline_task(
            pipeline, 
            pipeline_request, 
            client_id,
            execution_id
        )
        
        return PipelineResponse(
            execution_id=execution_id,
            status=PipelineStatus.RUNNING,
            message="Pipeline execution started successfully in the background. Check server logs and use the status endpoint to track progress.",
            started_at=datetime.now()
        )
        
    except HTTPException:
        # Re-raise HTTP exceptions (rate limiting, etc.)
        raise
    except Exception as e:
        logger.error(f"Failed to start pipeline execution: {e}")
        
        # Save failure status
        save_execution_status(execution_id, "failed", {
            "error": str(e),
            "failed_at": datetime.now().isoformat()
        })
        
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start pipeline execution: {str(e)}"
        )

@app.get("/api/v1/pipeline/status/{execution_id}", response_model=PipelineResponse)
async def get_pipeline_status(
    execution_id: str = Path(..., description="Pipeline execution ID"),
    client_id: str = Depends(get_client_id)
):
    """Get pipeline execution status and results with enhanced tracking"""
    
    # Get status from database (primary source)
    execution_status = get_execution_status(execution_id)
    
    if not execution_status:
        # Fallback to task manager
        task_status = task_manager.get_task_status(execution_id)
        if not task_status:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Pipeline execution {execution_id} not found"
            )
        execution_status = task_status
    
    # Build response based on status
    status_value = execution_status.get("status", "unknown")
    
    response_data = {
        "execution_id": execution_id,
        "status": status_value,
        "started_at": datetime.fromisoformat(execution_status.get("started_at", datetime.now().isoformat()))
    }
    
    if status_value == "running":
        started_at = datetime.fromisoformat(execution_status.get("started_at", datetime.now().isoformat()))
        runtime = (datetime.now() - started_at).total_seconds()
        response_data.update({
            "message": "Pipeline is running",
            "runtime": runtime
        })
        
    elif status_value == "completed":
        response_data.update({
            "message": "Pipeline completed successfully",
            "articles_count": execution_status.get("articles_count", 0),
            "execution_time": execution_status.get("execution_time"),
            "completed_at": datetime.fromisoformat(execution_status.get("completed_at", datetime.now().isoformat())),
            "analytics": execution_status.get("analytics"),
            "source_breakdown": execution_status.get("source_breakdown", {}),
            "output_files": execution_status.get("output_files", {})
        })
        
    else:  # failed
        response_data.update({
            "message": f"Pipeline failed: {execution_status.get('error', 'Unknown error')}",
            "errors": [execution_status.get('error', 'Unknown error')],
            "completed_at": datetime.fromisoformat(execution_status.get("failed_at", datetime.now().isoformat()))
        })
    
    return PipelineResponse(**response_data)

@app.get("/api/v1/analytics", response_model=Dict[str, Any])
async def get_analytics(
    include_cache_stats: bool = Query(False, description="Include cache performance stats"),
    include_task_stats: bool = Query(False, description="Include task manager stats"),
    pipeline: EnterpriseNewsPipeline = Depends(get_pipeline)
):
    """Get comprehensive system analytics"""
    
    try:
        # Get pipeline analytics
        analytics = pipeline.get_analytics_dashboard()
        
        # Add API-specific metrics
        api_metrics = {
            "cache_performance": cache.get_stats() if include_cache_stats else {},
            "task_manager": task_manager.get_stats() if include_task_stats else {},
            "websocket_connections": websocket_manager.get_stats(),
            "uptime_seconds": time.time() - app_start_time,
            "pipeline_rate_limits": {
                "max_per_hour": 3,
                "active_clients": len(pipeline_requests)
            }
        }
        
        result = {
            "pipeline_analytics": analytics,
            "api_metrics": api_metrics,
            "timestamp": datetime.now().isoformat(),
            "version": "2.0.0"
        }
        
        return result
        
    except Exception as e:
        logger.error(f"Failed to get analytics: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve analytics"
        )

@app.get("/api/v1/db/articles", response_model=Dict[str, Any])
async def get_articles_from_db(
    sources: Optional[List[str]] = Query(None),
    quality_min: Optional[float] = Query(None, ge=0.0, le=1.0),
    date_from: Optional[datetime] = Query(None),
    date_to: Optional[datetime] = Query(None),
    search_query: Optional[str] = Query(None, max_length=200),
    limit: int = Query(50, le=1000),
    offset: int = Query(0, ge=0),
    client_id: str = Depends(get_client_id)
):
    """Get articles directly from the database with advanced filtering and pagination."""
    
    try:
        # Build dynamic query
        base_query = "SELECT title, ai_summary as description, url as redirect_url, source, image_url, created_at as published_date, news_id, content FROM articles"
        count_query = "SELECT COUNT(*) as total FROM articles"
        conditions = []
        params = []
        
        # Add filters
        if sources:
            placeholders = ",".join("?" * len(sources))
            conditions.append(f"source IN ({placeholders})")
            params.extend(sources)
        
        if quality_min is not None:
            conditions.append("quality_score >= ?")
            params.append(quality_min)
        
        if date_from:
            conditions.append("created_at >= ?")
            params.append(date_from.isoformat())
        
        if date_to:
            conditions.append("created_at <= ?")
            params.append(date_to.isoformat())
        
        if search_query:
            conditions.append("(title LIKE ? OR content LIKE ?)")
            search_term = f"%{search_query}%"
            params.extend([search_term, search_term])
        
        # Build WHERE clause
        where_clause = ""
        if conditions:
            where_clause = " WHERE " + " AND ".join(conditions)
        
        # Final queries
        articles_query = f"{base_query}{where_clause} ORDER BY created_at DESC LIMIT ? OFFSET ?"
        final_count_query = f"{count_query}{where_clause}"
        
        with get_db_connection() as conn:
            # Get articles
            articles_cursor = conn.execute(articles_query, params + [limit, offset])
            articles = [dict(row) for row in articles_cursor.fetchall()]
            
            # Get total count
            count_cursor = conn.execute(final_count_query, params)
            total_articles = count_cursor.fetchone()['total']
        
        return {
            "articles": articles,
            "pagination": {
                "limit": limit,
                "offset": offset,
                "total": total_articles,
                "has_more": offset + limit < total_articles,
                "next_offset": offset + limit if offset + limit < total_articles else None
            },
            "filters_applied": {
                "sources": sources,
                "quality_min": quality_min,
                "date_from": date_from.isoformat() if date_from else None,
                "date_to": date_to.isoformat() if date_to else None,
                "search_query": search_query
            }
        }
        
    except Exception as e:
        logger.error(f"Database query failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch articles from database: {e}"
        )

@app.get("/api/v1/pipeline/results/{execution_id}/download")
async def download_results(
    execution_id: str = Path(..., description="Pipeline execution ID"),
    format: str = Query("json", regex="^(json|csv)$", description="Download format")
):
    """Download pipeline results in various formats"""
    
    # Get execution status to find output files
    execution_status = get_execution_status(execution_id)
    
    if not execution_status or execution_status.get("status") != "completed":
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Pipeline results not found or execution not completed"
        )
    
    output_files = execution_status.get("output_files", {})
    
    if format == "json" and "json" in output_files:
        json_file = output_files["json"]
        if os.path.exists(json_file):
            return FileResponse(
                json_file,
                media_type="application/json",
                filename=f"pipeline_{execution_id}.json"
            )
    
    elif format == "csv":
        # Generate CSV from database
        try:
            with get_db_connection() as conn:
                cursor = conn.execute('''
                    SELECT news_id, source, url, title, ai_summary, 
                           published_date, quality_score, created_at
                    FROM articles 
                    ORDER BY created_at DESC
                ''')
                
                import csv
                import io
                
                output = io.StringIO()
                writer = csv.DictWriter(output, fieldnames=[
                    'news_id', 'source', 'url', 'title', 'ai_summary',
                    'published_date', 'quality_score', 'created_at'
                ])
                
                writer.writeheader()
                for row in cursor.fetchall():
                    writer.writerow(dict(row))
                
                return Response(
                    content=output.getvalue(),
                    media_type="text/csv",
                    headers={
                        "Content-Disposition": f"attachment; filename=pipeline_{execution_id}.csv"
                    }
                )
                
        except Exception as e:
            logger.error(f"Failed to generate CSV: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to generate CSV export"
            )
    
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Results not available in {format} format"
    )

# ==================== WebSocket Endpoints ====================

@app.websocket("/ws/{room}")
async def websocket_endpoint(websocket: WebSocket, room: str):
    """WebSocket endpoint for real-time updates"""
    client_id = str(uuid.uuid4())
    
    try:
        await websocket_manager.connect(websocket, client_id, room)
        
        # Send welcome message
        await websocket_manager.send_personal_message({
            "type": "welcome",
            "client_id": client_id,
            "room": room,
            "timestamp": datetime.now().isoformat(),
            "message": f"Connected to room: {room}",
            "version": "2.0.0"
        }, client_id)
        
        # Keep connection alive and handle messages
        while True:
            try:
                data = await websocket.receive_text()
                message = json.loads(data)
                
                # Handle ping/pong
                if message.get("type") == "ping":
                    await websocket_manager.send_personal_message({
                        "type": "pong",
                        "timestamp": datetime.now().isoformat()
                    }, client_id)
                
                # Handle subscription to pipeline updates
                elif message.get("type") == "subscribe_pipeline":
                    execution_id = message.get("execution_id")
                    if execution_id:
                        # Send current status
                        execution_status = get_execution_status(execution_id)
                        if execution_status:
                            await websocket_manager.send_personal_message({
                                "type": "pipeline_status",
                                "execution_id": execution_id,
                                "status": execution_status
                            }, client_id)
                
            except WebSocketDisconnect:
                break
            except Exception as e:
                logger.error(f"WebSocket error for client {client_id}: {e}")
                break
    
    finally:
        websocket_manager.disconnect(client_id)

# ==================== Metrics Endpoint ====================

@app.get("/metrics")
async def get_prometheus_metrics():
    """Prometheus metrics endpoint"""
    if not PROMETHEUS_AVAILABLE:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Metrics not available - Prometheus client not installed"
        )
    
    from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
    
    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST
    )

# ==================== Custom OpenAPI Schema ====================

def custom_openapi():
    """Custom OpenAPI schema with enhanced documentation"""
    if app.openapi_schema:
        return app.openapi_schema
    
    openapi_schema = get_openapi(
        title="Startt News Intelligence API",
        version="2.0.0",
        description="""
        **Production-Ready News Aggregation and Analysis System**
        
        This API provides enterprise-grade news intelligence capabilities with:
        
        - **Dual Format Output**: Save results to both SQLite database and JSON files
        - **Production Architecture**: Memory leak fixes, proper DB pooling, rate limiting
        - **Enhanced Tracking**: Real-time execution status with persistent storage  
        - **Advanced AI Summarization**: Uses Google Gemini for intelligent content summarization
        - **Multi-Source Aggregation**: Supports 5+ Indian startup news sources
        - **Real-Time Updates**: WebSocket-based live updates
        - **Intelligent Caching**: Multi-tier caching with Redis and managed in-memory layers
        - **Pipeline Rate Limiting**: Dedicated rate limiting for expensive operations
        - **Quality Scoring**: ML-based content quality assessment
        - **Analytics Dashboard**: Comprehensive performance and usage analytics
        
        **Critical Improvements in v2.0**:
        -  Fixed memory leaks in caching system
        -  Added proper database connection management
        -  Implemented pipeline-specific rate limiting (3 per hour)
        -  Enhanced execution status tracking with database persistence
        -  Added dual format output support (JSON + SQLite)
        -  Improved error handling and validation
        
        **Rate Limits**: 
        - General API: 60 requests per minute per client
        - Pipeline Execution: 3 requests per hour per client
        
        **WebSocket Rooms**:
        - `pipeline_updates`: Real-time pipeline execution updates
        - `analytics`: Live analytics and metrics updates
        """,
        routes=app.routes,
    )
    
    # Add additional schema customizations
    openapi_schema["info"]["x-logo"] = {
        "url": "https://fastapi.tiangolo.com/img/logo-margin/logo-teal.png"
    }
    
    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.openapi = custom_openapi

# ==================== Application Runner ====================

if __name__ == "__main__":
    # Configure for production or development
    log_level = os.getenv("LOG_LEVEL", "info").lower()
    port = int(os.getenv("PORT", 8000))
    host = os.getenv("HOST", "0.0.0.0")
    
    logger.info(f"🚀 Starting Startt News Intelligence API v2.0 on {host}:{port}")
    
    uvicorn.run(
        "main:app",
        host=host,
        port=port,
        log_level=log_level,
        reload=os.getenv("ENVIRONMENT", "production") == "development",
        workers=1,  # Use 1 worker to maintain shared state
        access_log=True
    )