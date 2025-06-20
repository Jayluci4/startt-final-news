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
from contextlib import asynccontextmanager
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
    """Pipeline execution request model"""
    max_articles_per_source: PositiveInt = Field(15, description="Maximum articles per source", le=100)
    max_workers: PositiveInt = Field(3, description="Maximum concurrent workers", le=10)
    sources: Optional[List[str]] = Field(None, description="Specific sources to scrape")
    enable_ai_summary: bool = Field(True, description="Enable AI summarization")
    quality_threshold: float = Field(0.3, ge=0.0, le=1.0, description="Minimum quality threshold")
    cache_strategy: CacheStrategy = Field(CacheStrategy.CONSERVATIVE, description="Caching strategy")
    
    @validator('sources')
    def validate_sources(cls, v):
        if v is not None:
            valid_sources = ['inc42', 'entrackr', 'moneycontrol', 'startupnews', 'indianstartup']
            invalid = [s for s in v if s not in valid_sources]
            if invalid:
                raise ValueError(f"Invalid sources: {invalid}. Valid: {valid_sources}")
        return v

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

class HealthCheck(BaseModel):
    """Health check response"""
    status: str
    timestamp: datetime
    version: str = "1.0"
    components: Dict[str, str]
    uptime_seconds: float

# ==================== Advanced Caching System ====================

class IntelligentCache:
    """Multi-tier intelligent caching system"""
    
    def __init__(self):
        self.memory_cache = {}
        self.cache_stats = defaultdict(int)
        self.access_patterns = defaultdict(list)
        self.redis_client = None
        
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
    
    async def get(self, key: str, default=None) -> Any:
        """Get value with intelligent tier selection"""
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
                    self.cache_stats['redis_hits'] += 1
                    self._update_access_pattern(key)
                    return parsed_value
            except Exception as e:
                logger.warning(f"Redis get failed: {e}")
        
        self.cache_stats['misses'] += 1
        return default
    
    async def set(self, key: str, value: Any, ttl: int = 3600) -> bool:
        """Set value with intelligent tier distribution"""
        try:
            # Always store in memory for immediate access
            self.memory_cache[key] = value
            
            # Store in Redis for persistence
            if self.redis_client:
                try:
                    serialized = json.dumps(value, default=str)
                    self.redis_client.setex(key, ttl, serialized)
                except Exception as e:
                    logger.warning(f"Redis set failed: {e}")
            
            # Manage memory cache size
            if len(self.memory_cache) > 1000:  # LRU eviction
                oldest_key = min(self.memory_cache.keys(), 
                               key=lambda k: min(self.access_patterns.get(k, [time.time()])))
                del self.memory_cache[oldest_key]
            
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
            'memory_cache_size': len(self.memory_cache)
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
                                 client_id: str) -> str:
        """Submit pipeline task with comprehensive monitoring"""
        task_id = str(uuid.uuid4())
        start_time = datetime.now()
        
        def pipeline_wrapper():
            """Wrapper function for pipeline execution with error handling"""
            try:
                articles = pipeline.process_all_sources(
                    max_articles_per_source=request.max_articles_per_source,
                    max_workers=request.max_workers
                )
                
                # Filter articles by quality threshold
                filtered_articles = [
                    article for article in articles
                    if article.get('metadata', {}).get('quality_score', 0) >= request.quality_threshold
                ]
                
                return {
                    'status': 'completed',
                    'articles': filtered_articles,
                    'analytics': pipeline.get_analytics_dashboard()
                }
            except Exception as e:
                logger.error(f"Pipeline task {task_id} failed: {e}")
                return {
                    'status': 'failed',
                    'error': str(e),
                    'articles': []
                }
        
        # Submit task to executor
        future = self.executor.submit(pipeline_wrapper)
        
        self.running_tasks[task_id] = {
            'future': future,
            'client_id': client_id,
            'request': request,
            'started_at': start_time,
            'status': PipelineStatus.RUNNING
        }
        
        self.task_stats['submitted'] += 1
        
        if PROMETHEUS_AVAILABLE:
            PIPELINE_RUNS.labels(status='started').inc()
        
        logger.info(f"Pipeline task {task_id} submitted for client {client_id}")
        return task_id
    
    def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Get comprehensive task status"""
        if task_id in self.running_tasks:
            task = self.running_tasks[task_id]
            future = task['future']
            
            if future.done():
                # Move to completed tasks
                try:
                    result = future.result()
                    status = PipelineStatus.COMPLETED if result['status'] == 'completed' else PipelineStatus.FAILED
                    
                    completed_task = {
                        'task_id': task_id,
                        'status': status,
                        'result': result,
                        'started_at': task['started_at'],
                        'completed_at': datetime.now(),
                        'execution_time': (datetime.now() - task['started_at']).total_seconds(),
                        'client_id': task['client_id']
                    }
                    
                    self.completed_tasks.append(completed_task)
                    del self.running_tasks[task_id]
                    
                    self.task_stats['completed' if status == PipelineStatus.COMPLETED else 'failed'] += 1
                    
                    if PROMETHEUS_AVAILABLE:
                        PIPELINE_RUNS.labels(status=status.value).inc()
                    
                    return completed_task
                except Exception as e:
                    logger.error(f"Task {task_id} failed with exception: {e}")
                    return {
                        'task_id': task_id,
                        'status': PipelineStatus.FAILED,
                        'error': str(e),
                        'started_at': task['started_at'],
                        'completed_at': datetime.now()
                    }
            else:
                return {
                    'task_id': task_id,
                    'status': PipelineStatus.RUNNING,
                    'started_at': task['started_at'],
                    'runtime': (datetime.now() - task['started_at']).total_seconds()
                }
        
        # Check completed tasks
        for completed_task in self.completed_tasks:
            if completed_task['task_id'] == task_id:
                return completed_task
        
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

# ==================== FastAPI Application ====================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    logger.info("🚀 Startt News Intelligence API starting up...")
    
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
            # Using a thread to not block the startup process
            def run_initial_pipeline():
                with ThreadPoolExecutor(max_workers=1) as executor:
                    # Provide default arguments for the initial run
                    future = executor.submit(pipeline_instance.process_all_sources, 15, 3)
                    future.result()
                    logger.info("Initial pipeline run completed.")
            
            initial_run_thread = threading.Thread(target=run_initial_pipeline)
            initial_run_thread.start()

        except Exception as e:
            logger.error(f"Initial pipeline run failed: {e}")
    
    yield
    
    logger.info("Startt News Intelligence API shutting down...")
    # Cleanup tasks
    task_manager.executor.shutdown(wait=True)

# Create FastAPI application
app = FastAPI(
    title="Startt News Intelligence API",
    description="Advanced news aggregation and analysis system with real-time capabilities",
    version="1.0.0",
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

# ==================== API Endpoints ====================

@app.get("/", response_model=Dict[str, Any])
async def root():
    """API root endpoint with comprehensive system information"""
    uptime = time.time() - app_start_time
    
    return {
        "service": "Startt News Intelligence API",
        "version": "1.0.0",
        "status": "operational",
        "uptime_seconds": uptime,
        "features": [
            "Advanced news aggregation",
            "AI-powered summarization",
            "Real-time WebSocket updates",
            "Multi-tier caching",
            "Rate limiting",
            "Comprehensive analytics"
        ],
        "endpoints": {
            "pipeline": "/api/v1/pipeline/run",
            "articles": "/api/v1/db/articles",
            "analytics": "/api/v1/analytics",
            "websocket": "/ws",
            "health": "/health",
            "metrics": "/metrics"
        }
    }

@app.get("/health", response_model=HealthCheck)
async def health_check():
    """Comprehensive health check endpoint"""
    components = {}
    
    # Check pipeline
    components["pipeline"] = "healthy" if pipeline_instance else "unavailable"
    
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

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request, exc):
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
    safe_body = safe_error(getattr(exc, 'body', None))
    return JSONResponse(
        status_code=422,
        content={
            "detail": safe_errors,
            "body": safe_body,
            "message": "Invalid request body for this endpoint. Please check the documentation for the correct schema. If you want to use all defaults, send an empty object {} or no body at all."
        },
    )

@app.post("/api/v1/pipeline/run", response_model=PipelineResponse)
async def run_pipeline(
    background_tasks: BackgroundTasks,
    request: PipelineRequest = Body(default_factory=PipelineRequest),
    client_id: str = Depends(get_client_id),
    rate_limit_info: Dict = Depends(check_rate_limit)
):
    """
    Execute the news aggregation pipeline by running the run_pipeline.py script.
    This provides the full functionality of the command-line interface as an API endpoint.
    """
    pipeline_request = request
    execution_id = str(uuid.uuid4())
    # Convert Pydantic model to a dictionary for the config
    config = pipeline_request.dict()
    # Add other necessary config defaults that are not in PipelineRequest
    config['gemini_api_key'] = os.getenv('GEMINI_API_KEY')
    config['db_path'] = os.getenv('DB_PATH', 'news_pipeline.db')
    def run_in_background():
        """Wrapper to run the script and handle output/errors."""
        try:
            print(f"Starting pipeline execution {execution_id} from API request.")
            # Create a mock 'args' object for execute_pipeline
            args = Namespace(quiet=True, analytics=True, reset_db=False)
            execute_pipeline(config, args)
            print(f"Pipeline execution {execution_id} finished.")
        except Exception as e:
            print(f"Error during pipeline execution {execution_id}: {e}")
            import traceback
            traceback.print_exc()
    background_tasks.add_task(run_in_background)
    return PipelineResponse(
        execution_id=execution_id,
        status=PipelineStatus.RUNNING,
        message="Pipeline execution started in the background. Check server logs for progress and results.",
        started_at=datetime.now()
    )

@app.get("/api/v1/pipeline/status/{execution_id}", response_model=PipelineResponse)
async def get_pipeline_status(
    execution_id: str = Path(..., description="Pipeline execution ID"),
    client_id: str = Depends(get_client_id)
):
    """Get pipeline execution status and results"""
    
    task_status = task_manager.get_task_status(execution_id)
    
    if not task_status:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Pipeline execution {execution_id} not found"
        )
    
    # Build response
    response_data = {
        "execution_id": execution_id,
        "status": task_status["status"],
        "started_at": task_status["started_at"]
    }
    
    if task_status["status"] == PipelineStatus.RUNNING:
        response_data.update({
            "message": "Pipeline is running",
            "runtime": task_status.get("runtime", 0)
        })
    elif task_status["status"] == PipelineStatus.COMPLETED:
        result = task_status.get("result", {})
        articles = result.get("articles", [])
        
        # Cache results for fast retrieval
        await cache.set(f"pipeline_result:{execution_id}", articles, ttl=7200)
        
        response_data.update({
            "message": "Pipeline completed successfully",
            "articles_count": len(articles),
            "execution_time": task_status.get("execution_time"),
            "completed_at": task_status.get("completed_at"),
            "analytics": result.get("analytics"),
            "source_breakdown": {
                source: len([a for a in articles if a.get("news_source") == source])
                for source in ["Inc42", "Entrackr", "Moneycontrol", "StartupNews.fyi", "IndianStartupNews"]
            }
        })
    else:  # FAILED
        response_data.update({
            "message": f"Pipeline failed: {task_status.get('error', 'Unknown error')}",
            "errors": [task_status.get('error', 'Unknown error')],
            "completed_at": task_status.get("completed_at")
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
            "uptime_seconds": time.time() - app_start_time
        }
        
        result = {
            "pipeline_analytics": analytics,
            "api_metrics": api_metrics,
            "timestamp": datetime.now().isoformat()
        }
        
        return result
        
    except Exception as e:
        logger.error(f"Failed to get analytics: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve analytics"
        )

@app.get("/api/v1/pipeline/results/{execution_id}/download")
async def download_results(
    execution_id: str = Path(..., description="Pipeline execution ID"),
    format: str = Query("json", regex="^(json|csv)$", description="Download format")
):
    """Download pipeline results in various formats"""
    
    # Get cached results
    articles = await cache.get(f"pipeline_result:{execution_id}")
    
    if not articles:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Pipeline results not found or expired"
        )
    
    if format == "json":
        # Return compressed JSON
        json_data = json.dumps({
            "execution_id": execution_id,
            "exported_at": datetime.now().isoformat(),
            "articles": articles
        }, indent=2)
        
        response = Response(
            content=gzip.compress(json_data.encode()),
            media_type="application/json",
            headers={
                "Content-Encoding": "gzip",
                "Content-Disposition": f"attachment; filename=pipeline_{execution_id}.json.gz"
            }
        )
        return response
    
    elif format == "csv":
        # Convert to CSV format
        import csv
        import io
        
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=[
            'news_id', 'news_source', 'source_url', 'news_title', 
            'ai_summary', 'news_published_date', 'quality_score'
        ])
        
        writer.writeheader()
        for article in articles:
            writer.writerow({
                'news_id': article.get('news_id'),
                'news_source': article.get('news_source'),
                'source_url': article.get('source_url'),
                'news_title': article.get('news_title'),
                'ai_summary': article.get('ai_summary'),
                'news_published_date': article.get('news_published_date'),
                'quality_score': article.get('metadata', {}).get('quality_score')
            })
        
        return Response(
            content=output.getvalue(),
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename=pipeline_{execution_id}.csv"
            }
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
            "message": f"Connected to room: {room}"
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
                        task_status = task_manager.get_task_status(execution_id)
                        if task_status:
                            await websocket_manager.send_personal_message({
                                "type": "pipeline_status",
                                "execution_id": execution_id,
                                "status": task_status
                            }, client_id)
                
            except WebSocketDisconnect:
                break
            except Exception as e:
                logger.error(f"WebSocket error for client {client_id}: {e}")
                break
    
    finally:
        websocket_manager.disconnect(client_id)

# ==================== Advanced Features ====================

@app.get("/api/v1/pipeline/schedule", response_model=Dict[str, Any])
async def schedule_pipeline(
    cron_expression: str = Query(..., description="Cron expression for scheduling"),
    pipeline_config: PipelineRequest = Depends()
):
    """Schedule periodic pipeline execution (placeholder for future implementation)"""
    
    # This would integrate with a job scheduler like Celery or APScheduler
    return {
        "message": "Pipeline scheduling not yet implemented",
        "cron_expression": cron_expression,
        "config": jsonable_encoder(pipeline_config),
        "note": "This feature will be available in the next release"
    }

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
        version="1.0.0",
        description="""
        **Advanced News Aggregation and Analysis System**
        
        This API provides enterprise-grade news intelligence capabilities with:
        
        - **Advanced AI Summarization**: Uses Google Gemini for intelligent content summarization
        - **Multi-Source Aggregation**: Supports 5+ Indian startup news sources
        - **Real-Time Updates**: WebSocket-based live updates
        - **Intelligent Caching**: Multi-tier caching with Redis and in-memory layers
        - **Rate Limiting**: Token bucket algorithm with adaptive thresholds
        - **Quality Scoring**: ML-based content quality assessment
        - **Analytics Dashboard**: Comprehensive performance and usage analytics
        
        **Rate Limits**: 60 requests per minute per client
        
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
    
    logger.info(f"🚀 Starting Startt News Intelligence API on {host}:{port}")
    
    uvicorn.run(
        "main:app",
        host=host,
        port=port,
        log_level=log_level,
        reload=os.getenv("ENVIRONMENT", "production") == "development",
        workers=1,  # Use 1 worker to maintain shared state
        access_log=True
    )

def get_db_connection():
    """Create and return a new database connection."""
    try:
        conn = sqlite3.connect(os.getenv('DB_PATH', 'news_pipeline.db'))
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error as e:
        logger.error(f"Database connection error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database connection failed"
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
    # Reconstruct ArticleFilter from query params
    filter_params = ArticleFilter(
        sources=sources,
        quality_min=quality_min,
        date_from=date_from,
        date_to=date_to,
        search_query=search_query,
        limit=limit,
        offset=offset
    )
    query = "SELECT * FROM articles LIMIT 50"
    try:
        with get_db_connection() as conn:
            articles_cursor = conn.execute(query)
            articles = [dict(row) for row in articles_cursor.fetchall()]
            total_articles = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
    except sqlite3.Error as e:
        logger.error(f"Database query failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch articles from database: {e}"
        )
    return {
        "articles": articles,
        "pagination": {
            "limit": 50,
            "offset": 0,
            "total": total_articles,
            "has_more": total_articles > 50,
        },
        "filters_applied": jsonable_encoder(filter_params),
    }
