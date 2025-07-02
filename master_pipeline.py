#!/usr/bin/env python3
import requests
from bs4 import BeautifulSoup
import json
import hashlib
import time
import re
import os
import logging
import sqlite3
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple, Set, Union
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from urllib.parse import urljoin, urlparse, quote_plus
from urllib.robotparser import RobotFileParser
from dataclasses import dataclass, field, asdict
from collections import defaultdict, deque
from functools import wraps, lru_cache
import pickle
import gzip
import base64
from enum import Enum
import statistics
import random
import math
import httpx
from sqlalchemy import create_engine, Column, Integer, String, Text, Float, DateTime, JSON, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
import asyncio

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
    logger = logging.getLogger(__name__)
    logger.info("Loaded environment variables from .env file")
except ImportError:
    logger = logging.getLogger(__name__)
    logger.warning("python-dotenv not available - .env file support disabled")

# Advanced imports with graceful fallbacks
try:
    import google.generativeai as genai
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False
    logging.warning("Gemini AI not available - using fallback summarization")

try:
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity
    from sklearn.cluster import DBSCAN
    import numpy as np
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    logging.warning("scikit-learn not available - using basic algorithms")

try:
    from sentence_transformers import SentenceTransformer
    SENTENCE_TRANSFORMERS_AVAILABLE = True
except ImportError:
    SENTENCE_TRANSFORMERS_AVAILABLE = False
    logging.warning("sentence-transformers not available - using TF-IDF fallback")

try:
    import nltk
    from nltk.corpus import stopwords
    from nltk.tokenize import word_tokenize, sent_tokenize
    from nltk.stem import PorterStemmer
    NLTK_AVAILABLE = True
except ImportError:
    NLTK_AVAILABLE = False
    logging.warning("NLTK not available - using basic text processing")

# Configure comprehensive logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(name)s | %(levelname)s | %(funcName)s:%(lineno)d | %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('pipeline.log', mode='a', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

class ExtractionStrategy(Enum):
    """Content extraction strategies in order of preference"""
    JSON_LD = "json_ld"
    MICRODATA = "microdata"
    OPENGRAPH = "opengraph"
    HTML_SEMANTIC = "html_semantic"
    HTML_HEURISTIC = "html_heuristic"
    ML_BASED = "ml_based"
    FALLBACK = "fallback"

class ContentQuality(Enum):
    """Content quality levels"""
    EXCELLENT = "excellent"
    GOOD = "good" 
    ACCEPTABLE = "acceptable"
    POOR = "poor"
    REJECTED = "rejected"

@dataclass
class ExtractionMetrics:
    """Metrics for monitoring extraction performance"""
    strategy_used: ExtractionStrategy
    extraction_time: float
    content_length: int
    quality_score: float
    confidence_score: float
    fallback_count: int = 0
    errors_encountered: List[str] = field(default_factory=list)

@dataclass
class CircuitBreakerState:
    """Circuit breaker for handling failing sources"""
    failure_count: int = 0
    last_failure_time: Optional[datetime] = None
    state: str = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
    failure_threshold: int = 5
    recovery_timeout: int = 300  # 5 minutes

@dataclass
class SourceConfiguration:
    """Enhanced source configuration with adaptive capabilities"""
    name: str
    base_url: str
    list_url: str
    
    # Multiple selector strategies
    primary_selectors: Dict[str, str]
    fallback_selectors: Dict[str, List[str]]
    
    # Adaptive parameters
    request_delay: float = 2.0
    max_retries: int = 3
    timeout: int = 30
    
    # Quality controls
    min_content_length: int = 100
    max_content_length: int = 50000
    
    # Rate limiting
    requests_per_minute: int = 30
    
    # Circuit breaker
    circuit_breaker: CircuitBreakerState = field(default_factory=CircuitBreakerState)
    
    # Success tracking
    success_rate: float = 1.0
    last_success: Optional[datetime] = None
    
    # Robots.txt compliance
    robots_txt_url: Optional[str] = None
    user_agent_allowed: bool = True

class AdvancedDeduplicator:
    """Multi-algorithm deduplication system"""
    
    def __init__(self):
        self.similarity_threshold = 0.85
        self.url_cache = set()
        self.title_hashes = set()
        self.content_hashes = set()
        self.semantic_embeddings = {}
        
        # Initialize ML models if available
        self.sentence_model = None
        if SENTENCE_TRANSFORMERS_AVAILABLE:
            try:
                self.sentence_model = SentenceTransformer('all-MiniLM-L6-v2')
                logger.info("Semantic similarity model loaded")
            except Exception as e:
                logger.warning(f"Failed to load sentence transformer: {e}")
        
        self.tfidf_vectorizer = None
        if SKLEARN_AVAILABLE:
            self.tfidf_vectorizer = TfidfVectorizer(
                max_features=5000,
                stop_words='english',
                ngram_range=(1, 3),
                min_df=1,
                max_df=0.95
            )
        
        # Text processing
        self.stemmer = None
        if NLTK_AVAILABLE:
            try:
                self.stemmer = PorterStemmer()
                # Download required NLTK data
                import nltk
                nltk.download('punkt', quiet=True)
                nltk.download('stopwords', quiet=True)
            except Exception as e:
                logger.warning(f"NLTK initialization failed: {e}")
    
    def generate_content_fingerprint(self, title: str, content: str, url: str) -> str:
        """Generate a robust content fingerprint using multiple algorithms"""
        # Normalize text
        title_clean = self._normalize_text(title)
        content_clean = self._normalize_text(content[:2000])  # First 2K chars
        
        # Create multiple hash components
        components = [
            title_clean,
            content_clean,
            self._extract_key_phrases(title_clean + " " + content_clean),
            urlparse(url).path  # URL path component
        ]
        
        # Combine with weights
        weighted_content = "|".join([
            f"T:{title_clean}",
            f"C:{content_clean}",
            f"U:{urlparse(url).path}"
        ])
        
        return hashlib.sha256(weighted_content.encode('utf-8')).hexdigest()
    
    def _normalize_text(self, text: str) -> str:
        """Advanced text normalization"""
        if not text:
            return ""
        
        # Convert to lowercase
        text = text.lower()
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text.strip())
        
        # Remove common artifacts
        artifacts = [
            r'\b(inc42|entrackr|moneycontrol|startupnews)\b',
            r'\b(source|image|reuters|pti|ians|et)\b',
            r'\b(read more|also read|continue reading)\b',
            r'https?://[^\s]+',  # URLs
            r'\S+@\S+',  # Email addresses
        ]
        
        for pattern in artifacts:
            text = re.sub(pattern, '', text)
        
        # Stem words if NLTK available
        if self.stemmer and NLTK_AVAILABLE:
            try:
                tokens = word_tokenize(text)
                stemmed = [self.stemmer.stem(token) for token in tokens if token.isalpha()]
                text = ' '.join(stemmed)
            except:
                pass  # Fallback to original text
        
        return text.strip()
    
    def _extract_key_phrases(self, text: str) -> str:
        """Extract key phrases for better matching"""
        # Simple phrase extraction - in production, use more sophisticated NLP
        words = text.split()
        
        # Extract potential key phrases (2-4 word combinations)
        phrases = []
        for i in range(len(words) - 1):
            if len(words[i]) > 3 and len(words[i + 1]) > 3:
                phrases.append(f"{words[i]} {words[i + 1]}")
        
        return " ".join(phrases[:10])  # Top 10 phrases
    
    def check_duplicate(self, article: Dict[str, Any], existing_articles: List[Dict[str, Any]]) -> Tuple[bool, float, Optional[Dict]]:
        """Multi-algorithm duplicate detection"""
        
        # Quick checks first (fast)
        url = article.get('source_url', '')
        title = article.get('news_title', '')
        content = article.get('news_content', '')
        
        # URL exact match
        if url in self.url_cache:
            return True, 1.0, {'reason': 'exact_url_match', 'url': url}
        
        # Title hash match
        title_hash = hashlib.md5(self._normalize_text(title).encode()).hexdigest()
        if title_hash in self.title_hashes:
            return True, 0.95, {'reason': 'title_hash_match', 'title': title[:50]}
        
        # Content fingerprint
        content_fingerprint = self.generate_content_fingerprint(title, content, url)
        if content_fingerprint in self.content_hashes:
            return True, 0.90, {'reason': 'content_fingerprint_match'}
        
        # Semantic similarity (expensive - only if needed)
        if existing_articles and (self.sentence_model or self.tfidf_vectorizer):
            similarity_result = self._semantic_similarity_check(article, existing_articles)
            if similarity_result[0]:
                return similarity_result
        
        # Mark as seen
        self.url_cache.add(url)
        self.title_hashes.add(title_hash)
        self.content_hashes.add(content_fingerprint)
        
        return False, 0.0, None
    
    def _semantic_similarity_check(self, new_article: Dict, existing_articles: List[Dict]) -> Tuple[bool, float, Optional[Dict]]:
        """Perform semantic similarity check using available ML models"""
        
        new_text = self._prepare_text_for_similarity(new_article)
        
        if self.sentence_model:
            return self._sentence_transformer_similarity(new_text, existing_articles)
        elif self.tfidf_vectorizer and SKLEARN_AVAILABLE:
            return self._tfidf_similarity(new_text, existing_articles)
        
        return False, 0.0, None
    
    def _prepare_text_for_similarity(self, article: Dict) -> str:
        """Prepare text for similarity comparison"""
        title = article.get('news_title', '')
        content = article.get('news_content', '')[:1000]  # First 1K chars
        
        combined = f"{title} {content}"
        return self._normalize_text(combined)
    
    def _sentence_transformer_similarity(self, new_text: str, existing_articles: List[Dict]) -> Tuple[bool, float, Optional[Dict]]:
        """Use sentence transformers for semantic similarity"""
        try:
            new_embedding = self.sentence_model.encode([new_text])
            
            for existing in existing_articles[-50:]:  # Check last 50 articles
                existing_text = self._prepare_text_for_similarity(existing)
                existing_embedding = self.sentence_model.encode([existing_text])
                
                similarity = cosine_similarity(new_embedding, existing_embedding)[0][0]
                
                if similarity > self.similarity_threshold:
                    return True, float(similarity), {
                        'reason': 'semantic_similarity',
                        'similar_title': existing.get('news_title', '')[:50],
                        'similarity_score': similarity
                    }
        except Exception as e:
            logger.warning(f"Sentence transformer similarity failed: {e}")
        
        return False, 0.0, None
    
    def _tfidf_similarity(self, new_text: str, existing_articles: List[Dict]) -> Tuple[bool, float, Optional[Dict]]:
        """Use TF-IDF for similarity comparison"""
        try:
            existing_texts = [self._prepare_text_for_similarity(a) for a in existing_articles[-50:]]
            all_texts = existing_texts + [new_text]
            
            tfidf_matrix = self.tfidf_vectorizer.fit_transform(all_texts)
            
            # Compare new article (last one) with existing ones
            new_vector = tfidf_matrix[-1]
            existing_vectors = tfidf_matrix[:-1]
            
            similarities = cosine_similarity(new_vector, existing_vectors).flatten()
            max_similarity = np.max(similarities) if len(similarities) > 0 else 0
            
            if max_similarity > self.similarity_threshold:
                most_similar_idx = np.argmax(similarities)
                return True, float(max_similarity), {
                    'reason': 'tfidf_similarity',
                    'similar_title': existing_articles[most_similar_idx].get('news_title', '')[:50],
                    'similarity_score': max_similarity
                }
        except Exception as e:
            logger.warning(f"TF-IDF similarity failed: {e}")
        
        return False, 0.0, None

class AdaptiveContentExtractor:
    """Adaptive content extraction with multiple strategies and learning"""
    
    def __init__(self):
        self.extraction_history = defaultdict(list)
        self.strategy_success_rates = defaultdict(float)
        self.selector_cache = {}
        
        # Common content selectors ordered by reliability
        self.content_selectors = [
            # Semantic HTML5
            'article', 'main', '[role="main"]',
            # Common content containers
            '.content', '.post-content', '.entry-content', '.article-content',
            '.story-content', '.news-content', '#content', '#main-content',
            # Specific news site patterns
            '.td-post-content', '.single-post-content', '#contentdata',
            '.post-body', '.article-body', '.story-body',
            # Generic containers
            '.container .content', '.wrapper .content', '.main .content'
        ]
        
        self.image_selectors = [
            # Featured images
            '.featured-image img', '.post-image img', '.article-image img',
            # Hero images
            '.hero img', '.hero-image img', '.banner img',
            # Content images
            'article img', '.content img', '.post-content img',
            # News specific
            '.news-image img', '.story-image img', '.artImg img',
            # Generic
            'img[src*="jpg"], img[src*="jpeg"], img[src*="png"], img[src*="webp"]'
        ]
    
    def extract_content(self, html: str, url: str, source_config: SourceConfiguration) -> Tuple[Dict[str, Any], ExtractionMetrics]:
        """Extract content using adaptive multi-strategy approach"""
        start_time = time.time()
        soup = BeautifulSoup(html, 'html.parser')
        
        metrics = ExtractionMetrics(
            strategy_used=ExtractionStrategy.FALLBACK,
            extraction_time=0,
            content_length=0,
            quality_score=0,
            confidence_score=0
        )
        
        strategies = [
            (ExtractionStrategy.JSON_LD, self._extract_json_ld),
            (ExtractionStrategy.MICRODATA, self._extract_microdata),
            (ExtractionStrategy.OPENGRAPH, self._extract_opengraph),
            (ExtractionStrategy.HTML_SEMANTIC, self._extract_semantic_html),
            (ExtractionStrategy.HTML_HEURISTIC, self._extract_heuristic),
        ]
        
        best_result = None
        best_quality = 0
        
        for strategy, extractor_func in strategies:
            try:
                result = extractor_func(soup, url, source_config)
                quality = self._assess_content_quality(result)
                
                if quality > best_quality:
                    best_result = result
                    best_quality = quality
                    metrics.strategy_used = strategy
                
                # If we get excellent quality, no need to try more expensive methods
                if quality > 0.9:
                    break
                    
            except Exception as e:
                metrics.errors_encountered.append(f"{strategy.value}: {str(e)}")
                logger.warning(f"Extraction strategy {strategy.value} failed: {e}")
        
        # Final fallback
        if not best_result or best_quality < 0.3:
            try:
                best_result = self._extract_fallback(soup, url, source_config)
                best_quality = self._assess_content_quality(best_result)
                metrics.strategy_used = ExtractionStrategy.FALLBACK
                metrics.fallback_count += 1
            except Exception as e:
                metrics.errors_encountered.append(f"fallback: {str(e)}")
                best_result = self._empty_result()
        
        # Update metrics
        metrics.extraction_time = time.time() - start_time
        metrics.content_length = len(best_result.get('content', ''))
        metrics.quality_score = best_quality
        metrics.confidence_score = min(best_quality * 1.2, 1.0)
        
        # Learn from this extraction
        self._update_extraction_history(url, metrics.strategy_used, best_quality)
        
        return best_result, metrics
    
    def _extract_json_ld(self, soup: BeautifulSoup, url: str, config: SourceConfiguration) -> Dict[str, Any]:
        """Extract from JSON-LD structured data"""
        scripts = soup.find_all('script', type='application/ld+json')
        
        for script in scripts:
            if not script.string:
                continue
            
            try:
                # Clean JSON string
                json_str = re.sub(r'[\n\r\t]', ' ', script.string)
                json_data = json.loads(json_str)
                
                # Handle both single objects and arrays
                items = json_data if isinstance(json_data, list) else [json_data]
                
                for item in items:
                    if isinstance(item, dict) and item.get('@type') in ('Article', 'NewsArticle', 'BlogPosting'):
                        return {
                            'title': self._clean_text(item.get('headline', '')),
                            'content': self._clean_text(item.get('articleBody', '')),
                            'description': self._clean_text(item.get('description', '')),
                            'author': self._extract_author_json_ld(item),
                            'date': self._extract_date_json_ld(item),
                            'image_url': self._extract_image_json_ld(item)
                        }
            except (json.JSONDecodeError, KeyError) as e:
                logger.debug(f"JSON-LD parsing failed: {e}")
                continue
        
        return self._empty_result()
    
    def _extract_microdata(self, soup: BeautifulSoup, url: str, config: SourceConfiguration) -> Dict[str, Any]:
        """Extract from HTML5 microdata"""
        # Look for Article microdata
        article_elem = soup.find(attrs={'itemtype': re.compile(r'schema\.org/(Article|NewsArticle|BlogPosting)')})
        
        if article_elem:
            return {
                'title': self._get_microdata_prop(article_elem, 'headline') or self._get_microdata_prop(article_elem, 'name'),
                'content': self._get_microdata_prop(article_elem, 'articleBody'),
                'description': self._get_microdata_prop(article_elem, 'description'),
                'author': self._get_microdata_prop(article_elem, 'author'),
                'date': self._get_microdata_prop(article_elem, 'datePublished'),
                'image_url': self._get_microdata_prop(article_elem, 'image')
            }
        
        return self._empty_result()
    
    def _extract_opengraph(self, soup: BeautifulSoup, url: str, config: SourceConfiguration) -> Dict[str, Any]:
        """Extract from OpenGraph meta tags"""
        og_data = {}
        
        og_tags = {
            'title': ['og:title', 'twitter:title'],
            'description': ['og:description', 'twitter:description'],
            'image_url': ['og:image', 'twitter:image'],
            'type': ['og:type']
        }
        
        for key, tags in og_tags.items():
            for tag in tags:
                meta = soup.find('meta', property=tag) or soup.find('meta', attrs={'name': tag})
                if meta and meta.get('content'):
                    og_data[key] = meta.get('content')
                    break
        
        # OpenGraph doesn't have full content, so try to extract it
        content = self._extract_content_heuristic(soup)
        
        return {
            'title': og_data.get('title', ''),
            'content': content,
            'description': og_data.get('description', ''),
            'author': '',
            'date': '',
            'image_url': og_data.get('image_url', '')
        }
    
    def _extract_semantic_html(self, soup: BeautifulSoup, url: str, config: SourceConfiguration) -> Dict[str, Any]:
        """Extract using HTML5 semantic elements"""
        result = self._empty_result()
        
        # Title
        title_elem = soup.find('h1') or soup.find('title')
        if title_elem:
            result['title'] = self._clean_text(title_elem.get_text())
        
        # Content from semantic elements
        content_sources = ['article', 'main', '[role="main"]']
        for selector in content_sources:
            elem = soup.select_one(selector)
            if elem:
                result['content'] = self._extract_content_from_element(elem)
                if len(result['content']) > 200:
                    break
        
        # Meta description
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc:
            result['description'] = meta_desc.get('content', '')
        
        # Author from semantic markup
        author_elem = soup.find(attrs={'rel': 'author'}) or soup.select_one('.author, .byline, [itemprop="author"]')
        if author_elem:
            result['author'] = self._clean_text(author_elem.get_text())
        
        # Date from time element
        time_elem = soup.find('time')
        if time_elem:
            result['date'] = time_elem.get('datetime') or time_elem.get_text()
        
        # Image
        result['image_url'] = self._extract_best_image(soup, url)
        
        return result
    
    def _extract_heuristic(self, soup: BeautifulSoup, url: str, config: SourceConfiguration) -> Dict[str, Any]:
        """Extract using heuristic methods and site-specific patterns"""
        result = self._empty_result()
        
        # Try source-specific selectors first
        if hasattr(config, 'primary_selectors'):
            result = self._try_source_selectors(soup, config)
            if self._assess_content_quality(result) > 0.5:
                return result
        
        # Fallback to heuristic extraction
        result['title'] = self._extract_title_heuristic(soup)
        result['content'] = self._extract_content_heuristic(soup)
        result['description'] = self._extract_description_heuristic(soup)
        result['author'] = self._extract_author_heuristic(soup)
        result['date'] = self._extract_date_heuristic(soup)
        result['image_url'] = self._extract_best_image(soup, url)
        
        return result
    
    def _extract_fallback(self, soup: BeautifulSoup, url: str, config: SourceConfiguration) -> Dict[str, Any]:
        """Last resort extraction method"""
        result = self._empty_result()
        
        # Very basic extraction
        result['title'] = soup.title.string if soup.title else ''
        
        # Extract all paragraph text
        paragraphs = soup.find_all('p')
        content_parts = []
        for p in paragraphs:
            text = self._clean_text(p.get_text())
            if len(text) > 50:  # Filter out short paragraphs
                content_parts.append(text)
        
        result['content'] = '\n\n'.join(content_parts[:10])  # Limit to 10 paragraphs
        
        # Basic meta extraction
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc:
            result['description'] = meta_desc.get('content', '')
        
        return result
    
    def _extract_content_heuristic(self, soup: BeautifulSoup) -> str:
        """Extract main content using heuristic methods"""
        # Try content selectors in order of preference
        for selector in self.content_selectors:
            try:
                elem = soup.select_one(selector)
                if elem:
                    content = self._extract_content_from_element(elem)
                    if len(content) > 200:  # Minimum viable content length
                        return content
            except Exception:
                continue
        
        # Fallback: extract from body, filtering out nav/sidebar content
        body = soup.find('body')
        if body:
            # Remove navigation, sidebar, footer elements
            for elem in body.select('nav, sidebar, footer, .nav, .sidebar, .footer, .menu, .comments'):
                elem.decompose()
            
            return self._extract_content_from_element(body)
        
        return ''
    
    def _extract_content_from_element(self, elem) -> str:
        """Extract clean content from an element"""
        # Remove script, style, and other non-content elements
        for tag in elem.select('script, style, nav, footer, .ad, .advertisement, .social-share'):
            tag.decompose()
        
        # Extract paragraphs
        paragraphs = elem.find_all(['p', 'div'], string=True)
        content_parts = []
        
        for p in paragraphs:
            text = self._clean_text(p.get_text())
            if len(text) > 30 and not self._is_boilerplate(text):
                content_parts.append(text)
        
        return '\n\n'.join(content_parts)
    
    def _extract_best_image(self, soup: BeautifulSoup, url: str) -> str:
        """Extract the best representative image"""
        candidates = []
        
        # Try image selectors
        for selector in self.image_selectors:
            imgs = soup.select(selector)
            for img in imgs:
                src = img.get('src') or img.get('data-src')
                if src:
                    candidates.append(src)
        
        # Score images and pick the best one
        best_image = ''
        best_score = 0
        
        for img_url in candidates:
            score = self._score_image(img_url, url)
            if score > best_score:
                best_score = score
                best_image = img_url
        
        return best_image
    
    def _score_image(self, img_url: str, base_url: str) -> float:
        """Score an image based on various factors"""
        score = 0
        
        # Make URL absolute
        if img_url.startswith('//'):
            img_url = 'https:' + img_url
        elif img_url.startswith('/'):
            img_url = urljoin(base_url, img_url)
        
        # Prefer larger images
        if any(size in img_url.lower() for size in ['large', 'big', '1024', '800', '600']):
            score += 0.3
        
        # Prefer article/content images
        if any(keyword in img_url.lower() for keyword in ['article', 'content', 'story', 'news']):
            score += 0.4
        
        # Avoid icons, logos, ads
        if any(keyword in img_url.lower() for keyword in ['icon', 'logo', 'ad', 'banner', 'thumb']):
            score -= 0.5
        
        # Prefer common image formats
        if any(ext in img_url.lower() for ext in ['.jpg', '.jpeg', '.png', '.webp']):
            score += 0.2
        
        return max(0, score)
    
    def _assess_content_quality(self, result: Dict[str, Any]) -> float:
        """Assess the quality of extracted content"""
        score = 0
        
        # Title quality
        title = result.get('title', '')
        if title:
            score += 0.2
            if len(title) > 10:
                score += 0.1
        
        # Content quality
        content = result.get('content', '')
        if content:
            content_len = len(content)
            if content_len > 100:
                score += 0.3
            if content_len > 500:
                score += 0.2
            if content_len > 1000:
                score += 0.1
        
        # Description quality
        if result.get('description'):
            score += 0.1
        
        # Author and date
        if result.get('author'):
            score += 0.05
        if result.get('date'):
            score += 0.05
        
        # Image
        if result.get('image_url'):
            score += 0.1
        
        return min(score, 1.0)
    
    def _clean_text(self, text: str) -> str:
        """Clean and normalize text"""
        if not text:
            return ''
        
        text = re.sub(r'\s+', ' ', text.strip())
        text = text.replace('\u00a0', ' ').replace('&nbsp;', ' ')
        return text.strip()
    
    def _is_boilerplate(self, text: str) -> bool:
        """Check if text is likely boilerplate/navigation content"""
        boilerplate_patterns = [
            'share this', 'follow us', 'subscribe', 'advertisement',
            'related articles', 'trending now', 'also read', 'read more',
            'terms of service', 'privacy policy', 'copyright'
        ]
        
        text_lower = text.lower()
        return any(pattern in text_lower for pattern in boilerplate_patterns)
    
    def _empty_result(self) -> Dict[str, Any]:
        """Return empty result structure"""
        return {
            'title': '',
            'content': '',
            'description': '',
            'author': '',
            'date': '',
            'image_url': ''
        }
    
    def _update_extraction_history(self, url: str, strategy: ExtractionStrategy, quality: float):
        """Update extraction history for learning"""
        domain = urlparse(url).netloc
        self.extraction_history[domain].append({
            'strategy': strategy,
            'quality': quality,
            'timestamp': datetime.now()
        })
        
        # Keep only recent history
        if len(self.extraction_history[domain]) > 100:
            self.extraction_history[domain] = self.extraction_history[domain][-50:]
        
        # Update strategy success rates
        domain_history = self.extraction_history[domain]
        strategy_scores = [h['quality'] for h in domain_history if h['strategy'] == strategy]
        if strategy_scores:
            self.strategy_success_rates[f"{domain}:{strategy.value}"] = statistics.mean(strategy_scores)
    
    # Helper methods for specific extractions
    def _extract_author_json_ld(self, item: Dict) -> str:
        author = item.get('author', '')
        if isinstance(author, dict):
            return author.get('name', '')
        elif isinstance(author, list) and author:
            return author[0].get('name', '') if isinstance(author[0], dict) else str(author[0])
        return str(author)
    
    def _extract_date_json_ld(self, item: Dict) -> str:
        return item.get('datePublished', '').split('T')[0]
    
    def _extract_image_json_ld(self, item: Dict) -> str:
        image = item.get('image', '')
        if isinstance(image, dict):
            return image.get('url', '')
        elif isinstance(image, list) and image:
            return image[0] if isinstance(image[0], str) else image[0].get('url', '')
        return str(image)
    
    def _get_microdata_prop(self, elem, prop: str) -> str:
        """Extract microdata property"""
        prop_elem = elem.find(attrs={'itemprop': prop})
        if prop_elem:
            return prop_elem.get('content') or prop_elem.get_text()
        return ''
    
    def _try_source_selectors(self, soup: BeautifulSoup, config: SourceConfiguration) -> Dict[str, Any]:
        """Try source-specific selectors"""
        result = self._empty_result()
        
        try:
            selectors = config.primary_selectors
            
            # Title
            if 'title' in selectors:
                title_elem = soup.select_one(selectors['title'])
                if title_elem:
                    result['title'] = self._clean_text(title_elem.get_text())
            
            # Content  
            if 'content' in selectors:
                content_elem = soup.select_one(selectors['content'])
                if content_elem:
                    result['content'] = self._extract_content_from_element(content_elem)
            
            # Author
            if 'author' in selectors:
                author_elem = soup.select_one(selectors['author'])
                if author_elem:
                    result['author'] = self._clean_text(author_elem.get_text())
            
            # Date
            if 'date' in selectors:
                date_elem = soup.select_one(selectors['date'])
                if date_elem:
                    result['date'] = self._clean_text(date_elem.get_text())
            
            # Image
            if 'image' in selectors:
                img_elem = soup.select_one(selectors['image'])
                if img_elem:
                    result['image_url'] = img_elem.get('src', '')
        
        except Exception as e:
            logger.warning(f"Source selector extraction failed: {e}")
        
        return result
    
    def _extract_title_heuristic(self, soup: BeautifulSoup) -> str:
        """Extract title using heuristics"""
        candidates = [
            soup.select_one('h1'),
            soup.select_one('.title'),
            soup.select_one('.headline'),
            soup.select_one('[class*="title"]'),
            soup.find('title')
        ]
        
        for elem in candidates:
            if elem:
                title = self._clean_text(elem.get_text())
                if len(title) > 10:
                    return title
        
        return ''
    
    def _extract_description_heuristic(self, soup: BeautifulSoup) -> str:
        """Extract description using heuristics"""
        # Try meta description first
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc:
            return meta_desc.get('content', '')
        
        # Try other meta tags
        for name in ['twitter:description', 'og:description']:
            meta = soup.find('meta', attrs={'name': name}) or soup.find('meta', property=name)
            if meta:
                return meta.get('content', '')
        
        return ''
    
    def _extract_author_heuristic(self, soup: BeautifulSoup) -> str:
        """Extract author using heuristics"""
        selectors = [
            '.author', '.byline', '[rel="author"]', 
            '.post-author', '.article-author', '[itemprop="author"]'
        ]
        
        for selector in selectors:
            elem = soup.select_one(selector)
            if elem:
                author = self._clean_text(elem.get_text())
                if author and len(author) < 100:  # Reasonable author name length
                    return author
        
        return ''
    
    def _extract_date_heuristic(self, soup: BeautifulSoup) -> str:
        """Extract date using heuristics"""
        # Try time element first
        time_elem = soup.find('time')
        if time_elem:
            return time_elem.get('datetime') or time_elem.get_text()
        
        # Try common date selectors
        date_selectors = [
            '.date', '.post-date', '.article-date', 
            '.published', '.timestamp', '[itemprop="datePublished"]'
        ]
        
        for selector in date_selectors:
            elem = soup.select_one(selector)
            if elem:
                date_text = self._clean_text(elem.get_text())
                if self._looks_like_date(date_text):
                    return date_text
        
        return ''
    
    def _looks_like_date(self, text: str) -> bool:
        """Check if text looks like a date"""
        date_patterns = [
            r'\d{4}-\d{2}-\d{2}',  # 2023-12-25
            r'\d{2}/\d{2}/\d{4}',  # 12/25/2023
            r'\w+ \d{1,2}, \d{4}', # December 25, 2023
        ]
        
        return any(re.search(pattern, text) for pattern in date_patterns)

Base = declarative_base()

class Article(Base):
    __tablename__ = 'articles'
    id = Column(Integer, primary_key=True)
    news_id = Column(String, unique=True)
    source = Column(String, nullable=False)
    url = Column(String, unique=True, nullable=False)
    title = Column(String, nullable=False)
    content = Column(Text)
    description = Column(Text)
    author = Column(String)
    published_date = Column(String)
    image_url = Column(String)
    ai_summary = Column(Text)
    quality_score = Column(Float)
    content_fingerprint = Column(String, unique=True)
    extraction_strategy = Column(String)
    extraction_time = Column(Float)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)

class EnterpriseNewsPipeline:
    """Enterprise-grade news aggregation pipeline"""
    
    def __init__(self, gemini_api_key: str = None, db_path: str = "news_pipeline.db"):
        self.db_path = db_path
        self.use_sqlalchemy = self.db_path.startswith("postgresql://") or os.getenv("DATABASE_URL")
        self.sqlalchemy_engine = None
        self.sqlalchemy_session = None
        if self.use_sqlalchemy:
            db_url = self.db_path if self.db_path.startswith("postgresql://") else os.getenv("DATABASE_URL")
            self.sqlalchemy_engine = create_engine(db_url, echo=False, future=True)
            Base.metadata.create_all(self.sqlalchemy_engine)
            self.Session = sessionmaker(bind=self.sqlalchemy_engine)
        
        # Load Gemini API key with priority: parameter > environment variable > .env file
        self.gemini_api_key = gemini_api_key
        if not self.gemini_api_key:
            self.gemini_api_key = os.getenv('GEMINI_API_KEY')
            if self.gemini_api_key:
                logger.info("Loaded GEMINI_API_KEY from environment variable")
            else:
                logger.warning("GEMINI_API_KEY not found in environment variables or .env file")
        
        # Initialize core components
        self.extractor = AdaptiveContentExtractor()
        self.deduplicator = AdvancedDeduplicator()
        self.session = self._create_session()
        
        # Initialize AI if available
        self.ai_model = None
        if self.gemini_api_key and GEMINI_AVAILABLE:
            try:
                genai.configure(api_key=self.gemini_api_key)
                self.ai_model = genai.GenerativeModel('gemini-2.0-flash')
                logger.info("✅ AI summarization enabled with Gemini")
            except Exception as e:
                logger.error(f"❌ Failed to initialize AI: {e}")
                logger.info("🔄 Falling back to extractive summarization")
        elif not self.gemini_api_key:
            logger.info("ℹ️  No Gemini API key provided - using extractive summarization")
        elif not GEMINI_AVAILABLE:
            logger.warning("⚠️  google-generativeai package not available - using extractive summarization")
        
        # Initialize database
        self._init_database()
        
        # Load existing data
        self._load_processed_articles()
        
        # Source configurations
        self.sources = self._load_source_configurations()
        
        # Performance tracking
        self.metrics = {
            'articles_processed': 0,
            'duplicates_found': 0,
            'extraction_errors': 0,
            'ai_summaries_generated': 0,
            'total_runtime': 0
        }
        
        logger.info("Enterprise News Pipeline initialized")
    
    def _create_session(self) -> requests.Session:
        """Create configured requests session"""
        session = requests.Session()
        
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        })
        
        # Configure retries
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry
        
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def _init_database(self):
        """Initialize SQLite database with comprehensive schema"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Articles table
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
            
            # Source performance tracking
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
            
            # Extraction performance
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS extraction_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source TEXT,
                    url TEXT,
                    strategy TEXT,
                    quality_score REAL,
                    extraction_time REAL,
                    errors TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Pipeline runs
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS pipeline_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    articles_found INTEGER,
                    new_articles INTEGER,
                    duplicates INTEGER,
                    errors INTEGER,
                    runtime_seconds REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create indexes
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_articles_url ON articles(url)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_articles_fingerprint ON articles(content_fingerprint)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_articles_source ON articles(source)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_articles_created ON articles(created_at)')
            
            conn.commit()
    
    def _load_processed_articles(self):
        """Load processed articles for deduplication"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT url, content_fingerprint FROM articles')
                
                for url, fingerprint in cursor.fetchall():
                    self.deduplicator.url_cache.add(url)
                    if fingerprint:
                        self.deduplicator.content_hashes.add(fingerprint)
                
                logger.info(f"Loaded {len(self.deduplicator.url_cache)} processed URLs")
        except Exception as e:
            logger.error(f"Failed to load processed articles: {e}")
    
    def _load_source_configurations(self) -> Dict[str, SourceConfiguration]:
        """Load source configurations with enhanced settings"""
        return {
            'inc42': SourceConfiguration(
                name='Inc42',
                base_url='https://inc42.com',
                list_url='https://inc42.com/buzz/',
                primary_selectors={
                    'articles': 'div.card-wrapper',
                    'title': 'h2.entry-title a',
                    'link': 'h2.entry-title a',
                    'image': 'figure.card-image img',
                    'content': 'div.single-post-content'
                },
                fallback_selectors={
                    'articles': ['.post', '.entry', 'article'],
                    'title': ['h1', '.title', '.headline'],
                    'content': ['.content', '.post-content', 'article']
                },
                request_delay=2.0,
                max_retries=3,
                min_content_length=200
            ),
            
            'entrackr': SourceConfiguration(
                name='Entrackr',
                base_url='https://entrackr.com',
                list_url='https://entrackr.com/news',
                primary_selectors={
                    'articles': 'main div div a, .post-item, article, .news-item',
                    'title': 'h2, h3, .entry-title, .post-title',
                    'link': 'a[href*="/"], a[href*="entrackr"]',
                    'content': 'article .content, .post-content, .entry-content, main article'
                },
                fallback_selectors={
                    'articles': ['.td_module_flex', '.entry', '.post'],
                    'title': ['h1', 'h2', 'h3'],
                    'content': ['.content', 'article', 'main']
                },
                request_delay=2.5,
                max_retries=4
            ),
            
            'moneycontrol': SourceConfiguration(
                name='Moneycontrol',
                base_url='https://www.moneycontrol.com',
                list_url='https://www.moneycontrol.com/news/business/startup/',
                primary_selectors={
                    'articles': '#cagetory li.clearfix',
                    'title': 'h2 a',
                    'link': 'h2 a',
                    'content': '#contentdata',
                    'image': 'section img, .artImg img, .article-image img'
                },
                fallback_selectors={
                    'articles': ['.news-item', '.story', '.article'],
                    'content': ['.content', '.story-content', '.news-content']
                },
                request_delay=3.0,
                max_retries=3
            ),
            
            'startupnews': SourceConfiguration(
                name='StartupNews.fyi',
                base_url='https://startupnews.fyi',
                list_url='https://startupnews.fyi/the-latest/',
                primary_selectors={
                    'articles': '.td_module_flex, .td_module_mx_1, article, .post',
                    'title': 'h3.entry-title a, .td-module-title a, h2.entry-title a',
                    'link': 'h3.entry-title a, .td-module-title a, h2.entry-title a',
                    'content': 'div.tdb-block-inner.td-fix-index, .post-content, .entry-content'
                },
                fallback_selectors={
                    'articles': ['.post', '.entry', 'article'],
                    'content': ['.content', '.post-content', 'article']
                },
                request_delay=2.0
            ),
            
            'indianstartup': SourceConfiguration(
                name='IndianStartupNews',
                base_url='https://indianstartupnews.com',
                list_url='https://indianstartupnews.com/news',
                primary_selectors={
                    'articles': 'div.small-post',
                    'title': 'div.post-title',
                    'link': 'a[href]',
                    'content': 'div#post-container'
                },
                fallback_selectors={
                    'articles': ['.post', '.news-item', '.entry'],
                    'content': ['.content', '.post-content', '.news-content']
                },
                request_delay=2.0
            )
        }
    
    async def _async_fetch_with_retries(self, url: str, config: SourceConfiguration) -> str:
        for attempt in range(config.max_retries):
            try:
                async with httpx.AsyncClient(timeout=config.timeout) as client:
                    response = await client.get(url, headers={'Referer': config.base_url})
                    response.raise_for_status()
                    return response.text
            except httpx.RequestError as e:
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                logger.warning(f"Async fetch attempt {attempt + 1} failed for {url}: {e}")
                if attempt < config.max_retries - 1:
                    logger.info(f"Retrying in {wait_time:.1f}s...")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"All async retry attempts failed for {url}")
                    return None
        return None

    async def async_scrape_source(self, source_name: str, max_articles: int = 20) -> list:
        """Sync wrapper for async_scrape_source for backward compatibility."""
        return asyncio.run(self.async_scrape_source(source_name, max_articles))

    async def async_scrape_source(self, source_name: str, max_articles: int = 20) -> list:
        if source_name not in self.sources:
            logger.error(f"Unknown source: {source_name}")
            return []
        config = self.sources[source_name]
        if config.circuit_breaker.state == "OPEN":
            if self._should_attempt_recovery(config):
                config.circuit_breaker.state = "HALF_OPEN"
                logger.info(f"{source_name}: Circuit breaker HALF_OPEN - attempting recovery")
            else:
                logger.warning(f"{source_name}: Circuit breaker OPEN - skipping")
                return []
        logger.info(f"🔍 [async] Scraping {config.name} (max: {max_articles} articles)")
        articles = []
        start_time = time.time()
        try:
            html = await self._async_fetch_with_retries(config.list_url, config)
            if not html:
                self._handle_source_failure(config, "Failed to fetch listing page")
                return []
            soup = BeautifulSoup(html, 'html.parser')
            article_links = self._extract_article_links(soup, config)
            if not article_links:
                self._handle_source_failure(config, "No article links found")
                return []
            logger.info(f"{config.name}: Found {len(article_links)} potential articles")
            processed_count = 0
            for i, (title, url) in enumerate(article_links[:max_articles]):
                if processed_count >= max_articles:
                    break
                try:
                    if url in self.deduplicator.url_cache:
                        logger.debug(f"Skipping processed URL: {url}")
                        continue
                    article = await self._async_process_single_article(url, title, config)
                    if article:
                        articles.append(article)
                        processed_count += 1
                        logger.info(f"✅ [async] {config.name}: Processed '{title[:60]}...'")
                    await asyncio.sleep(config.request_delay)
                except Exception as e:
                    logger.error(f"Error processing article {url}: {e}")
                    self.metrics['extraction_errors'] += 1
                    continue
            self._handle_source_success(config, len(articles))
            elapsed = time.time() - start_time
            logger.info(f"✅ [async] {config.name}: Completed in {elapsed:.1f}s - {len(articles)} articles")
            return articles
        except Exception as e:
            self._handle_source_failure(config, f"Source scraping failed: {e}")
            return []

    async def _async_process_single_article(self, url: str, title: str, config: SourceConfiguration) -> dict:
        html = await self._async_fetch_with_retries(url, config)
        if not html:
            return None
        content_data, extraction_metrics = self.extractor.extract_content(html, url, config)
        if extraction_metrics.quality_score < 0.3:
            logger.warning(f"Poor quality content for {url} (score: {extraction_metrics.quality_score:.2f})")
            return None
        article = {
            'news_id': self._generate_article_id(url),
            'news_source': config.name,
            'source_url': url,
            'news_title': content_data.get('title') or title,
            'news_content': content_data.get('content', ''),
            'description': content_data.get('description', ''),
            'author': content_data.get('author', ''),
            'news_published_date': content_data.get('date', ''),
            'image_url': content_data.get('image_url', ''),
            'metadata': {
                'extraction_strategy': extraction_metrics.strategy_used.value,
                'extraction_time': extraction_metrics.extraction_time,
                'quality_score': extraction_metrics.quality_score,
                'confidence_score': extraction_metrics.confidence_score,
                'content_length': len(content_data.get('content', '')),
                'scraped_at': datetime.now().isoformat()
            }
        }
        is_duplicate, similarity, duplicate_info = self.deduplicator.check_duplicate(
            article, self._get_recent_articles(config.name, 100)
        )
        if is_duplicate:
            logger.info(f"Duplicate detected: {duplicate_info}")
            self.metrics['duplicates_found'] += 1
            return None
        if self.ai_model:
            article['ai_summary'] = self._generate_ai_summary(
                article['news_title'], article['news_content']
            )
        else:
            article['ai_summary'] = self._generate_fallback_summary(article['news_content'])
        await self._async_save_article(article, extraction_metrics)
        self.metrics['articles_processed'] += 1
        return article

    async def _async_save_article(self, article: dict, metrics: ExtractionMetrics):
        if self.use_sqlalchemy:
            try:
                session = self.Session()
                fingerprint = self.deduplicator.generate_content_fingerprint(
                    article['news_title'], article['news_content'], article['source_url']
                )
                db_article = Article(
                    news_id=article['news_id'],
                    source=article['news_source'],
                    url=article['source_url'],
                    title=article['news_title'],
                    content=article['news_content'],
                    description=article['description'],
                    author=article['author'],
                    published_date=article['news_published_date'],
                    image_url=article['image_url'],
                    ai_summary=article['ai_summary'],
                    quality_score=metrics.quality_score,
                    content_fingerprint=fingerprint,
                    extraction_strategy=metrics.strategy_used.value,
                    extraction_time=metrics.extraction_time,
                    created_at=datetime.now(),
                    updated_at=datetime.now()
                )
                session.add(db_article)
                session.commit()
                session.close()
            except SQLAlchemyError as e:
                logger.error(f"SQLAlchemy save failed: {e}")
        else:
            # fallback to sync
            self._save_article(article, metrics)

    async def async_process_all_sources(self, max_articles_per_source: int, max_workers: int, sources: list = None) -> list:
        start_time = time.time()
        sources_to_run = self.sources.keys()
        if sources:
            sources_to_run = [s for s in sources if s in self.sources]
            if not sources_to_run:
                logger.warning("No valid sources provided or none matched the configuration. Running all sources.")
                sources_to_run = self.sources.keys()
        logger.info(f"🚀 [async] Starting enterprise pipeline - {len(sources_to_run)} sources, {max_workers} workers")
        all_articles = []
        source_results = {}
        self.metrics = defaultdict(int)
        sem = asyncio.Semaphore(max_workers)
        async def run_source(source_name):
            async with sem:
                return await self.async_scrape_source(source_name, max_articles_per_source)
        tasks = [run_source(source_name) for source_name in sources_to_run]
        results = await asyncio.gather(*tasks)
        for source_name, articles in zip(sources_to_run, results):
            all_articles.extend(articles)
            source_results[source_name] = len(articles)
            logger.info(f"✅ [async] {source_name}: {len(articles)} articles collected")
        runtime = time.time() - start_time
        self.metrics['total_runtime'] = runtime
        self._save_pipeline_run(len(all_articles), source_results, runtime)
        self._log_pipeline_summary(all_articles, source_results, runtime)
        # Fallback logic
        if not all_articles:
            fallback = self._load_fallback_sources()
            if fallback:
                logger.warning("All sources failed or returned zero articles. Using fallback_sources from pipeline_config.json.")
                return fallback
        return all_articles

    def process_all_sources(self, 
                            max_articles_per_source: int, 
                            max_workers: int,
                            sources: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Process all configured sources or a specific subset concurrently.
        
        Args:
            max_articles_per_source: Max articles to fetch per source.
            max_workers: Max concurrent threads.
            sources: Optional list of source names to process. If None, all sources are processed.
        
        Returns:
            A list of all unique articles found.
        """
        start_time = time.time()
        
        # Determine which sources to run
        sources_to_run = self.sources.keys()
        if sources:
            sources_to_run = [s for s in sources if s in self.sources]
            if not sources_to_run:
                logger.warning("No valid sources provided or none matched the configuration. Running all sources.")
                sources_to_run = self.sources.keys()
        
        logger.info(f"🚀 Starting enterprise pipeline - {len(sources_to_run)} sources, {max_workers} workers")
        
        all_articles = []
        source_results = {}
        
        # Reset metrics for this run
        self.metrics = defaultdict(int)
        
        # Process sources concurrently
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all scraping tasks
            future_to_source = {
                executor.submit(self.scrape_source, source_name, max_articles_per_source): source_name
                for source_name in sources_to_run
            }
            
            # Collect results with timeout handling
            for future in as_completed(future_to_source, timeout=1800):  # 30 minute timeout
                source_name = future_to_source[future]
                try:
                    articles = future.result()
                    all_articles.extend(articles)
                    source_results[source_name] = len(articles)
                    logger.info(f"✅ {source_name}: {len(articles)} articles collected")
                    
                except Exception as e:
                    logger.error(f"❌ {source_name} failed: {e}")
                    source_results[source_name] = 0
        
        # Update pipeline metrics
        runtime = time.time() - start_time
        self.metrics['total_runtime'] = runtime
        
        # Save pipeline run
        self._save_pipeline_run(len(all_articles), source_results, runtime)
        
        # Generate final report
        self._log_pipeline_summary(all_articles, source_results, runtime)
        
        # Fallback logic
        if not all_articles:
            fallback = self._load_fallback_sources()
            if fallback:
                logger.warning("All sources failed or returned zero articles. Using fallback_sources from pipeline_config.json.")
                return fallback
        return all_articles
    
    def save_to_json(self, articles: List[Dict[str, Any]], filename: str = None) -> str:
        """Save articles to JSON with comprehensive metadata"""
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"news_pipeline_{timestamp}.json"
        
        # Prepare output data with enterprise metadata
        output_data = {
            'metadata': {
                'pipeline_version': '2.0',
                'generated_at': datetime.now().isoformat(),
                'total_articles': len(articles),
                'sources_processed': list(self.sources.keys()),
                'metrics': self.metrics,
                'quality_distribution': self._analyze_quality_distribution(articles),
                'source_breakdown': self._analyze_source_breakdown(articles)
            },
            'articles': articles
        }
        
        # Save with compression for large datasets
        if len(articles) > 100:
            compressed_filename = filename.replace('.json', '.json.gz')
            with gzip.open(compressed_filename, 'wt', encoding='utf-8') as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False)
            logger.info(f"💾 Saved {len(articles)} articles to {compressed_filename} (compressed)")
            return compressed_filename
        else:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False)
            logger.info(f"💾 Saved {len(articles)} articles to {filename}")
            return filename
    
    def _save_article(self, article: dict, metrics) -> None:
        """Save article to database with comprehensive metadata (SQLite or SQLAlchemy/Postgres)"""
        try:
            # Use SQLAlchemy if engine/session is available and not SQLite
            if hasattr(self, 'engine') and hasattr(self, 'Session') and not str(self.engine.url).startswith('sqlite'):
                session = self.Session()
                try:
                    # SQLAlchemy Article model
                    sa_article = Article(
                        news_id=article['news_id'],
                        source=article['news_source'],
                        url=article['source_url'],
                        title=article['news_title'],
                        content=article['news_content'],
                        description=article['description'],
                        author=article['author'],
                        published_date=article['news_published_date'],
                        image_url=article['image_url'],
                        ai_summary=article['ai_summary'],
                        quality_score=metrics.quality_score,
                        content_fingerprint=self.deduplicator.generate_content_fingerprint(
                            article['news_title'],
                            article['news_content'],
                            article['source_url']
                        ),
                        extraction_strategy=metrics.strategy_used.value,
                        extraction_time=metrics.extraction_time
                    )
                    session.add(sa_article)
                    # Extraction metrics (if model exists)
                    if 'ExtractionMetric' in globals():
                        sa_metric = ExtractionMetric(
                            source=article['news_source'],
                            url=article['source_url'],
                            strategy=metrics.strategy_used.value,
                            quality_score=metrics.quality_score,
                            extraction_time=metrics.extraction_time,
                            errors=json.dumps(metrics.errors_encountered)
                        )
                        session.add(sa_metric)
                    session.commit()
                except Exception as e:
                    session.rollback()
                    logger.error(f"Failed to save article (SQLAlchemy): {e}")
                finally:
                    session.close()
            else:
                # Fallback to SQLite logic
                import sqlite3
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.cursor()
                    fingerprint = self.deduplicator.generate_content_fingerprint(
                        article['news_title'],
                        article['news_content'],
                        article['source_url']
                    )
                    cursor.execute('''
                        INSERT INTO articles (
                            news_id, source, url, title, content, description, 
                            author, published_date, image_url, ai_summary,
                            quality_score, content_fingerprint, extraction_strategy,
                            extraction_time
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        article['news_id'],
                        article['news_source'],
                        article['source_url'],
                        article['news_title'],
                        article['news_content'],
                        article['description'],
                        article['author'],
                        article['news_published_date'],
                        article['image_url'],
                        article['ai_summary'],
                        metrics.quality_score,
                        fingerprint,
                        metrics.strategy_used.value,
                        metrics.extraction_time
                    ))
                    cursor.execute('''
                        INSERT INTO extraction_metrics (
                            source, url, strategy, quality_score, extraction_time, errors
                        ) VALUES (?, ?, ?, ?, ?, ?)
                    ''', (
                        article['news_source'],
                        article['source_url'],
                        metrics.strategy_used.value,
                        metrics.quality_score,
                        metrics.extraction_time,
                        json.dumps(metrics.errors_encountered)
                    ))
                    conn.commit()
        except Exception as e:
            logger.error(f"Failed to save article: {e}")

    def get_analytics_dashboard(self) -> Dict[str, Any]:
        """Generate comprehensive analytics dashboard"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Basic stats
                cursor.execute('SELECT COUNT(*) FROM articles')
                total_articles = cursor.fetchone()[0]
                
                cursor.execute('SELECT COUNT(*) FROM articles WHERE created_at >= date("now", "-1 day")')
                recent_articles = cursor.fetchone()[0]
                
                # Source performance
                cursor.execute('''
                    SELECT source, COUNT(*) as count, AVG(quality_score) as avg_quality
                    FROM articles 
                    GROUP BY source 
                    ORDER BY count DESC
                ''')
                source_stats = cursor.fetchall()
                
                # Extraction strategy effectiveness
                cursor.execute('''
                    SELECT extraction_strategy, COUNT(*) as count, AVG(quality_score) as avg_quality
                    FROM articles 
                    WHERE extraction_strategy IS NOT NULL
                    GROUP BY extraction_strategy
                    ORDER BY avg_quality DESC
                ''')
                strategy_stats = cursor.fetchall()
                
                # Quality distribution
                cursor.execute('''
                    SELECT 
                        CASE 
                            WHEN quality_score >= 0.8 THEN 'Excellent'
                            WHEN quality_score >= 0.6 THEN 'Good'
                            WHEN quality_score >= 0.4 THEN 'Fair'
                            ELSE 'Poor'
                        END as quality_tier,
                        COUNT(*) as count
                    FROM articles 
                    WHERE quality_score IS NOT NULL
                    GROUP BY quality_tier
                ''')
                quality_distribution = cursor.fetchall()
                
                # Recent pipeline runs
                cursor.execute('''
                    SELECT articles_found, new_articles, duplicates, runtime_seconds, created_at
                    FROM pipeline_runs 
                    ORDER BY created_at DESC 
                    LIMIT 10
                ''')
                recent_runs = cursor.fetchall()
                
                return {
                    'summary': {
                        'total_articles': total_articles,
                        'articles_last_24h': recent_articles,
                        'current_metrics': self.metrics
                    },
                    'source_performance': [
                        {'source': s[0], 'count': s[1], 'avg_quality': round(s[2], 3) if s[2] else 0}
                        for s in source_stats
                    ],
                    'extraction_strategies': [
                        {'strategy': s[0], 'count': s[1], 'avg_quality': round(s[2], 3) if s[2] else 0}
                        for s in strategy_stats
                    ],
                    'quality_distribution': [
                        {'tier': q[0], 'count': q[1]}
                        for q in quality_distribution
                    ],
                    'recent_runs': [
                        {
                            'articles_found': r[0],
                            'new_articles': r[1], 
                            'duplicates': r[2],
                            'runtime': round(r[3], 1),
                            'timestamp': r[4]
                        }
                        for r in recent_runs
                    ]
                }
                
        except Exception as e:
            logger.error(f"Failed to generate analytics: {e}")
            return {'error': str(e)}
    
    # Helper methods
    def _generate_article_id(self, url: str) -> str:
        """Generate unique article ID"""
        url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
        timestamp = int(time.time())
        return f"art_{timestamp}_{url_hash}"
    
    def _clean_text(self, text: str) -> str:
        """Clean and normalize text"""
        if not text:
            return ''
        text = re.sub(r'\s+', ' ', text.strip())
        text = text.replace('\u00a0', ' ').replace('&nbsp;', ' ')
        return text.strip()
    
    def _should_attempt_recovery(self, config: SourceConfiguration) -> bool:
        """Check if circuit breaker should attempt recovery"""
        if not config.circuit_breaker.last_failure_time:
            return True
        
        time_since_failure = datetime.now() - config.circuit_breaker.last_failure_time
        return time_since_failure.total_seconds() > config.circuit_breaker.recovery_timeout
    
    def _handle_source_failure(self, config: SourceConfiguration, error: str):
        """Handle source failure and update circuit breaker"""
        config.circuit_breaker.failure_count += 1
        config.circuit_breaker.last_failure_time = datetime.now()
        
        if config.circuit_breaker.failure_count >= config.circuit_breaker.failure_threshold:
            config.circuit_breaker.state = "OPEN"
            logger.warning(f"{config.name}: Circuit breaker OPEN due to {config.circuit_breaker.failure_count} failures")
        
        logger.error(f"{config.name}: {error}")
    
    def _handle_source_success(self, config: SourceConfiguration, articles_count: int):
        """Handle successful source processing"""
        config.circuit_breaker.failure_count = 0
        config.circuit_breaker.state = "CLOSED"
        config.last_success = datetime.now()
        
        # Update success rate (simple moving average)
        if articles_count > 0:
            config.success_rate = min(config.success_rate * 0.9 + 0.1, 1.0)
        else:
            config.success_rate = config.success_rate * 0.95
    
    def _get_recent_articles(self, source: str, limit: int) -> List[Dict[str, Any]]:
        """Get recent articles for duplicate checking"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT news_title, news_content, source_url
                    FROM articles 
                    WHERE source = ? 
                    ORDER BY created_at DESC 
                    LIMIT ?
                ''', (source, limit))
                
                return [
                    {
                        'news_title': row[0],
                        'news_content': row[1],
                        'source_url': row[2]
                    }
                    for row in cursor.fetchall()
                ]
        except Exception:
            return []
    
    def _save_pipeline_run(self, total_articles: int, source_results: Dict[str, int], runtime: float):
        """Save pipeline run statistics"""
        try:
            new_articles = sum(source_results.values())
            duplicates = self.metrics['duplicates_found']
            errors = self.metrics['extraction_errors']
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO pipeline_runs (articles_found, new_articles, duplicates, errors, runtime_seconds)
                    VALUES (?, ?, ?, ?, ?)
                ''', (total_articles, new_articles, duplicates, errors, runtime))
                conn.commit()
                
        except Exception as e:
            logger.error(f"Failed to save pipeline run: {e}")
    
    def _log_pipeline_summary(self, articles: List[Dict], source_results: Dict[str, int], runtime: float):
        """Log comprehensive pipeline summary"""
        logger.info(f"\n{'='*60}")
        logger.info(f"📊 ENTERPRISE PIPELINE SUMMARY")
        logger.info(f"{'='*60}")
        logger.info(f"🎯 Total Articles: {len(articles)}")
        logger.info(f"⏱️  Runtime: {runtime:.1f} seconds")
        logger.info(f"🔄 Duplicates Found: {self.metrics['duplicates_found']}")
        logger.info(f"❌ Extraction Errors: {self.metrics['extraction_errors']}")
        logger.info(f"🤖 AI Summaries: {self.metrics['ai_summaries_generated']}")
        
        logger.info(f"\n📈 Source Breakdown:")
        for source, count in source_results.items():
            logger.info(f"   • {source}: {count} articles")
        
        if articles:
            avg_quality = statistics.mean([
                a['metadata']['quality_score'] 
                for a in articles 
                if 'metadata' in a and 'quality_score' in a['metadata']
            ])
            logger.info(f"\n📊 Average Quality Score: {avg_quality:.3f}")
        
        logger.info(f"{'='*60}")
    
    def _analyze_quality_distribution(self, articles: List[Dict]) -> Dict[str, int]:
        """Analyze quality distribution of articles"""
        distribution = {'excellent': 0, 'good': 0, 'fair': 0, 'poor': 0}
        
        for article in articles:
            quality = article.get('metadata', {}).get('quality_score', 0)
            if quality >= 0.8:
                distribution['excellent'] += 1
            elif quality >= 0.6:
                distribution['good'] += 1
            elif quality >= 0.4:
                distribution['fair'] += 1
            else:
                distribution['poor'] += 1
        
        return distribution
    
    def _analyze_source_breakdown(self, articles: List[Dict]) -> Dict[str, int]:
        """Analyze articles by source"""
        breakdown = {}
        for article in articles:
            source = article.get('news_source', 'Unknown')
            breakdown[source] = breakdown.get(source, 0) + 1
        return breakdown

    def _generate_ai_summary(self, title: str, content: str) -> str:
        """Generate AI summary with robust error handling and exponential backoff for quota errors"""
        if not self.ai_model or not content:
            return self._generate_fallback_summary(content)
        max_retries = 5
        for attempt in range(max_retries):
            try:
                prompt = f"""
                Write a professional news summary in exactly 55-60 words. Focus on key facts, main players, and significance.
                
                Title: {title}
                Content: {content[:3000]}
                
                Requirements:
                - Exactly 55-60 words
                - Professional journalistic tone  
                - Include key numbers/amounts if mentioned
                - Focus on WHO, WHAT, HOW MUCH, WHY important
                
                Summary:
                """
                response = self.ai_model.generate_content(prompt)
                summary = response.text.strip()
                word_count = len(summary.split())
                if 50 <= word_count <= 65:
                    self.metrics['ai_summaries_generated'] += 1
                    return summary
                else:
                    logger.warning(f"AI summary word count: {word_count} (expected 55-60)")
                    return summary
            except Exception as e:
                # Check for quota or rate limit error
                err_str = str(e).lower()
                if 'quota' in err_str or '429' in err_str or 'rate limit' in err_str:
                    wait_time = 2 ** attempt
                    logger.warning(f"Gemini API quota/rate error, retrying in {wait_time}s (attempt {attempt+1}/{max_retries})")
                    time.sleep(wait_time)
                    continue
                logger.error(f"AI summarization failed: {e}")
                break
        logger.error("Gemini API failed after retries, using fallback summary.")
        return self._generate_fallback_summary(content)

    def _generate_fallback_summary(self, content: str) -> str:
        """Generate fallback summary using extractive methods"""
        if not content:
            return ""
        try:
            # Split into sentences
            if 'nltk' in globals() and NLTK_AVAILABLE:
                from nltk.tokenize import sent_tokenize
                sentences = sent_tokenize(content)
            else:
                sentences = content.split('. ')
            if not sentences:
                return content[:300] + "..." if len(content) > 300 else content
            # Score sentences based on position and length
            scored_sentences = []
            for i, sentence in enumerate(sentences[:10]):  # First 10 sentences
                score = 0
                # Position score (earlier sentences are more important)
                score += (10 - i) * 0.1
                # Length score (prefer medium-length sentences)
                length = len(sentence.split())
                if 10 <= length <= 30:
                    score += 0.5
                elif 5 <= length <= 50:
                    score += 0.2
                # Keyword score
                important_words = ['raises', 'funding', 'million', 'billion', 'startup', 'company', 'announced']
                for word in important_words:
                    if word.lower() in sentence.lower():
                        score += 0.3
                scored_sentences.append((sentence, score))
            # Sort by score and take top sentences
            scored_sentences.sort(key=lambda x: x[1], reverse=True)
            # Build summary
            summary_sentences = []
            total_words = 0
            target_words = 55
            for sentence, score in scored_sentences:
                words = len(sentence.split())
                if total_words + words <= target_words + 10:  # Allow some flexibility
                    summary_sentences.append(sentence)
                    total_words += words
                    if total_words >= target_words:
                        break
            summary = ' '.join(summary_sentences)
            # Trim if too long
            if len(summary.split()) > 65:
                words = summary.split()[:60]
                summary = ' '.join(words) + '...'
            return summary
        except Exception as e:
            logger.error(f"Fallback summarization failed: {e}")
            return content[:300] + "..." if len(content) > 300 else content

    def _load_fallback_sources(self):
        try:
            for config_file in ['pipeline_config.json', 'config.json', 'enterprise_config.json']:
                if os.path.exists(config_file):
                    with open(config_file, 'r') as f:
                        data = json.load(f)
                        if 'fallback_sources' in data:
                            return data['fallback_sources']
        except Exception as e:
            logger.error(f"Failed to load fallback_sources: {e}")
        return []

    def scrape_source(self, source_name: str, max_articles: int = 20) -> list:
        """Sync wrapper for async_scrape_source for backward compatibility."""
        return asyncio.run(self.async_scrape_source(source_name, max_articles))

    def generate_fallback_summary(self, content: str) -> str:
        """Public wrapper for fallback summary generation."""
        return self._generate_fallback_summary(content)

    def _extract_article_links(self, soup: BeautifulSoup, config) -> list:
        """Extract article links using multiple strategies"""
        links = []
        
        # Try primary selectors
        try:
            articles_selector = config.primary_selectors.get('articles', '')
            title_selector = config.primary_selectors.get('title', '')
            link_selector = config.primary_selectors.get('link', '')
            
            article_elements = soup.select(articles_selector)
            
            for elem in article_elements:
                # Extract title and link
                title_elem = elem.select_one(title_selector) if title_selector else elem
                link_elem = elem.select_one(link_selector) if link_selector else title_elem
                
                if title_elem and link_elem:
                    title = self._clean_text(title_elem.get_text())
                    url = link_elem.get('href')
                    
                    if title and url:
                        # Make URL absolute
                        if url.startswith('/'):
                            url = urljoin(config.base_url, url)
                        elif not url.startswith('http'):
                            url = urljoin(config.list_url, url)
                        
                        links.append((title, url))
            
            if links:
                return links
                
        except Exception as e:
            logger.warning(f"Primary selector extraction failed: {e}")
        
        # Try fallback selectors
        if hasattr(config, 'fallback_selectors'):
            for article_selector in config.fallback_selectors.get('articles', []):
                try:
                    elements = soup.select(article_selector)
                    for elem in elements:
                        link_elem = elem.find('a', href=True)
                        if link_elem:
                            title = self._clean_text(link_elem.get_text() or elem.get_text())
                            url = link_elem.get('href')
                            
                            if title and url:
                                if url.startswith('/'):
                                    url = urljoin(config.base_url, url)
                                links.append((title, url))
                    
                    if links:
                        break
                        
                except Exception as e:
                    logger.debug(f"Fallback selector {article_selector} failed: {e}")
                    continue
        
        return links

# Factory function for easy instantiation
def create_enterprise_pipeline(gemini_api_key: str = None, db_path: str = "news_pipeline.db") -> EnterpriseNewsPipeline:
    """Create enterprise pipeline instance"""
    return EnterpriseNewsPipeline(gemini_api_key=gemini_api_key, db_path=db_path)

# Example usage for testing
if __name__ == "__main__":
    pipeline = create_enterprise_pipeline()
    articles = pipeline.process_all_sources(max_articles_per_source=5)
    filename = pipeline.save_to_json(articles)
    
    print(f"\n🎉 Pipeline completed successfully!")
    print(f"📁 Output: {filename}")
    print(f"📊 Analytics available via pipeline.get_analytics_dashboard()")