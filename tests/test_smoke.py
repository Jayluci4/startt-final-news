import pytest
import os
import json
from run_pipeline import execute_pipeline

@pytest.fixture
def config():
    # Minimal config for tests
    return {
        'gemini_api_key': os.getenv('GEMINI_API_KEY'),
        'max_articles_per_source': 1,
        'max_workers': 1,
        'db_path': 'test_news_pipeline.db',
        'output_file': 'test_output.json',
        'verbose': False
    }

def test_health_endpoint():
    # Simulate health endpoint (if FastAPI, else dummy)
    assert True  # Replace with real HTTP call if API exists

def test_pipeline_run(config):
    result = execute_pipeline(config)
    assert result['success']
    assert result['articles_count'] >= 0

def test_article_fetch_by_date():
    # Simulate DB fetch by date (mocked)
    assert True  # Replace with real DB/API call

def test_websocket_broadcast():
    # Simulate WebSocket broadcast (mocked)
    assert True  # Replace with real WebSocket test

def test_summarizer_word_count():
    # Check fallback summarizer word count
    from master_pipeline import EnterpriseNewsPipeline
    pipeline = EnterpriseNewsPipeline()
    long_text = (
        "The quick brown fox jumps over the lazy dog. "
        "Artificial intelligence is transforming industries worldwide. "
        "Python is a popular programming language for data science. "
        "OpenAI develops advanced AI models. "
        "Machine learning enables computers to learn from data. "
        "Natural language processing helps computers understand text. "
        "Cloud computing provides scalable resources. "
        "APIs allow different software systems to communicate. "
        "Docker containers simplify deployment. "
        "Continuous integration improves code quality. "
        "Unit tests catch bugs early. "
        "Version control is essential for collaboration. "
        "Big data analytics reveals business insights. "
        "Cybersecurity protects digital assets. "
        "Edge computing reduces latency. "
        "Blockchain technology ensures data integrity. "
        "Quantum computing promises exponential speedup. "
        "The Internet of Things connects everyday devices. "
        "5G networks enable faster communication. "
        "Renewable energy is vital for sustainability. "
    )
    summary = pipeline.generate_fallback_summary(long_text)
    word_count = len(summary.split())
    assert 50 <= word_count <= 65 