# startt-final-news
Startt News Intelligence API
A high-performance, enterprise-grade news aggregation and analysis pipeline designed to scrape, process, and serve news from top Indian startup media outlets. The system features an advanced FastAPI backend, AI-powered summarization with Google Gemini, real-time WebSocket updates, and a robust, self-healing architecture.

🚀 Key Features
Multi-Source Aggregation: Concurrently pulls articles from multiple sources:

Inc42

Entrackr

Moneycontrol (Startups)

StartupNews.fyi

IndianStartupNews

Adaptive Content Extraction: Utilizes a multi-strategy approach (JSON-LD, Microdata, OpenGraph, Heuristics) to reliably extract content from complex web pages.

AI-Powered Summarization: Leverages Google Gemini for concise, professional news summaries, with a robust extractive fallback mechanism.

Advanced Deduplication: Employs a multi-algorithm system (URL hashes, content fingerprinting, and semantic similarity with Sentence-Transformers) to prevent duplicate entries.

Robust Data Persistence: Stores all processed articles and pipeline metrics in a local SQLite database (news_pipeline.db).

Concurrent Processing: Uses ThreadPoolExecutor to efficiently scrape multiple sources in parallel.

High-Performance API: Built with FastAPI, offering asynchronous endpoints, Pydantic data validation, and automatic OpenAPI documentation.

Real-Time Updates: WebSocket endpoint for broadcasting live pipeline status and updates.

Intelligent Caching: Multi-tier caching system (in-memory and Redis) to accelerate API responses.

Rate Limiting: Integrated token bucket rate limiter to protect the API from abuse.

Health Checks & Monitoring: Comprehensive /health endpoint and Prometheus metrics for system monitoring.

Flexible Configuration: Easily manage pipeline behavior via pipeline_config.json and environment variables.

🏗️ Architecture Overview
The project is composed of four main components:

master_pipeline.py: The heart of the data processing engine. It contains the logic for scraping, HTML parsing, content extraction, deduplication, AI summarization, and database interactions. It is designed to be completely independent and can run on its own.

run_pipeline.py: A command-line interface (CLI) to execute the master_pipeline. It handles configuration loading, environment validation, and provides a clean way to trigger pipeline runs from the terminal or a cron job.

main.py: A powerful FastAPI application that serves the processed data via a RESTful API. It provides endpoints to run the pipeline, fetch articles, view analytics, and connect to a real-time WebSocket feed. It uses the run_pipeline script to trigger background processing.

pipeline_config.json: A JSON file to define the operational parameters of the pipeline, such as API keys, number of articles to scrape, and concurrency settings.

🛠️ Setup and Installation
1. Prerequisites
Python 3.8+

An API key for Google Gemini (for AI summarization)

2. Clone the Repository
git clone [https://github.com/your-username/startt_news_dashboard.git](https://github.com/your-username/startt_final_news.git)
cd startt_fianl_news

3. Install Dependencies
It is highly recommended to create a virtual environment.

python -m venv venv
source venv/bin/activate  # On Windows, use `venv\Scripts\activate`

pip install -r requirements.txt

(You will need to create a requirements.txt file. See the suggestion below.)

Suggested requirements.txt:

# Core
fastapi
uvicorn[standard]
requests
beautifulsoup4
pydantic

# AI & ML
google-generativeai
scikit-learn
sentence-transformers
nltk
numpy

# API & Performance
redis
aiofiles
prometheus-client

4. Configure the Pipeline
Create a file named pipeline_config.json in the root directory and add your configuration. Your Gemini API key is required for AI features.

pipeline_config.json Example:

{
  "gemini_api_key": "YOUR_GEMINI_API_KEY_HERE",
  "max_articles_per_source": 25,
  "max_workers": 5,
  "output_file": "startup_news_results.json",
  "verbose": true
}

Alternatively, you can set the Gemini key as an environment variable:

export GEMINI_API_KEY="YOUR_GEMINI_API_KEY_HERE"

▶️ How to Run
Running the Pipeline via CLI
You can execute a pipeline run directly from the terminal using run_pipeline.py. This will scrape the sources, process the articles, and save them to the news_pipeline.db database.

python run_pipeline.py

Optional Arguments:

--articles <num>: Set the max number of articles to scrape per source.

--workers <num>: Set the number of concurrent workers.

--sources <name1> <name2>: Scrape only specific sources (e.g., inc42 entrackr).

--analytics: Show detailed analytics after the run.

--reset-db: Clear the database before running.

Running the API Server
To start the FastAPI server, use uvicorn. This will make the API endpoints and WebSocket available.

uvicorn main:app --host 0.0.0.0 --port 8000 --reload

--reload: Enables hot-reloading for development. Remove this in production.

The API documentation will be available at http://localhost:8000/docs (Swagger UI) and http://localhost:8000/redoc.

⚙️ API Endpoints
The API provides several endpoints to interact with the news intelligence system.

Method

Endpoint

Description

POST

/api/v1/pipeline/run

Asynchronously triggers a new pipeline run.

GET

/api/v1/db/articles

Fetches articles from the database with advanced filters.

GET

/api/v1/analytics

Retrieves comprehensive pipeline and system analytics.

GET

/health

Provides a detailed health check of system components.

WS

/ws/{room}

WebSocket endpoint for real-time updates.

🤝 Contributing
Contributions are welcome! Please follow these steps:

Fork the repository.

Create a new branch (git checkout -b feature/your-feature-name).

Make your changes.

Commit your changes (git commit -m 'Add some feature').

Push to the branch (git push origin feature/your-feature-name).

Open a Pull Request.
