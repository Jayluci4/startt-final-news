# syntax=docker/dockerfile:1
FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Optional: install system dependencies for lxml, etc.
RUN apt-get update && apt-get install -y gcc libpq-dev && rm -rf /var/lib/apt/lists/*

# Healthcheck (simple)
HEALTHCHECK CMD curl --fail http://localhost:8000/health || exit 1

EXPOSE 8000

CMD ["python", "run_pipeline.py"] 