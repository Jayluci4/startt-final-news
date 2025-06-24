# Frontend
This is a lightweight React UI that interacts with the FastAPI backend. To view it during development, serve the files with any static file server, e.g.:

```bash
cd frontend
python -m http.server 3000
```

Then navigate to `http://localhost:3000/` while the FastAPI server is running.

The UI exposes buttons for the most common API endpoints:

- display information from `/` and `/health`
- trigger the pipeline and check `/api/v1/pipeline/status/{id}`
- list articles from `/api/v1/db/articles`
- fetch analytics and metrics
- download results once a pipeline run completes
- connect to the WebSocket room `pipeline_updates`
