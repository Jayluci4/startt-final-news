const { useState, useEffect, useRef } = React;

function App() {
  const [rootInfo, setRootInfo] = useState(null);
  const [health, setHealth] = useState(null);
  const [articles, setArticles] = useState([]);
  const [pipelineId, setPipelineId] = useState(null);
  const [pipelineStatus, setPipelineStatus] = useState(null);
  const [analytics, setAnalytics] = useState(null);
  const [metrics, setMetrics] = useState('');
  const [loading, setLoading] = useState(false);
  const [wsMessages, setWsMessages] = useState([]);
  const wsRef = useRef(null);

  useEffect(() => {
    fetchRoot();
    fetchArticles();
  }, []);

  const fetchRoot = () => {
    fetch('/')
      .then(res => res.json())
      .then(setRootInfo)
      .catch(err => console.error('Error fetching root:', err));
  };

  const fetchHealth = () => {
    fetch('/health')
      .then(res => res.json())
      .then(setHealth)
      .catch(err => console.error('Error fetching health:', err));
  };

  const fetchArticles = () => {
    fetch('/api/v1/db/articles')
      .then(res => res.json())
      .then(data => setArticles(data.articles || []))
      .catch(err => console.error('Error fetching articles:', err));
  };

  const runPipeline = () => {
    setLoading(true);
    fetch('/api/v1/pipeline/run', { method: 'POST' })
      .then(res => res.json())
      .then(data => {
        setPipelineId(data.execution_id);
        setPipelineStatus('started');
      })
      .catch(err => console.error('Error running pipeline:', err))
      .finally(() => setLoading(false));
  };

  useEffect(() => {
    if (!pipelineId) return;
    const interval = setInterval(() => {
      fetch(`/api/v1/pipeline/status/${pipelineId}`)
        .then(res => res.json())
        .then(data => {
          setPipelineStatus(data.status);
          if (data.status === 'completed' || data.status === 'failed') {
            clearInterval(interval);
          }
        })
        .catch(err => {
          console.error('Error fetching status:', err);
          clearInterval(interval);
        });
    }, 3000);
    return () => clearInterval(interval);
  }, [pipelineId]);

  const downloadResults = (format = 'json') => {
    if (!pipelineId) return;
    window.open(`/api/v1/pipeline/results/${pipelineId}/download?format=${format}`);
  };

  const fetchAnalytics = () => {
    fetch('/api/v1/analytics')
      .then(res => res.json())
      .then(setAnalytics)
      .catch(err => console.error('Error fetching analytics:', err));
  };

  const fetchMetrics = () => {
    fetch('/metrics')
      .then(res => res.text())
      .then(setMetrics)
      .catch(err => console.error('Error fetching metrics:', err));
  };

  const connectWebSocket = () => {
    if (wsRef.current) wsRef.current.close();
    const protocol = location.protocol === 'https:' ? 'wss' : 'ws';
    wsRef.current = new WebSocket(`${protocol}://${location.host}/ws/pipeline_updates`);
    wsRef.current.onmessage = (event) => {
      setWsMessages(msgs => [...msgs, event.data]);
    };
    wsRef.current.onclose = () => {
      setWsMessages(msgs => [...msgs, 'WebSocket disconnected']);
    };
  };

  return (
    <div>
      <h1 className="mb-4">Startt News Intelligence</h1>

      <section className="mb-4">
        <h2>API Info</h2>
        <button className="btn btn-outline-primary mb-2" onClick={fetchRoot}>Refresh</button>
        {rootInfo && (
          <pre style={{ whiteSpace: 'pre-wrap' }}>{JSON.stringify(rootInfo, null, 2)}</pre>
        )}
      </section>

      <section className="mb-4">
        <h2>Pipeline</h2>
        <button className="btn btn-primary me-2" onClick={runPipeline} disabled={loading}>
          {loading ? 'Running...' : 'Run Pipeline'}
        </button>
        {pipelineId && (
          <>
            <button className="btn btn-secondary me-2" onClick={() => downloadResults('json')}>
              Download JSON
            </button>
            <button className="btn btn-secondary me-2" onClick={() => downloadResults('csv')}>
              Download CSV
            </button>
          </>
        )}
        {pipelineStatus && (
          <div className="alert alert-info mt-2">Status: {pipelineStatus}</div>
        )}
      </section>

      <section className="mb-4">
        <h2>Articles</h2>
        <button className="btn btn-outline-primary mb-2" onClick={fetchArticles}>Refresh</button>
        <ul className="list-group">
          {articles.map(article => (
            <li key={article.news_id} className="list-group-item">
              <a href={article.source_url} target="_blank" rel="noopener noreferrer">
                {article.news_title}
              </a>
            </li>
          ))}
        </ul>
      </section>

      <section className="mb-4">
        <h2>Analytics</h2>
        <button className="btn btn-outline-primary mb-2" onClick={fetchAnalytics}>Load Analytics</button>
        {analytics && (
          <pre style={{ whiteSpace: 'pre-wrap' }}>{JSON.stringify(analytics, null, 2)}</pre>
        )}
      </section>

      <section className="mb-4">
        <h2>Health & Metrics</h2>
        <button className="btn btn-outline-primary me-2" onClick={fetchHealth}>Health</button>
        <button className="btn btn-outline-primary" onClick={fetchMetrics}>Metrics</button>
        {health && (
          <pre className="mt-2" style={{ whiteSpace: 'pre-wrap' }}>{JSON.stringify(health, null, 2)}</pre>
        )}
        {metrics && (
          <pre className="mt-2" style={{ whiteSpace: 'pre-wrap' }}>{metrics}</pre>
        )}
      </section>

      <section className="mb-4">
        <h2>WebSocket</h2>
        <button className="btn btn-outline-primary" onClick={connectWebSocket}>Connect</button>
        <div className="mt-2">
          {wsMessages.map((m, idx) => (
            <div key={idx}><code>{m}</code></div>
          ))}
        </div>
      </section>
    </div>
  );
}

ReactDOM.createRoot(document.getElementById('root')).render(<App />);
