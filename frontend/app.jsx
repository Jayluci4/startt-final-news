const { useState, useEffect } = React;

function App() {
  const [articles, setArticles] = useState([]);
  const [loading, setLoading] = useState(false);
  const [pipelineId, setPipelineId] = useState(null);
  const [pipelineStatus, setPipelineStatus] = useState(null);

  // Fetch articles on mount
  useEffect(() => {
    fetch('/api/v1/db/articles')
      .then(res => res.json())
      .then(data => {
        setArticles(data.articles || []);
      })
      .catch(err => console.error('Error fetching articles:', err));
  }, []);

  // Poll pipeline status if running
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

  return (
    <div>
      <h1 className="mb-4">Startt News Intelligence</h1>
      <button className="btn btn-primary mb-3" onClick={runPipeline} disabled={loading}>
        {loading ? 'Running...' : 'Run Pipeline'}
      </button>
      {pipelineStatus && (
        <div className="alert alert-info">Pipeline status: {pipelineStatus}</div>
      )}
      <h2>Articles</h2>
      <ul className="list-group">
        {articles.map(article => (
          <li key={article.id} className="list-group-item">
            <a href={article.url} target="_blank" rel="noopener noreferrer">
              {article.title}
            </a>
          </li>
        ))}
      </ul>
    </div>
  );
}

ReactDOM.createRoot(document.getElementById('root')).render(<App />);
