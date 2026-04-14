#[derive(Debug, thiserror::Error)]
pub enum LoaderError {
    #[error("format error: {0}")]
    Format(#[from] frostmap_format::Error),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("arrow error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),

    #[error("source error: {0}")]
    Source(Box<dyn std::error::Error + Send + Sync>),

    #[error("thread pool error: {0}")]
    ThreadPool(#[from] rayon::ThreadPoolBuildError),

    #[error("task join error: {0}")]
    Join(#[from] tokio::task::JoinError),
}
