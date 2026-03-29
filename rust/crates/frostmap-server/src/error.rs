#[derive(Debug, thiserror::Error)]
pub enum ServeError {
    #[error("blocking task panicked: {0}")]
    BlockingTaskPanicked(String),

    #[error(transparent)]
    Format(#[from] frostmap_format::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("key has no dataset prefix (expected <dataset>:<key>)")]
    MissingDatasetPrefix,

    #[error("dataset name is not valid UTF-8")]
    InvalidDatasetName,
}
