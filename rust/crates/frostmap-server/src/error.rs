#[derive(Debug, thiserror::Error)]
pub enum ServeError {
    #[error("blocking task panicked")]
    BlockingTaskPanicked,

    #[error(transparent)]
    Format(#[from] frostmap_format::Error),
}
