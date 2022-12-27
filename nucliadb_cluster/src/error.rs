use thiserror::Error;

/// The error that may occur when manipulating the node.
#[derive(Debug, Error)]
pub enum Error {
    #[error("cannot start the node: {0}")]
    Start(anyhow::Error),
    #[error("cannot shutdown the node: {0}")]
    Shutdown(anyhow::Error),
}
