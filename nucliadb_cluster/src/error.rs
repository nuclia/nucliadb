use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Cluster error kinds.
#[derive(Error, Debug, Serialize, Deserialize)]
pub enum Error {
    /// Read host key error.
    #[error("Failed to read host key: `{0}`")]
    ReadHostKey(String),
    /// Write host key error.
    #[error("Failed to write host key: `{0}`")]
    WriteHostKey(String),
    /// Incorrect node type
    #[error("Unknown node type: `{0}`")]
    UnknownNodeType(String),
    #[error("Cannot get state from node `{0}`")]
    MissingNodeState(String),
    #[error("Cannot start cluster: {0}")]
    CannotStartCluster(String),
}
