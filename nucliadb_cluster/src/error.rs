use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Cluster error kinds.
#[derive(Error, Debug, Serialize, Deserialize)]
pub enum ClusterError {
    /// Create cluster error.
    #[error("Failed to create cluster: `{message}`")]
    CreateClusterError {
        /// Underlying error message.
        message: String,
    },

    /// Port binding error.
    #[error("Failed to bind to UDP port `{port}` for the gossip membership protocol: `{message}`")]
    UDPPortBindingError {
        /// Port number.
        port: u16,
        /// Underlying error message.
        message: String,
    },

    /// Read host key error.
    #[error("Failed to read host key: `{message}`")]
    ReadHostKeyError {
        /// Underlying error message.
        message: String,
    },

    /// Write host key error.
    #[error("Failed to write host key: `{message}`")]
    WriteHostKeyError {
        /// Underlying error message.
        message: String,
    },

    /// Incorrect node type
    #[error("Failed to parse node type from string: `{message}`")]
    ParseNodeTypeError { message: String },

    #[error("Error from chitchat crate: `{message}`")]
    ChitchatError { message: String },
}

impl From<ClusterError> for tonic::Status {
    fn from(error: ClusterError) -> tonic::Status {
        let code = match error {
            ClusterError::CreateClusterError { .. } => tonic::Code::Internal,
            ClusterError::UDPPortBindingError { .. } => tonic::Code::PermissionDenied,
            ClusterError::ReadHostKeyError { .. } => tonic::Code::Internal,
            ClusterError::WriteHostKeyError { .. } => tonic::Code::Internal,
            ClusterError::ParseNodeTypeError { .. } => tonic::Code::Internal,
            ClusterError::ChitchatError { .. } => tonic::Code::Internal,
        };
        let message = error.to_string();
        tonic::Status::new(code, message)
    }
}

/// Generic Result type for cluster operations.
pub type ClusterResult<T> = Result<T, ClusterError>;
