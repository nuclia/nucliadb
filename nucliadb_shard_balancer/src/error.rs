use thiserror::Error;
use tonic::{Status, Code};

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    HttpError(#[from] reqwest::Error),
    #[error(transparent)]
    TransportError(#[from] tonic::transport::Error),
    #[error("RPC call failure ({0})")]
    RpcError(Code),
}

impl From<Status> for Error {
    fn from(status: Status) -> Self {
        match status.code() {
            Code::Ok => panic!("cannot construct error from valid gRPC status"),
            code => Self::RpcError(code),
        }
    }
}
