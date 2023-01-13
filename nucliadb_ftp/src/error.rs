use std::io;

use thiserror::Error;

/// The error that may occur when publishing/receiving files/directories.
#[derive(Debug, Error)]
pub enum Error {
    #[error("path cannot end by '..' or be the root path '/'")]
    InvalidPath,
    #[error(transparent)]
    IoError(#[from] io::Error),
}
