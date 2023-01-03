use std::io;

use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
   #[error(transparent)]
   IoError(#[from] io::Error)
}
