#[derive(Debug)]
pub(crate) enum DiskStructError {
    IOErr(std::io::Error),
    BincodeErr(Box<bincode::ErrorKind>),
    InitErr,
}

impl From<std::io::Error> for DiskStructError {
    fn from(e: std::io::Error) -> Self {
        DiskStructError::IOErr(e)
    }
}

impl From<Box<bincode::ErrorKind>> for DiskStructError {
    fn from(e: Box<bincode::ErrorKind>) -> Self {
        DiskStructError::BincodeErr(e)
    }
}

pub(crate) type DiskStructResult<T> = Result<T, DiskStructError>;
