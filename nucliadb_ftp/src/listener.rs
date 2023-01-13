use std::{num::NonZeroUsize, path::PathBuf};

use tokio::net::TcpListener;
use tokio_tar::ArchiveBuilder;

use super::error::Error;

/// A structure for receiving files/directories over TCP/IP connection.
///
/// # Examples
/// ```no_run
/// # tokio_test::block_on(async {
/// use nucliadb_ftp::Listener;
///
/// Listener::default()
///     .save_at("my_path")
///     .preserve_metadata()
///     .listen_once(4242)
///     // Uncomment this line if you want to keep the listener active.
///     // .listen(4242)
///     .await
///     .unwrap();
/// # });
/// ```
#[must_use]
pub struct Listener {
    /// Preserves received file/directory metadata.
    /// Defaulted to `false`.
    preserve_metadata: bool,
    /// Indicates the location of the received files/directories on the file system.
    path: PathBuf,
}

impl Default for Listener {
    fn default() -> Self {
        Self {
            preserve_metadata: false,
            path: PathBuf::from("."),
        }
    }
}

impl Listener {
    /// Set the location of the received files/directories on the file system.
    ///
    /// Note that if the given location does not exist, it will be created on the fly
    /// on file/directory reception.
    pub fn save_at(mut self, path: impl Into<PathBuf>) -> Self {
        self.path = path.into();

        self
    }

    /// Preserves received file/directory metadata.
    ///
    /// Note that [`Publisher`](crate.publisher.Publisher.struct) needs to publish file/directory with metadata preservation.
    pub fn preserve_metadata(mut self) -> Self {
        self.preserve_metadata = true;

        self
    }

    /// Listen only once for receiving files/directories.
    ///
    /// # Errors
    /// This method can fail if:
    /// - The received files/directories are ill-formed.
    /// - The TCP/IP connection cannot be opened (port already in use).
    pub async fn listen_once(&self, port: u16) -> Result<(), Error> {
        self.listen_nth(port, unsafe { Some(NonZeroUsize::new_unchecked(1)) })
            .await
    }

    /// Listen for receiving files/directories.
    ///
    /// Note that this method only returns on failure.
    ///
    /// # Errors
    /// This method can fail if:
    /// - The received files/directories are ill-formed.
    /// - The TCP/IP connection cannot be opened (port already in use).
    pub async fn listen(&self, port: u16) -> Result<(), Error> {
        self.listen_nth(port, None).await
    }

    async fn listen_nth(&self, port: u16, mut limit: Option<NonZeroUsize>) -> Result<(), Error> {
        let listener = TcpListener::bind(format!("0.0.0.0:{port}")).await?;

        loop {
            let (socket, _) = listener.accept().await?;

            let mut archive = ArchiveBuilder::new(socket)
                .set_preserve_mtime(self.preserve_metadata)
                .set_preserve_permissions(self.preserve_metadata)
                .set_unpack_xattrs(self.preserve_metadata)
                .build();

            archive.unpack(&self.path).await?;

            limit = match limit.map(NonZeroUsize::get) {
                Some(1) => break,
                Some(n) => unsafe { Some(NonZeroUsize::new_unchecked(n - 1)) },
                None => continue,
            }
        }

        Ok(())
    }
}
