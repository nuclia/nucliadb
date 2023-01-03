// #![warn(clippy::pedantic)]

mod error;

use std::path::Path;

use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio_tar::{ArchiveBuilder, Builder, HeaderMode};

pub use error::Error;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct SendOptions {
    pub follow_symlinks: bool,
    pub append_recursively: bool,
    pub preserve_metadata: bool,
}

impl Default for SendOptions {
    fn default() -> Self {
        Self {
            follow_symlinks: false,
            append_recursively: true,
            preserve_metadata: true,
        }
    }
}

pub async fn send(address: impl ToSocketAddrs, source: impl AsRef<Path>) -> Result<(), Error> {
    send_with_options(address, source, SendOptions::default()).await
}

pub async fn send_with_options(
    address: impl ToSocketAddrs,
    source: impl AsRef<Path>,
    options: SendOptions,
) -> Result<(), Error> {
    let socket = TcpStream::connect(address).await?;
    let source = source.as_ref();
    let mut archive = Builder::new(socket);

    archive.mode(if options.preserve_metadata {
        HeaderMode::Complete
    } else {
        HeaderMode::Deterministic
    });

    archive.follow_symlinks(options.follow_symlinks);

    if source.is_dir() {
        let dir = source.file_name().unwrap();

        if options.append_recursively {
            archive.append_dir_all(dir, source).await?;
        } else {
            archive.append_dir(dir, source).await?;
        }
    } else {
        archive.append_path(source).await?;
    }

    let _writer = archive.into_inner().await?;

    Ok(())
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct ReceiveOptions {
    pub preserve_metadata: bool,
}

impl Default for ReceiveOptions {
    fn default() -> Self {
        Self {
            preserve_metadata: true,
        }
    }
}

pub async fn receive(
    address: impl ToSocketAddrs,
    destination: impl AsRef<Path>,
) -> Result<(), Error> {
    receive_with_options(address, destination, ReceiveOptions::default()).await
}

pub async fn receive_with_options(
    address: impl ToSocketAddrs,
    destination: impl AsRef<Path>,
    options: ReceiveOptions,
) -> Result<(), Error> {
    let listener = TcpListener::bind(address).await?;
    let (socket, _) = listener.accept().await?;

    let mut archive = if options.preserve_metadata {
        ArchiveBuilder::new(socket)
            .set_preserve_mtime(true)
            .set_preserve_permissions(true)
            .set_unpack_xattrs(true)
            .build()
    } else {
        ArchiveBuilder::new(socket).build()
    };

    Ok(archive.unpack(destination).await?)
}

#[cfg(test)]
mod tests {
    use std::fs::{self, File};
    use std::io::Write;

    use eyre::Result;

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn it_sends_file_on_localhost() -> Result<()> {
        let address = "0.0.0.0:4242";
        let file_name = "dummy.txt";
        let file_content = "Time spent with cats is never wasted";

        let sender = tokio::spawn({
            let address = address;
            let file_content = file_content;

            async move {
                let destination_dir = tempfile::tempdir()?;

                // Receiver::default()
                //     .destination(destination_dir)
                //     .keep_metadata()
                //     .listen(address).await?;

                receive(address, &destination_dir).await?;

                let destination = destination_dir.path().join(file_name);

                assert_eq!(file_content, &fs::read_to_string(destination)?);

                Ok(()) as Result<()>
            }
        });

        let receiver = tokio::spawn({
            async move {
                let source_dir = tempfile::tempdir()?;
                let source = source_dir.path().join(file_name);

                {
                    let mut file = File::create(&source)?;

                    writeln!(file, "{}", file_content)?;
                }

                // Sender::default()
                //     .source(file_name)
                //     .persist_metadata()
                //     .stream(address)
                //     .await?;

                send(address, source).await?;

                Ok(()) as Result<()>
            }
        });

        let _ = tokio::try_join!(sender, receiver)?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn it_sends_directory_on_localhost() -> Result<()> {
        let address = "0.0.0.0:4242";
        let file_name = "dummy.txt";
        let file_content = "Time spent with cats is never wasted";

        let sender = tokio::spawn({
            let address = address;
            let file_content = file_content;

            async move {
                let destination_dir = tempfile::tempdir()?;

                receive(address, &destination_dir).await?;

                let destination = destination_dir.path().join(file_name);

                assert_eq!(file_content, &fs::read_to_string(destination)?);

                Ok(()) as Result<()>
            }
        });

        let receiver = tokio::spawn({
            async move {
                let source_dir = tempfile::tempdir()?;
                let source = source_dir.path().join(file_name);

                {
                    let mut file = File::create(&source)?;

                    writeln!(file, "{}", file_content)?;
                }

                send(address, source).await?;

                Ok(()) as Result<()>
            }
        });

        let _ = tokio::try_join!(sender, receiver)?;

        Ok(())
    }
}
