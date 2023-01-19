// Copyright (C) 2021 Bosutech XXI S.L.
//
// nucliadb is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at info@nuclia.com.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.
//

#![warn(clippy::pedantic)]

//! # `nucliadb_ftp`
//!
//! The `nucliadb_ftp` crate aims to transfer files/directories asynchronously over the network
//! (using a TCP/IP connection), based on a patched version of [`tokio-tar`](https://github.com/alekece/tokio-tar) crate.
//!
//! To do so, `nucliadb_ftp` provides two simple and easy-to-use types:
//! - [`Publisher`] that helps appending files/directories before publishing them.
//! - [`Listener`] that helps listening (once or multiple time) incoming files/directories.
//!
//! ## Examples
//!
//! ```no_run
//! # tokio_test::block_on(async {
//! use nucliadb_ftp::{Listener, Publisher, RetryPolicy};
//!
//! let listener_task = tokio::spawn(async {
//!     Listener::default()
//!         .save_at("my_dir")
//!         // Uncomment this line if you want to preserve metadata of receveived files/directories.
//!         // .preserve_metadata()
//!         .listen_once(4242)
//!         // Uncomment this line if you want to keep the listener active.
//!         // .listen(4242)
//!         .await
//!         .unwrap();
//! });
//!
//! let publisher_task = tokio::spawn(async {
//!     Publisher::default()
//!         // Uncomment this line if you want to publish files/directories with their metadata
//!         //.preserve_metadata()
//!         // Uncomment this line if you want to follow symlinks in appended directories.
//!         // .follow_symlink()
//!         .append("my_dir").unwrap()
//!         .append("path/to/my_file").unwrap()
//!         .retry_on_failure(RetryPolicy::Always)
//!         .send_to_localhost(4242)
//!         // Or
//!         // .sent_to("x.x.x.x:4242")
//!         .await
//!         .unwrap();
//! });
//!
//! publisher_task.await.unwrap();
//! listener_task.await.unwrap();
//!
//! # });
//! ```

mod error;
mod listener;
mod publisher;

pub use error::Error;
pub use listener::Listener;
pub use publisher::{Publisher, RetryPolicy};

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;
    use std::fs::{self, File};
    use std::io::Write;

    use eyre::Result;
    use tokio::time::Duration;

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn it_sends_file_on_localhost() -> Result<()> {
        let port = 4242;
        let file_name = "dummy.txt";
        let file_content = "Time spent with cats is never wasted";

        let listener_task = tokio::spawn({
            let file_content = file_content;

            async move {
                let destination_dir = tempfile::tempdir()?;

                Listener::default()
                    .save_at(destination_dir.path())
                    .listen_once(port)
                    .await?;

                let destination = destination_dir.path().join(file_name);

                assert_eq!(file_content, &fs::read_to_string(destination)?);

                Ok(()) as Result<()>
            }
        });

        let publisher_task = tokio::spawn({
            async move {
                let source_dir = tempfile::tempdir()?;
                let source = source_dir.path().join(file_name);

                {
                    let mut file = File::create(&source)?;

                    write!(file, "{file_content}")?;
                }

                Publisher::default()
                    .retry_on_failure(RetryPolicy::MaxDuration(Duration::from_secs(30)))
                    .append(source)?
                    .send_to_localhost(port)
                    .await?;

                Ok(()) as Result<()>
            }
        });

        publisher_task.await??;
        listener_task.await??;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn it_sends_directory_on_localhost() -> Result<()> {
        let port = 4243;
        let files = &[
            ("dummy.txt", "Time spent with cats is never waster"),
            ("file1.org", "hello world"),
            ("README.md", include_str!("../README.md")),
        ];
        let dir_name = "my_dir";

        let listener_task = tokio::spawn(async move {
            let destination_dir = tempfile::tempdir()?;

            Listener::default()
                .save_at(destination_dir.path())
                .listen_once(port)
                .await?;

            let received_files = fs::read_dir(destination_dir.path().join(dir_name))?
                .into_iter()
                .filter_map(|entry| entry.ok().map(|e| e.path()))
                .collect::<Vec<_>>();

            assert_eq!(files.len(), received_files.len());

            for received_file in received_files {
                let (_, file_content) = files
                    .iter()
                    .find(|(name, _)| received_file.file_name() == Some(OsStr::new(name)))
                    .unwrap_or_else(|| {
                        panic!(
                            "'{}' not found in destination directory",
                            received_file.display()
                        )
                    });

                assert_eq!(file_content, &fs::read_to_string(received_file)?);
            }

            Ok(()) as Result<()>
        });

        let publisher_task = tokio::spawn(async move {
            let source_dir = tempfile::tempdir()?;
            let source_dir = source_dir.path().join(dir_name);

            fs::create_dir(&source_dir)?;

            {
                for (file_name, file_content) in files {
                    let source = source_dir.join(file_name);
                    let mut file = File::create(source)?;

                    write!(file, "{file_content}")?;
                }
            }

            Publisher::default()
                .retry_on_failure(RetryPolicy::MaxDuration(Duration::from_secs(30)))
                .append(source_dir)?
                .send_to_localhost(port)
                .await?;

            Ok(()) as Result<()>
        });

        publisher_task.await??;
        listener_task.await??;

        Ok(())
    }
}
