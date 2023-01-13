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

use std::path::PathBuf;

use tokio::net::{TcpStream, ToSocketAddrs};
use tokio_tar::{Builder, HeaderMode};

use super::error::Error;

/// A structure for publishing files/directories over TCP/IP connection.
///
/// # Examples
/// ```no_run
/// # tokio_test::block_on(async {
/// use nucliadb_ftp::Publisher;
///
/// Publisher::default()
///     .follow_symlinks()
///     .preserve_metadata()
///     .append("my_dir")
///     .append("path/to/my_file")
///     .send_to("0.0.0.0:4040")
///     .await
///     .unwrap();
/// # })
/// ```
#[must_use]
#[derive(Default)]
pub struct Publisher {
    /// Indicates to follow symlinks if any directory has been appended to the current `Publisher`.
    /// Defaulted to `false`.
    follow_symlinks: bool,
    /// Preserves file/directory metadata.
    /// Defaulted to `false`.
    preserve_metadata: bool,
    /// The list of files/directories published on [`Publisher::send_to`] call.
    /// Note that calling [`Publisher::send_to`] method with an empty list of paths results to a no-op.
    paths: Vec<PathBuf>,
}

impl Publisher {
    /// Follows symlinks on publishing.
    pub fn follow_symlinks(mut self) -> Self {
        self.follow_symlinks = true;

        self
    }

    /// Preserves file/directory metadata on publishing.
    pub fn preserve_metadata(mut self) -> Self {
        self.preserve_metadata = true;

        self
    }

    /// Appends the given path to the current publisher.
    ///
    /// Note that if the path point to a directory which result to publish the whole directory.
    pub fn append(mut self, path: impl Into<PathBuf>) -> Self {
        self.paths.push(path.into());

        self
    }

    /// Publishs all the appended files/directories to localhost.
    ///
    /// # Errors
    /// This method can fails if:
    /// - Any of the appended paths is invalid (does not exist, permission denied, and so on).
    /// - Can't connect to the given IP address (port already in use).
    pub async fn send_to_localhost(&self, port: u16) -> Result<(), Error> {
        self.send_to(format!("0.0.0.0:{port}")).await
    }

    /// Publishs all the appended files/directories to the given IP address.
    ///
    /// # Errors
    /// This method can fails if:
    /// - Any of the appended paths is invalid (does not exist, permission denied, and so on).
    /// - Can't connect to the given IP address.
    pub async fn send_to(&self, address: impl ToSocketAddrs) -> Result<(), Error> {
        let socket = TcpStream::connect(address).await?;
        let mut archive = Builder::new(socket);

        archive.mode(if self.preserve_metadata {
            HeaderMode::Complete
        } else {
            HeaderMode::Deterministic
        });

        archive.follow_symlinks(self.follow_symlinks);

        for path in &self.paths {
            let name = path.canonicalize()?;
            let name = name.file_name().unwrap_or_default();

            if path.is_dir() {
                archive.append_dir_all(name, path).await?;
            } else {
                archive.append_path_with_name(path, name).await?;
            }
        }

        let _writer = archive.into_inner().await?;

        Ok(())
    }
}
