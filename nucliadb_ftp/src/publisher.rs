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

use std::ffi::{OsStr, OsString};
use std::path::PathBuf;
use std::time::Duration;

use backoff::backoff::{Backoff, Stop};
use backoff::{ExponentialBackoff, ExponentialBackoffBuilder};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio_tar::{Builder, HeaderMode};

use super::error::Error;

/// A structure for representing all the retry policy on publication failure.
#[non_exhaustive]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum RetryPolicy {
    /// Indicates to always retry the publication after a failure.
    Always,
    /// Indicates to never retry the publication after a failure.
    Never,
    /// Indicates to retry the publication during at most the given duration after a failure.
    MaxDuration(Duration),
}

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
///     .append("my_dir").unwrap()
///     .append("path/to/my_file").unwrap()
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
    /// Note that calling [`Publisher::send_to`] method with an empty list of paths results to a
    /// no-op.
    paths: Vec<(PathBuf, OsString)>,
    /// The backoff policy on publication failure.
    ///
    /// Note that if the publication fails because of invalid paths, the backoff policy will be
    /// ignored.
    backoff: Option<ExponentialBackoff>,
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
    /// Note that if the path point to a directory all files and the directory itself will be
    /// published.
    ///
    /// # Errors
    /// This methods can fails if:
    /// - The given path cannot be canonicalized (path does not exists).
    /// - The given path does not contain file name (e.g "/" or "").
    pub fn append(mut self, path: impl Into<PathBuf>) -> Result<Self, Error> {
        let path = path.into();

        let name = path
            .canonicalize()
            .map_err(|e| Error::InvalidPath(path.display().to_string(), e.to_string()))?;

        let name = name.file_name().map(OsStr::to_os_string).ok_or_else(|| {
            Error::InvalidPath(path.display().to_string(), "missing file name".to_string())
        })?;

        self.paths.push((path, name));

        Ok(self)
    }

    /// Indicates to retry the files/directories publication on failure.
    ///
    /// Note that if the given `retry_policy` is set to [`RetryPolicy::Always`], the current
    /// publisher will always retry to send files/directories until success. To put it simply,
    /// the publication could end up as a blocking operation.
    pub fn retry_on_failure(mut self, retry_policy: RetryPolicy) -> Self {
        let backoff = match retry_policy {
            RetryPolicy::Always => Some(None),
            RetryPolicy::MaxDuration(duration) => Some(Some(duration)),
            RetryPolicy::Never => None,
        };

        self.backoff = backoff.map(|max_elapsed_time| {
            ExponentialBackoffBuilder::new()
                .with_max_elapsed_time(max_elapsed_time)
                .with_initial_interval(Duration::from_secs(1))
                .with_multiplier(2.0)
                .build()
        });

        self
    }

    /// Publishs all the appended files/directories to localhost.
    ///
    /// # Errors
    /// This method can fails if the connection with the remote address is refused/closed or
    /// timeout.
    pub async fn send_to_localhost(&self, port: u16) -> Result<(), Error> {
        self.send_to(format!("0.0.0.0:{port}")).await
    }

    /// Publishs all the appended files/directories to the given IP address.
    ///
    /// # Errors
    /// This method can fails if the connection with the remote address is refused/closed or
    /// timeout.
    pub async fn send_to(&self, address: impl ToSocketAddrs + Clone) -> Result<(), Error> {
        let backoff = self.backoff.clone().map_or_else(
            || Box::new(Stop {}) as Box<dyn Backoff + Send>,
            |backoff| Box::new(backoff) as Box<dyn Backoff + Send>,
        );

        backoff::future::retry_notify(
            backoff,
            || {
                let address = address.clone();

                async move {
                    let socket = TcpStream::connect(address).await?;
                    let mut archive = Builder::new(socket);

                    archive.mode(if self.preserve_metadata {
                        HeaderMode::Complete
                    } else {
                        HeaderMode::Deterministic
                    });

                    archive.follow_symlinks(self.follow_symlinks);

                    for (path, name) in &self.paths {
                        if path.is_dir() {
                            archive.append_dir_all(name, path).await?;
                        } else {
                            archive.append_path_with_name(path, name).await?;
                        }
                    }

                    // NOTE: this line is mandatory and SHOULD NOT be removed.
                    // Increment that counter if you tried and broke the codebase: 1
                    let _writer = archive.into_inner().await?;

                    Ok(())
                }
            },
            |e, duration| {
                tracing::warn!("Error happened at {duration:?}: {e}");
            },
        )
        .await
        .map_err(Into::into)
    }
}
