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

use std::{io::Write as _, path::Path};

use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt},
    net::{UnixListener, UnixStream},
};
use tracing::*;

use crate::{metrics, settings::EnvSettings, telemetry, NidxMetadata};

#[derive(Debug, Serialize, Deserialize, clap::Subcommand)]
pub enum ControlRequest {
    Ready,
    SetLogLevel {
        level: String,
    },
}

#[derive(Debug, Serialize, Deserialize)]
enum ControlResponse {
    // Generic responses
    Success,
    Failure(String),

    // Response for ControlRequest::Ready
    Ready {
        database: bool,
        searcher_sync_delay: Option<f64>,
    },
}

impl From<anyhow::Result<()>> for ControlResponse {
    fn from(value: anyhow::Result<()>) -> Self {
        match value {
            Ok(()) => ControlResponse::Success,
            Err(e) => ControlResponse::Failure(e.to_string()),
        }
    }
}

pub struct ControlServer {
    pub meta: NidxMetadata,
}

impl ControlServer {
    pub async fn run(&self, socket_path: &Path) -> anyhow::Result<()> {
        let _ = std::fs::remove_file(socket_path);
        let socket = UnixListener::bind(socket_path)?;
        loop {
            match socket.accept().await {
                Ok((stream, _addr)) => {
                    if let Err(e) = self.run_connection(stream).await {
                        warn!(?e, "Error processing control socket message")
                    }
                }
                Err(e) => return Err(e.into()),
            }
        }
    }

    pub async fn run_connection(&self, mut stream: UnixStream) -> anyhow::Result<()> {
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await?;
        let response = self.execute_command(serde_json::from_slice(&buf)?).await;
        stream.write_all(&serde_json::to_vec(&response)?).await?;
        stream.shutdown().await?;

        Ok(())
    }

    async fn execute_command(&self, req: ControlRequest) -> ControlResponse {
        match req {
            ControlRequest::Ready => {
                let metric = metrics::searcher::SYNC_DELAY.get();
                println!("metric={metric:?}");
                ControlResponse::Ready {
                    database: self.meta.pool.acquire().await.is_ok(),
                    searcher_sync_delay: if metric < 0.0 {
                        None
                    } else {
                        Some(metric)
                    },
                }
            }
            ControlRequest::SetLogLevel {
                level,
            } => telemetry::set_log_level(&level).into(),
        }
    }
}

pub fn control_client(settings: &EnvSettings, request: ControlRequest) -> anyhow::Result<()> {
    let socket_path = settings.control_socket.as_ref().expect("Control socket not configured");

    let mut stream = std::os::unix::net::UnixStream::connect(Path::new(&socket_path))?;
    stream.write_all(&serde_json::to_vec(&request)?)?;
    stream.shutdown(std::net::Shutdown::Write)?;

    let response: ControlResponse = serde_json::from_reader(stream)?;
    println!("{response:#?}");

    // Special handling for readyness check
    if let ControlResponse::Ready {
        database,
        searcher_sync_delay,
    } = response
    {
        if !(database && searcher_sync_delay.map(|d| d < 60.0).unwrap_or_default()) {
            return Err(anyhow!("Not ready"));
        }
    }

    Ok(())
}
