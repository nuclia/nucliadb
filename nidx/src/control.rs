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

use crate::{NidxMetadata, metrics, settings::EnvSettings, telemetry};

#[derive(Debug, Serialize, Deserialize, clap::Subcommand)]
pub enum ControlRequest {
    Alive,
    Ready,
    SetLogLevel { level: String },
}

#[derive(Debug, Serialize, Deserialize)]
struct Alive {
    database: bool,
}

impl Alive {
    fn all_ok(&self) -> bool {
        self.database
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum ControlResponse {
    // Generic responses
    Success,
    Failure(String),

    // Response for ControlRequest::Alive
    Alive(Alive),

    // Response for ControlRequest::Ready
    Ready {
        alive: Alive,
        searcher_sync_delay: Option<f64>,
        searcher_initally_synced: Option<bool>,
        nats_connected: Option<bool>,
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
    meta: NidxMetadata,
    searcher_synced: Option<bool>,
    nats_client: Option<async_nats::Client>,
}

impl ControlServer {
    pub fn new(meta: NidxMetadata, has_searcher: bool, nats_client: Option<async_nats::Client>) -> Self {
        Self {
            meta,
            searcher_synced: has_searcher.then_some(false),
            nats_client,
        }
    }

    pub async fn run(&mut self, socket_path: &Path) -> anyhow::Result<()> {
        let _ = std::fs::remove_file(socket_path);
        let socket = UnixListener::bind(socket_path)?;
        loop {
            match socket.accept().await {
                Ok((stream, _addr)) => {
                    if let Err(e) = self.run_connection(stream).await {
                        warn!("Error processing control socket message: {e:?}")
                    }
                }
                Err(e) => return Err(e.into()),
            }
        }
    }

    pub async fn run_connection(&mut self, mut stream: UnixStream) -> anyhow::Result<()> {
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await?;
        let response = self.execute_command(serde_json::from_slice(&buf)?).await;
        stream.write_all(&serde_json::to_vec(&response)?).await?;
        stream.shutdown().await?;

        Ok(())
    }

    async fn execute_command(&mut self, req: ControlRequest) -> ControlResponse {
        match req {
            ControlRequest::Alive => ControlResponse::Alive(self.alive().await),
            ControlRequest::Ready => self.ready().await,
            ControlRequest::SetLogLevel { level } => telemetry::set_log_level(&level).into(),
        }
    }

    async fn alive(&self) -> Alive {
        Alive {
            database: self.meta.pool.acquire().await.is_ok(),
        }
    }

    async fn ready(&mut self) -> ControlResponse {
        let searcher_sync_delay = if let Some(synced) = self.searcher_synced {
            let delay = metrics::searcher::SYNC_DELAY.get();
            if !synced && delay < 60.0 {
                self.searcher_synced = Some(true)
            }
            Some(delay)
        } else {
            None
        };

        ControlResponse::Ready {
            alive: self.alive().await,
            searcher_sync_delay,
            searcher_initally_synced: self.searcher_synced,
            nats_connected: self
                .nats_client
                .as_ref()
                .map(|c| c.connection_state() == async_nats::connection::State::Connected),
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

    // Special handling for liveliness/readyness check
    if let ControlResponse::Alive(alive) = response {
        if !alive.all_ok() {
            return Err(anyhow!("Not alive"));
        }
    } else if let ControlResponse::Ready {
        alive,
        searcher_initally_synced,
        nats_connected,
        ..
    } = response
    {
        if !(alive.all_ok() && searcher_initally_synced.unwrap_or(true) && nats_connected.unwrap_or(true)) {
            return Err(anyhow!("Not ready"));
        }
    }

    Ok(())
}
