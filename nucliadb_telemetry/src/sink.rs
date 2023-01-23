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

use std::time::Duration;

use async_trait::async_trait;
use reqwest::blocking::Client as BlockingClient;
use reqwest::redirect::Policy;
use reqwest::Client;
use tokio::sync::mpsc::UnboundedSender;

use crate::payload::TelemetryPayload;

/// Telemetry push API URL
const DEFAULT_TELEMETRY_PUSH_API_URL: &str = "https://telemetry.nuclia.cloud/";

fn telemetry_push_api_url() -> String {
    if let Some(push_api_url) = std::env::var_os("TELEMETRY_PUSH_API") {
        push_api_url.to_string_lossy().to_string()
    } else {
        DEFAULT_TELEMETRY_PUSH_API_URL.to_string()
    }
}

#[async_trait]
pub trait Sink: Send + Sync + 'static {
    async fn send_payload(&self, payload: TelemetryPayload);
}
pub struct HttpClient {
    client: Client,
    endpoint: String,
}

impl HttpClient {
    pub fn try_new() -> Option<Self> {
        let client = Client::builder()
            .redirect(Policy::limited(3))
            .timeout(Duration::from_secs(10))
            .build()
            .ok()?;
        Some(HttpClient {
            client,
            endpoint: telemetry_push_api_url(),
        })
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

#[async_trait]
impl Sink for UnboundedSender<TelemetryPayload> {
    async fn send_payload(&self, payload: TelemetryPayload) {
        let _ = self.send(payload);
    }
}

#[async_trait]
impl Sink for HttpClient {
    async fn send_payload(&self, payload: TelemetryPayload) {
        // Note that we swallow the error if any
        let _ = self.client.post(&self.endpoint).json(&payload).send().await;
    }
}

pub struct BlockingHttpClient {
    client: BlockingClient,
    endpoint: String,
}
impl BlockingHttpClient {
    pub fn try_new() -> Option<Self> {
        let client = BlockingClient::builder()
            .redirect(Policy::limited(3))
            .timeout(Duration::from_secs(10))
            .build()
            .ok()?;
        Some(BlockingHttpClient {
            client,
            endpoint: telemetry_push_api_url(),
        })
    }

    pub fn blocking_send(&self, payload: TelemetryPayload) {
        // Note that we swallow the error if any
        if let Err(err) = self.client.post(&self.endpoint).json(&payload).send() {
            tracing::error!("Error sending telemetry event: {err:?}");
        }
    }
}
