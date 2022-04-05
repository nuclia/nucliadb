use std::time::Duration;

use async_trait::async_trait;
use reqwest::redirect::Policy;
use reqwest::Client;
use tokio::sync::mpsc::UnboundedSender;

use crate::payload::TelemetryPayload;

/// Telemetry push API URL
const DEFAULT_TELEMETRY_PUSH_API_URL: &str = "https://telemetry.nuclia.com/";

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
