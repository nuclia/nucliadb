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

use std::env;
use std::time::UNIX_EPOCH;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Represents the payload of the request sent with telemetry requests.
#[derive(Debug, Serialize, Deserialize)]
pub struct TelemetryPayload {
    /// Client information. See details in `[ClientInformation]`.
    pub client_information: ClientInformation,
    pub events: Vec<EventWithTimestamp>,
    /// Represents the number of events that where drops due to the
    /// combination of the `TELEMETRY_PUSH_COOLDOWN` and `MAX_EVENT_IN_QUEUE`.
    pub num_dropped_events: usize,
}

impl TelemetryPayload {
    pub fn from_single_event(event: TelemetryEvent) -> TelemetryPayload {
        TelemetryPayload {
            client_information: ClientInformation::default(),
            events: vec![EventWithTimestamp::from(event)],
            num_dropped_events: 0,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EventWithTimestamp {
    /// Unix time in seconds.
    pub timestamp: u64,
    /// Telemetry event.
    pub r#type: TelemetryEvent,
}

/// Returns the number of seconds elapsed since UNIX_EPOCH.
///
/// If the system clock is set before 1970, returns 0.
fn unixtime() -> u64 {
    match UNIX_EPOCH.elapsed() {
        Ok(duration) => duration.as_secs(),
        Err(_) => 0u64,
    }
}

impl From<TelemetryEvent> for EventWithTimestamp {
    fn from(event: TelemetryEvent) -> Self {
        EventWithTimestamp {
            timestamp: unixtime(),
            r#type: event,
        }
    }
}

/// Represents a Telemetry Event send to nucliadb's server for usage information.
#[derive(Debug, Serialize, Deserialize)]
pub enum TelemetryEvent {
    /// Create command is called.
    Create,
    /// Delete command
    Delete,
    /// Garbage Collect command
    GarbageCollect,
    /// Serve command is called.
    Serve(ServeEvent),
    /// EndCommand (with the return code)
    EndCommand { return_code: i32 },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServeEvent {
    pub has_seed: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClientInformation {
    session_uuid: uuid::Uuid,
    nucliadb_version: String,
    os: String,
    arch: String,
    hashed_host_username: String,
    component: Option<String>,
    kubernetes: bool,
}

fn hashed_host_username() -> String {
    let hostname = hostname::get()
        .map(|hostname| hostname.to_string_lossy().to_string())
        .unwrap_or_else(|_| "".to_string());
    let username = username::get_user_name().unwrap_or_else(|_| "".to_string());
    let hashed_value = format!("{hostname}:{username}");
    let digest = md5::compute(hashed_value.as_bytes());
    format!("{digest:x}")
}

impl Default for ClientInformation {
    fn default() -> ClientInformation {
        ClientInformation {
            session_uuid: Uuid::new_v4(),
            nucliadb_version: env!("CARGO_PKG_VERSION").to_string(),
            os: env::consts::OS.to_string(),
            arch: env::consts::ARCH.to_string(),
            hashed_host_username: hashed_host_username(),
            component: Some("Node".to_string()),
            kubernetes: std::env::var_os("KUBERNETES_SERVICE_HOST").is_some(),
        }
    }
}
