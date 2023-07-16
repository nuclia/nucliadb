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
use std::env;
use std::path::PathBuf;
use std::str::FromStr;

use nucliadb_core::tracing::*;

use crate::utils::parse_log_levels;

const SENTRY_PROD: &str = "prod";
const SENTRY_DEV: &str = "stage";

/// Where data will be stored
pub fn data_path() -> PathBuf {
    match env::var("DATA_PATH") {
        Ok(var) => PathBuf::from(var),
        Err(_) => PathBuf::from("data"),
    }
}

/// Path for metadata file inside data folder
pub fn metadata_path() -> PathBuf {
    data_path().join("metadata.json")
}

/// Path for shards information inside data folder
pub fn shards_path() -> PathBuf {
    data_path().join("shards")
}

pub fn shards_path_id(id: &str) -> PathBuf {
    shards_path().join(id)
}

pub fn jaeger_agent_endp() -> String {
    let default_host = "localhost".to_string();
    let default_port = "6831".to_string();
    let host = env::var("JAEGER_AGENT_HOST").unwrap_or_else(|_| {
        warn!("JAEGER_AGENT_HOST not defined: Defaulting to {default_host}");
        default_host
    });
    let port = env::var("JAEGER_AGENT_PORT").unwrap_or_else(|_| {
        warn!("JAEGER_AGENT_PORT not defined: Defaulting to {default_port}");
        default_port
    });
    format!("{host}:{port}")
}

pub fn jaeger_enabled() -> bool {
    let default = false;
    match env::var("JAEGER_ENABLED") {
        Ok(v) => bool::from_str(&v).unwrap(),
        Err(_) => {
            warn!("JAEGER_ENABLED not defined: Defaulting to {}", default);
            default
        }
    }
}

pub fn sentry_url() -> String {
    let default = String::from("");
    match env::var("SENTRY_URL") {
        Ok(var) => match var.parse() {
            Ok(value) => value,
            Err(_) => {
                error!("Error parsing environment variable SENTRY_URL");
                default
            }
        },
        Err(_) => default,
    }
}

pub fn log_level() -> Vec<(String, Level)> {
    let default = "nucliadb_node=WARN,nucliadb_cluster=WARN".to_string();
    match env::var("RUST_LOG") {
        Ok(levels) => parse_log_levels(&levels),
        Err(_) => {
            error!("RUST_LOG not defined. Defaulting to {default}");
            parse_log_levels(&default)
        }
    }
}

pub fn span_levels() -> Vec<(String, Level)> {
    let default = "nucliadb_node=INFO,nucliadb_cluster=INFO,nucliadb_core=INFO".to_string();
    parse_log_levels(&default)
}

pub fn get_sentry_env() -> &'static str {
    let default = SENTRY_DEV;
    match env::var("RUNNING_ENVIRONMENT") {
        Ok(sentry_env) if sentry_env.eq("prod") => SENTRY_PROD,
        Ok(sentry_env) if sentry_env.eq("stage") => SENTRY_DEV,
        Ok(_) => {
            error!("RUNNING_ENVIRONMENT defined incorrectly. Defaulting to {default}");
            default
        }
        Err(_) => {
            error!("RUNNING_ENVIRONMENT not defined. Defaulting to {default}");
            default
        }
    }
}

pub fn metrics_http_port(default: u16) -> u16 {
    match env::var("METRICS_HTTP_PORT") {
        Ok(http_port) => {
            if let Ok(port) = http_port.parse() {
                port
            } else {
                error!("METRICS_HTTP_PORT defined incorrectly. Defaulting to {default}");
                default
            }
        }
        Err(_) => default,
    }
}

pub fn max_shards_per_node() -> usize {
    let default = 800;
    match env::var("MAX_NODE_REPLICAS") {
        Ok(max_shards) => {
            if let Ok(max_shards) = max_shards.parse() {
                max_shards
            } else {
                error!("MAX_NODE_REPLICAS defined incorrectly. Using default: {default}");
                default
            }
        }
        Err(_) => default,
    }
}

pub fn host_key_path() -> PathBuf {
    match env::var("HOST_KEY_PATH") {
        Ok(var) => PathBuf::from(var),
        Err(_) => PathBuf::from("host_key"),
    }
}
