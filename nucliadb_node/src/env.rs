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
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use nucliadb_core::tracing::*;

use crate::utils::{parse_log_level, reliable_lookup_host};

const SENTRY_PROD: &str = "prod";
const SENTRY_DEV: &str = "stage";

/// Where data will be stored
pub fn data_path() -> PathBuf {
    match env::var("DATA_PATH") {
        Ok(var) => PathBuf::from(var),
        Err(_) => PathBuf::from("data"),
    }
}

/// Path for shards information inside data folder
pub fn shards_path() -> PathBuf {
    data_path().join("shards")
}

pub fn shards_path_id(id: &str) -> PathBuf {
    shards_path().join(id)
}

/// Reader GRPC service port
pub fn reader_listen_address() -> SocketAddr {
    let port = chitchat_port() + 2;

    let default = SocketAddr::new(IpAddr::from_str("::1").unwrap(), port);

    match env::var("READER_LISTEN_ADDRESS") {
        Ok(var) => var
            .to_socket_addrs()
            .unwrap()
            .next()
            .expect("Error parsing Socket address for swim peers addrs"),
        Err(_) => {
            warn!(
                "READER_LISTEN_ADDRESS not defined. Defaulting to: {}",
                default
            );
            default
        }
    }
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

pub fn writer_listen_address() -> SocketAddr {
    let port = chitchat_port() + 1;
    let default = SocketAddr::new(IpAddr::from_str("::1").unwrap(), port);

    match env::var("WRITER_LISTEN_ADDRESS") {
        Ok(var) => var
            .to_socket_addrs()
            .unwrap()
            .next()
            .expect("Error parsing Socket address for swim peers addrs"),
        Err(_) => {
            warn!(
                "WRITER_LISTEN_ADDRESS not defined. Defaulting to: {}",
                default
            );
            default
        }
    }
}

pub fn lazy_loading() -> bool {
    let default = true;
    match env::var("LAZY_LOADING") {
        Ok(var) => match var.parse() {
            Ok(value) => value,
            Err(_) => {
                error!("Error parsing environment variable LAZY_LOADING");
                default
            }
        },
        Err(_) => {
            warn!("LAZY_LOADING not defined. Defaulting to: {}", default);
            default
        }
    }
}

pub fn host_key_path() -> String {
    let default = String::from("host_key");
    match env::var("HOST_KEY_PATH") {
        Ok(var) => match var.parse() {
            Ok(value) => value,
            Err(_) => {
                error!("Error parsing environment variable HOST_KEY_PATH");
                default
            }
        },
        Err(_) => default,
    }
}

pub fn chitchat_port() -> u16 {
    let default: u16 = 40100;
    match env::var("CHITCHAT_PORT") {
        Ok(var) => u16::from_str(&var).unwrap_or_else(|e| {
            error!("Can't parse CHITCHAT_PORT variable: {e}");
            default
        }),
        Err(_) => {
            warn!("CHITCHAT_PORT not defined. Defaulting to: {}", default);
            default
        }
    }
}

pub async fn public_ip() -> IpAddr {
    let default = IpAddr::from_str("::1").unwrap();
    match env::var("HOSTNAME") {
        Ok(v) => {
            let host = format!("{}:4444", &v);
            reliable_lookup_host(&host).await
        }
        Err(e) => {
            error!("HOSTNAME node defined. Defaulting to: {default}. Error details: {e}");
            default
        }
    }
}

pub fn seed_nodes() -> Vec<String> {
    let default = vec![];
    match env::var("SEED_NODES") {
        Ok(v) => v.split(';').map(|addr| addr.to_string()).collect(),
        Err(e) => {
            error!("Error parsing environment varialbe SEED_NODES: {e}");
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
    let default = "nucliadb_node=WARN,nucliadb_cluster=WARN,nucliadb_cluster=WARN".to_string();
    match env::var("RUST_LOG") {
        Ok(levels) => parse_log_level(&levels),
        Err(_) => {
            error!("RUST_LOG not defined. Defaulting to {default}");
            parse_log_level(&default)
        }
    }
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

/// Returns the Prometheus endpoint, if any.
pub fn get_prometheus_url() -> Option<String> {
    let error = match env::var("PROMETHEUS_URL") {
        Ok(value) if !value.is_empty() => return Some(value),
        Ok(_) => "PROMETHEUS_URL defined incorrectly. No defaulted",
        Err(_) => "PROMETHEUS_URL not defined. No defaulted",
    };

    error!(error);

    None
}

/// Returns the Prometheus username, if any.
pub fn get_prometheus_username() -> Option<String> {
    match env::var("PROMETHEUS_USERNAME") {
        Ok(value) => Some(value),
        Err(_) => {
            warn!("PROMETHEUS_USERNAME not defined. No defaulted");
            None
        }
    }
}

/// Returns the Prometheus password, if any.
pub fn get_prometheus_password() -> Option<String> {
    match env::var("PROMETHEUS_PASSWORD") {
        Ok(value) => Some(value),
        Err(_) => {
            warn!("PROMETHEUS_PASSWORD not defined. No defaulted");
            None
        }
    }
}

/// Retuns the Promethus push timing, defaulted to 1h if not defined.
pub fn get_prometheus_push_timing() -> Duration {
    const DEFAULT_TIMING_PLACEHOLDER: &str = "1h";
    const DEFAULT_TIMING: Duration = Duration::from_secs(60 * 60);

    match env::var("PROMETHEUS_PUSH_TIMING") {
        Ok(value) => {
            if let Ok(duration) = parse_duration::parse(&value) {
                duration
            } else {
                error!(
                    "PROMETHEUS_PUSH_TIMING defined incorrectly. Defaulting to \
                     {DEFAULT_TIMING_PLACEHOLDER}"
                );

                DEFAULT_TIMING
            }
        }
        Err(_) => {
            warn!("PROMETHEUS_PUSH_TIMING not defined. Defaulting to {DEFAULT_TIMING_PLACEHOLDER}");

            DEFAULT_TIMING
        }
    }
}

/// Retuns the liveliness interval update used by cluster node.
pub fn get_cluster_liveliness_interval_update() -> Duration {
    const DEFAULT_INTERVAL_UPDATE_PLACEHOLDER: &str = "500ms";
    const DEFAULT_INTERVAL_UPDATE: Duration = Duration::from_millis(500);

    match env::var("LIVELINESS_UPDATE") {
        Ok(value) => {
            if let Ok(duration) = parse_duration::parse(&value) {
                duration
            } else {
                error!(
                    "LIVELINESS_UPDATE defined incorrectly. Defaulting to \
                     {DEFAULT_INTERVAL_UPDATE_PLACEHOLDER}"
                );

                DEFAULT_INTERVAL_UPDATE
            }
        }
        Err(_) => {
            warn!(
                "LIVELINESS_UPDATE not defined. Defaulting to \
                 {DEFAULT_INTERVAL_UPDATE_PLACEHOLDER}"
            );

            DEFAULT_INTERVAL_UPDATE
        }
    }
}
