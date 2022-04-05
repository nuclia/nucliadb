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
use std::str::FromStr;

use tracing::*;

/// Global configuration options
pub struct Configuration {}

impl Configuration {
    pub fn swim_timeout() -> usize {
        let default: usize = 5;
        match env::var("SWIM_TIMEOUT") {
            Ok(var) => match var.parse() {
                Ok(result) => result,
                Err(e) => {
                    warn!(
                        "SWIM_TIMEOUT parsing error: {} defaulting to: {}",
                        e, default
                    );
                    default
                }
            },
            Err(_) => {
                warn!("SWIM_TIMEOUT not defined. Defaulting to: {}", default);
                default
            }
        }
    }

    pub fn swim_interval() -> usize {
        let default: usize = 5;
        match env::var("SWIM_INTERVAL") {
            Ok(var) => match var.parse() {
                Ok(result) => result,
                Err(e) => {
                    warn!(
                        "SWIM_INTERVAL parsing error: {} defaulting to: {}",
                        e, default
                    );
                    default
                }
            },
            Err(_) => {
                warn!("SWIM_INTERVAL not defined. Defaulting to: {}", default);
                default
            }
        }
    }

    /// Vector dimensions
    pub fn vectors_dimension() -> usize {
        let default: usize = 768;
        match env::var("VECTORS_DIMENSION") {
            Ok(var) => match var.parse() {
                Ok(result) => result,
                Err(e) => {
                    warn!(
                        "VECTORS_DIMENSION parsing error: {} defaulting to: {}",
                        e, default
                    );
                    default
                }
            },
            Err(_) => {
                warn!("VECTORS_DIMENSION not defined. Defaulting to: {}", default);
                default
            }
        }
    }

    /// Where data will be stored
    pub fn data_path() -> String {
        match env::var("DATA_PATH") {
            Ok(var) => var,
            Err(_) => String::from("data"),
        }
    }

    /// Path for shards information inside data folder
    pub fn shards_path() -> String {
        Configuration::data_path() + "/shards"
    }

    pub fn shards_path_id(id: &str) -> String {
        return format!("{}/{}", Configuration::shards_path(), id);
    }

    /// Reader GRPC service port
    pub fn reader_listen_address() -> SocketAddr {
        let swim_addr = Configuration::swim_addr();
        let port = swim_addr.port() + 2;

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

    pub fn writer_listen_address() -> SocketAddr {
        let swim_addr = Configuration::swim_addr();
        let port = swim_addr.port() + 1;

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

    pub fn swim_addr() -> SocketAddr {
        let default = SocketAddr::new(IpAddr::from_str("::1").unwrap(), 9999);
        match env::var("SWIM_ADDR") {
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

    pub fn swim_peers_addrs() -> Vec<SocketAddr> {
        let default = vec![];

        match env::var("SWIM_PEERS_ADDR") {
            Ok(var) => {
                let addrs_str: Vec<&str> =
                    serde_json::from_str(&var).unwrap_or_else(|_| Vec::default());
                trace!("Swim peer address: {:?}", addrs_str);

                addrs_str
                    .iter()
                    .map(|addr| {
                        addr.to_socket_addrs()
                            .unwrap()
                            .next()
                            .expect("Error parsing Socket address for swim peers addrs")
                    })
                    .collect()
            }
            Err(_) => default,
        }
    }
}
