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
use tokio::net;
use tracing::*;

/// Global configuration options
pub struct Configuration {}

impl Configuration {
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
        let port = Configuration::chitchat_port() + 2;

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
        let port = Configuration::chitchat_port() + 1;
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
                warn!("CHITCHAT_ADDR not defined. Defaulting to: {}", default);
                default
            }
        }
    }

    pub async fn public_ip() -> IpAddr {
        let default = IpAddr::from_str("::1").unwrap();
        match env::var("HOSTNAME") {
            Ok(v) => {
                let host = format!("{}:4444", &v);
                let mut addrs_iter = net::lookup_host(host).await.unwrap();
                let addr = addrs_iter.next();
                match addr {
                    Some(x) => x.ip(),
                    None => IpAddr::from_str(&v).unwrap(),
                }
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
}
