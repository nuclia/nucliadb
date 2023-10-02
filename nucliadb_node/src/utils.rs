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
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;
use std::{fs, thread};

use http::Uri;
use nucliadb_core::tracing::{info, Level};
use nucliadb_core::{Context, NodeResult};
use tonic::transport::Endpoint;
use uuid::Uuid;

pub static ALL_TARGETS: &str = "*";

/// Prepares a socket addr for a grpc endpoint to connect to
pub fn socket_to_endpoint(grpc_addr: SocketAddr) -> NodeResult<Endpoint> {
    let uri = Uri::builder()
        .scheme("http")
        .authority(grpc_addr.to_string().as_str())
        .path_and_query("/")
        .build()?;
    // Create a channel with connect_lazy to automatically reconnect to the node.
    let channel = Endpoint::from(uri);
    Ok(channel)
}

pub fn reliable_lookup_host(host: &str) -> SocketAddr {
    // We need an address with port to resolve it
    let host = if !host.contains(':') {
        format!("{host}:0")
    } else {
        host.to_string()
    };

    let mut tries = 5;
    while tries != 0 {
        if let Ok(mut addr_iter) = host.to_socket_addrs() {
            if let Some(addr) = addr_iter.next() {
                return addr;
            }
        }
        tries -= 1;
        thread::sleep(Duration::from_secs(1))
    }
    SocketAddr::from_str(&host)
        .unwrap_or_else(|_| panic!("Unable to resolve IP address for {}", host))
}

pub fn parse_log_levels(levels: &str) -> Vec<(String, Level)> {
    levels
        .split(',')
        .map(|s| s.splitn(2, '=').collect::<Vec<_>>())
        .map(|v| (v[0].to_string(), Level::from_str(v[1]).unwrap()))
        .collect()
}

pub fn read_host_key(hk_path: PathBuf) -> NodeResult<Uuid> {
    let host_key_contents = fs::read(hk_path.clone())
        .with_context(|| format!("Failed to read host key from '{}'", hk_path.display()))?;

    let host_key = Uuid::from_slice(host_key_contents.as_slice())
        .with_context(|| format!("Invalid host key from '{}'", hk_path.display()))?;

    Ok(host_key)
}

/// Reads the key that makes a node unique from the given file.
/// If the file does not exist, it generates an ID and writes it to the file
/// so that it can be reused on reboot.
pub fn read_or_create_host_key(hk_path: PathBuf) -> NodeResult<Uuid> {
    let host_key;

    if hk_path.exists() {
        host_key = read_host_key(hk_path)?;
        info!(host_key=?host_key, "Read existing host key.");
    } else {
        host_key = Uuid::new_v4();
        fs::write(hk_path.clone(), host_key.as_bytes())
            .with_context(|| format!("Failed to write host key to '{}'", hk_path.display()))?;
        info!(host_key=?host_key, host_key_path=?hk_path, "Create new host key.");
    }

    Ok(host_key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lookup_localhost() {
        let hosts = vec!["127.0.0.1", "localhost", "localhost:8080"];
        for host in hosts.into_iter() {
            let lookup = reliable_lookup_host(host).ip().to_string();
            assert!(lookup == "127.0.0.1" || lookup == "::1");
        }
    }

    #[test]
    fn test_parse_log_levels() {
        let levels = "nucliadb=INFO,node_*=DEBUG,*=TRACE";
        let res = parse_log_levels(levels);
        assert_eq!(
            vec![
                ("nucliadb".to_string(), Level::INFO),
                ("node_*".to_string(), Level::DEBUG),
                ("*".to_string(), Level::TRACE)
            ],
            res
        );
    }
}
