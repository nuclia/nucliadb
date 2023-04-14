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
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::time::Duration;

use http::Uri;
use nucliadb_core::tracing::Level;
use opentelemetry::propagation::Extractor;
use tokio::net;
use tokio::time::sleep;
use tonic::transport::Endpoint;

/// Prepares a socket addr for a grpc endpoint to connect to
pub fn socket_to_endpoint(grpc_addr: SocketAddr) -> anyhow::Result<Endpoint> {
    let uri = Uri::builder()
        .scheme("http")
        .authority(grpc_addr.to_string().as_str())
        .path_and_query("/")
        .build()?;
    // Create a channel with connect_lazy to automatically reconnect to the node.
    let channel = Endpoint::from(uri);
    Ok(channel)
}

/// Metadata mapping
pub struct MetadataMap<'a>(pub &'a tonic::metadata::MetadataMap);

impl<'a> Extractor for MetadataMap<'a> {
    /// Gets a value for a key from the MetadataMap.  If the value can't be converted to &str,
    /// returns None
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|metadata| metadata.to_str().ok())
    }

    /// Collect all the keys from the MetadataMap.
    fn keys(&self) -> Vec<&str> {
        self.0
            .keys()
            .map(|key| match key {
                tonic::metadata::KeyRef::Ascii(v) => v.as_str(),
                tonic::metadata::KeyRef::Binary(v) => v.as_str(),
            })
            .collect::<Vec<_>>()
    }
}

pub async fn reliable_lookup_host(host: &str) -> IpAddr {
    let mut tries = 5;
    while tries != 0 {
        if let Ok(mut addr_iter) = net::lookup_host(host).await {
            if let Some(addr) = addr_iter.next() {
                return addr.ip();
            }
        }
        tries -= 1;
        sleep(Duration::from_secs(1)).await;
    }
    IpAddr::from_str(host).unwrap()
}

pub fn parse_log_level(levels: &str) -> Vec<(String, Level)> {
    levels
        .split(',')
        .map(|s| s.splitn(2, '=').collect::<Vec<_>>())
        .map(|v| (v[0].to_string(), Level::from_str(v[1]).unwrap()))
        .collect()
}
