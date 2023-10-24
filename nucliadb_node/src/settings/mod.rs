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

//! Global settings and providers.
//!
//! This module exports a `Settings` struct thought as a global context for the
//! application. Using diferent providers, one can obtain a `Settings` objects
//! using values from different places.
//!
//! As an example, a `EnvSettingsProvider` collects it's values from environment
//! variables.
//!
//! The trait `SettingsProvider` makes it easy to extend this module with more
//! providers (to parse from CLI for example).

pub mod providers;

use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::path::PathBuf;

use derive_builder::Builder;
use nucliadb_core::tracing::{error, Level};
pub use providers::{EnvSettingsProvider, SettingsProvider};

use crate::disk_structure::{METADATA_FILE, SHARDS_DIR};
use crate::replication::NodeRole;
use crate::utils::{parse_log_levels, parse_node_role, reliable_lookup_host};

#[derive(Builder)]
#[builder(pattern = "mutable", setter(strip_option, into))]
pub struct Settings {
    // Data storage and access
    #[builder(default = "\"data\".into()", setter(custom))]
    data_path: PathBuf,
    #[builder(private, default = "PathBuf::from(\"data\").join(METADATA_FILE)")]
    metadata_path: PathBuf,
    #[builder(private, default = "PathBuf::from(\"data\").join(SHARDS_DIR)")]
    shards_path: PathBuf,
    #[builder(default = "true", setter(custom))]
    lazy_loading: bool,
    #[builder(default = "800")]
    max_shards_per_node: usize,

    // Index node self data
    #[builder(default = "\"host_key\".into()", setter(name = "host_key_path"))]
    node_id_path: PathBuf,
    #[builder(default = "reliable_lookup_host(\"localhost\").ip()", setter(custom))]
    public_ip: IpAddr,

    // Errors
    #[builder(default = "true", setter(custom))]
    sentry_enabled: bool,
    #[builder(default = "String::new()")]
    sentry_url: String,
    #[builder(default = "SENTRY_DEV", setter(custom))]
    sentry_env: &'static str,

    // Logs and traces
    #[builder(default = "parse_log_levels(\"nucliadb_node=WARN\")")]
    log_levels: Vec<(String, Level)>,
    #[rustfmt::skip]
    #[builder(
        default = "parse_log_levels(\"nucliadb_node=INFO,nucliadb_core=INFO\")",
        setter(skip)
    )]
    span_levels: Vec<(String, Level)>,

    #[builder(default = "false", setter(custom))]
    plain_logs: bool,

    // Telemetry
    #[builder(default = "false", setter(custom))]
    jaeger_enabled: bool,
    #[builder(default = "\"localhost\".into()")]
    jaeger_agent_host: String,
    #[builder(default = "6831")]
    jaeger_agent_port: u16,

    #[builder(default = "reliable_lookup_host(\"localhost:40102\")", setter(custom))]
    reader_listen_address: SocketAddr,
    #[builder(default = "reliable_lookup_host(\"localhost:40101\")", setter(custom))]
    writer_listen_address: SocketAddr,

    #[builder(default = "3030")]
    metrics_port: u16,

    // replications settings
    #[builder(default = "\"http://localhost:10000\".into()")]
    primary_address: String,
    #[builder(default = "parse_node_role(\"primary\")")]
    node_role: NodeRole,
    #[builder(default = "3")]
    replication_delay_seconds: u64,
}

impl Settings {
    pub fn builder() -> SettingsBuilder {
        SettingsBuilder::default()
    }

    /// Path to main directory where all index node data is stored
    pub fn data_path(&self) -> PathBuf {
        self.data_path.clone()
    }

    /// Path to index node metadata file
    pub fn metadata_path(&self) -> PathBuf {
        self.metadata_path.clone()
    }

    /// Path where all shards are stored
    pub fn shards_path(&self) -> PathBuf {
        self.shards_path.clone()
    }

    /// When shard lazy loading is enabled, reader and writer will try to load a
    /// shard before using it. Otherwise, they'll load all shards at startup
    pub fn lazy_loading(&self) -> bool {
        self.lazy_loading
    }

    /// Maximum number of shards an index node will store
    pub fn max_shards_per_node(&self) -> usize {
        self.max_shards_per_node
    }

    // TODO: rename to `node_id_path` or similar
    /// Path to index node UUID file
    pub fn host_key_path(&self) -> PathBuf {
        self.node_id_path.clone()
    }

    /// Host public IP
    pub fn public_ip(&self) -> IpAddr {
        self.public_ip
    }

    /// When enabled, sentry will be activated
    pub fn sentry_enabled(&self) -> bool {
        self.sentry_enabled
    }

    pub fn sentry_url(&self) -> String {
        self.sentry_url.clone()
    }

    /// Sentry environment to report errors
    pub fn sentry_env(&self) -> &'static str {
        self.sentry_env
    }

    /// Log levels. Every element is a crate-level pair
    pub fn log_levels(&self) -> &[(String, Level)] {
        &self.log_levels
    }

    /// When enabled, stdout logs are formatted as plain compact
    pub fn plain_logs(&self) -> bool {
        self.plain_logs
    }

    /// Span levels. Every element is a crate-level pair
    pub fn span_levels(&self) -> &[(String, Level)] {
        &self.span_levels
    }

    /// When enabled, traces will be exported to Jaeger
    pub fn jaeger_enabled(&self) -> bool {
        self.jaeger_enabled
    }

    /// Jaeger Agent address used to export traces
    pub fn jaeger_agent_address(&self) -> String {
        format!("{}:{}", self.jaeger_agent_host, self.jaeger_agent_port)
    }

    /// Address where index node read will listen to
    pub fn reader_listen_address(&self) -> SocketAddr {
        self.reader_listen_address
    }

    /// Address where index node read will listen to
    pub fn writer_listen_address(&self) -> SocketAddr {
        self.writer_listen_address
    }

    pub fn metrics_port(&self) -> u16 {
        self.metrics_port
    }

    /// Address where secondary node read will connect to primary node through
    pub fn primary_address(&self) -> String {
        self.primary_address.clone()
    }

    pub fn node_role(&self) -> NodeRole {
        self.node_role
    }
    pub fn replication_delay_seconds(&self) -> u64 {
        self.replication_delay_seconds
    }
}

const SENTRY_PROD: &str = "prod";
const SENTRY_DEV: &str = "stage";

impl SettingsBuilder {
    pub fn data_path(&mut self, data_path: impl Into<PathBuf>) -> &mut Self {
        let data_path = data_path.into();
        self.metadata_path = Some(data_path.join(METADATA_FILE));
        self.shards_path = Some(data_path.join(SHARDS_DIR));
        self.data_path = Some(data_path);
        self
    }

    pub fn without_lazy_loading(&mut self) -> &mut Self {
        self.lazy_loading = Some(false);
        self
    }

    pub fn hostname(&mut self, hostname: impl Into<String>) -> &mut Self {
        let hostname = hostname.into();
        self.public_ip = Some(reliable_lookup_host(&hostname).ip());
        self
    }

    pub fn sentry_env(&mut self, sentry_env: impl Into<String>) -> &mut Self {
        let sentry_env = sentry_env.into();
        if sentry_env == "prod" {
            self.sentry_env = Some(SENTRY_PROD);
        } else if sentry_env == "stage" {
            self.sentry_env = Some(SENTRY_DEV);
        } else {
            error!(
                "Invalid sentry environment: {sentry_env}. Keeping default one: {:?}",
                self.sentry_env
            );
        }
        self
    }

    pub fn with_jaeger_enabled(&mut self) -> &mut Self {
        self.jaeger_enabled = Some(true);
        self
    }

    pub fn reader_listen_address(&mut self, addr: impl Into<String>) -> &mut Self {
        let addr = addr.into();
        self.reader_listen_address = Some(
            addr.to_socket_addrs()
                .unwrap_or_else(|_| panic!("Invalid reader listen address: {}", addr))
                .next()
                .expect("Error parsing socket reader listen address"),
        );
        self
    }

    pub fn writer_listen_address(&mut self, addr: impl Into<String>) -> &mut Self {
        let addr = addr.into();
        self.writer_listen_address = Some(
            addr.to_socket_addrs()
                .unwrap_or_else(|_| panic!("Invalid writer listen address: {}", addr))
                .next()
                .expect("Error parsing socket writer listen address"),
        );
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_settings_defaults() {
        let settings = Settings::builder().build().unwrap();

        assert_eq!(settings.shards_path().to_str().unwrap(), "data/shards");
    }

    #[test]
    fn test_settings_custom_data_path() {
        let settings = Settings::builder().data_path("mydata").build().unwrap();

        assert_eq!(settings.shards_path().to_str().unwrap(), "mydata/shards");
    }

    #[test]
    fn test_settings_with_custom_setter() {
        let settings = Settings::builder()
            .without_lazy_loading()
            .hostname("localhost")
            .sentry_env("prod")
            .with_jaeger_enabled()
            .reader_listen_address("localhost:2020")
            .writer_listen_address("localhost:2021")
            .build()
            .unwrap();

        assert!(!settings.lazy_loading());
        assert!(
            Ok(settings.public_ip()) == "127.0.0.1".parse()
                || Ok(settings.public_ip()) == "::1".parse()
        );
        assert!(settings.sentry_enabled);
        assert_eq!(settings.sentry_env, SENTRY_PROD);
        assert!(
            Ok(settings.reader_listen_address()) == "127.0.0.1:2020".parse()
                || Ok(settings.reader_listen_address()) == "[::1]:2020".parse()
        );
        assert!(
            Ok(settings.writer_listen_address()) == "127.0.0.1:2021".parse()
                || Ok(settings.writer_listen_address()) == "[::1]:2021".parse()
        );
    }
}
