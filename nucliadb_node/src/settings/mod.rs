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

mod inner_settings;
pub mod providers;

use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;

use inner_settings::InnerSettings;
pub use inner_settings::InnerSettingsBuilder;
use nucliadb_core::tracing::Level;
pub use providers::{EnvSettingsProvider, SettingsProvider};

use crate::replication::NodeRole;

pub struct Settings {
    inner: Arc<InnerSettings>,
}

impl From<InnerSettings> for Settings {
    fn from(value: InnerSettings) -> Self {
        Settings {
            inner: Arc::new(value),
        }
    }
}

impl Clone for Settings {
    fn clone(&self) -> Self {
        Settings {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl Settings {
    pub fn builder() -> InnerSettingsBuilder {
        InnerSettingsBuilder::default()
    }

    /// Path to main directory where all index node data is stored
    pub fn debug(&self) -> bool {
        self.inner.debug
    }

    /// Path to main directory where all index node data is stored
    pub fn data_path(&self) -> PathBuf {
        self.inner.data_path.clone()
    }

    /// Path to index node metadata file
    pub fn metadata_path(&self) -> PathBuf {
        self.inner.metadata_path.clone()
    }

    /// Path where all shards are stored
    pub fn shards_path(&self) -> PathBuf {
        self.inner.shards_path.clone()
    }

    /// When shard lazy loading is enabled, reader and writer will try to load a
    /// shard before using it. Otherwise, they'll load all shards at startup
    pub fn lazy_loading(&self) -> bool {
        self.inner.lazy_loading
    }

    /// Maximum number of shards an index node will store
    pub fn max_shards_per_node(&self) -> usize {
        self.inner.max_shards_per_node
    }

    // TODO: rename to `node_id_path` or similar
    /// Path to index node UUID file
    pub fn host_key_path(&self) -> PathBuf {
        self.inner.node_id_path.clone()
    }

    /// Host public IP
    pub fn public_ip(&self) -> IpAddr {
        self.inner.public_ip
    }

    /// When enabled, sentry will be activated
    pub fn sentry_enabled(&self) -> bool {
        self.inner.sentry_enabled
    }

    pub fn sentry_url(&self) -> String {
        self.inner.sentry_url.clone()
    }

    /// Sentry environment to report errors
    pub fn sentry_env(&self) -> &'static str {
        self.inner.sentry_env
    }

    /// Log levels. Every element is a crate-level pair
    pub fn log_levels(&self) -> &[(String, Level)] {
        &self.inner.log_levels
    }

    /// When enabled, stdout logs are formatted as plain compact
    pub fn plain_logs(&self) -> bool {
        self.inner.plain_logs
    }

    /// Span levels. Every element is a crate-level pair
    pub fn span_levels(&self) -> &[(String, Level)] {
        &self.inner.span_levels
    }

    /// When enabled, traces will be exported to Jaeger
    pub fn jaeger_enabled(&self) -> bool {
        self.inner.jaeger_enabled
    }

    /// Jaeger Agent address used to export traces
    pub fn jaeger_agent_address(&self) -> String {
        let host = &self.inner.jaeger_agent_host;
        let port = &self.inner.jaeger_agent_port;
        format!("{}:{}", host, port)
    }

    /// Address where index node read will listen to
    pub fn reader_listen_address(&self) -> SocketAddr {
        self.inner.reader_listen_address
    }

    /// Address where index node read will listen to
    pub fn writer_listen_address(&self) -> SocketAddr {
        self.inner.writer_listen_address
    }

    pub fn metrics_port(&self) -> u16 {
        self.inner.metrics_port
    }

    /// Address where secondary node read will connect to primary node through
    pub fn primary_address(&self) -> String {
        self.inner.primary_address.clone()
    }

    pub fn node_role(&self) -> NodeRole {
        self.inner.node_role
    }
    pub fn replication_delay_seconds(&self) -> u64 {
        self.inner.replication_delay_seconds
    }

    pub fn replication_max_concurrency(&self) -> u64 {
        self.inner.replication_max_concurrency
    }

    pub fn replication_healthy_delay(&self) -> u64 {
        self.inner.replication_healthy_delay
    }

    pub fn max_node_replicas(&self) -> u64 {
        self.inner.max_node_replicas
    }

    pub fn num_global_rayon_threads(&self) -> usize {
        self.inner.num_global_rayon_threads
    }
}

#[cfg(test)]
mod tests {
    use super::inner_settings::SENTRY_PROD;
    use super::*;

    #[test]
    fn test_settings_defaults() {
        let settings = Settings::builder().inner_build().unwrap();

        assert_eq!(settings.shards_path.to_str().unwrap(), "data/shards");
    }

    #[test]
    fn test_settings_custom_data_path() {
        let settings = Settings::builder()
            .data_path("mydata")
            .inner_build()
            .unwrap();

        assert_eq!(settings.shards_path.to_str().unwrap(), "mydata/shards");
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
            .inner_build()
            .unwrap();

        assert!(!settings.lazy_loading);
        assert!(
            Ok(settings.public_ip) == "127.0.0.1".parse()
                || Ok(settings.public_ip) == "::1".parse()
        );
        assert!(settings.sentry_enabled);
        assert_eq!(settings.sentry_env, SENTRY_PROD);
        assert!(
            Ok(settings.reader_listen_address) == "127.0.0.1:2020".parse()
                || Ok(settings.reader_listen_address) == "[::1]:2020".parse()
        );
        assert!(
            Ok(settings.writer_listen_address) == "127.0.0.1:2021".parse()
                || Ok(settings.writer_listen_address) == "[::1]:2021".parse()
        );
    }
}
