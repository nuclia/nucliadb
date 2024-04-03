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
use std::time::Duration;

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

macro_rules! delegate {
    ($(#[$attr:meta])* $name:ident: $type:ty) => {
        $(#[$attr])*
        pub fn $name(&self) -> $type {
            self.inner.$name
        }
    };
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

    delegate!(
        /// Maximum number of shards an index node will store
        max_shards_per_node: usize
    );

    // TODO: rename to `node_id_path` or similar
    /// Path to index node UUID file
    pub fn host_key_path(&self) -> PathBuf {
        self.inner.node_id_path.clone()
    }

    delegate!(
        /// Host public IP
        public_ip: IpAddr
    );

    delegate!(
        /// When enabled, sentry will be activated
        sentry_enabled: bool
    );

    pub fn sentry_url(&self) -> String {
        self.inner.sentry_url.clone()
    }

    delegate!(
        /// Sentry environment to report errors
        sentry_env: &'static str
    );

    /// Log levels. Every element is a crate-level pair
    pub fn log_levels(&self) -> &[(String, Level)] {
        &self.inner.log_levels
    }

    delegate!(
        /// When enabled, stdout logs are formatted as plain compact
        plain_logs: bool
    );

    /// Span levels. Every element is a crate-level pair
    pub fn span_levels(&self) -> &[(String, Level)] {
        &self.inner.span_levels
    }

    delegate!(
        /// When enabled, traces will be exported to Jaeger
        jaeger_enabled: bool
    );

    /// Jaeger Agent address used to export traces
    pub fn jaeger_agent_address(&self) -> String {
        let host = &self.inner.jaeger_agent_host;
        let port = &self.inner.jaeger_agent_port;
        format!("{}:{}", host, port)
    }

    delegate!(
        /// Address where index node reader will listen to
        reader_listen_address: SocketAddr
    );

    delegate!(
        /// Address where index node writer will listen to
        writer_listen_address: SocketAddr
    );

    delegate!(
        /// Port where metrics endpoint listens
        metrics_port: u16
    );

    /// Address where secondary node read will connect to primary node through
    pub fn primary_address(&self) -> String {
        self.inner.primary_address.clone()
    }

    delegate!(
        /// Replication role (Primary/Secondary)
        node_role: NodeRole
    );

    delegate!(
        /// Delay between replications runs
        replication_delay: Duration
    );

    delegate!(
        /// Max concurrency during replication
        replication_max_concurrency: u64
    );

    delegate!(
        /// Delay for updating replication healthy status
        replication_healthy_delay: Duration
    );

    delegate!(
        /// Maximum replicas per node
        max_node_replicas: u64
    );

    delegate!(
        /// Number of threads in threadpool
        num_global_rayon_threads: usize
    );

    delegate!(
        /// Delay before idle merge scheduler runs
        merge_scheduler_free_time_work_scheduling_delay: Duration
    );

    delegate!(
        /// Max nodes in merged segments (idle)
        merge_scheduler_max_nodes_in_merge: usize
    );

    delegate!(
        /// Min segments before idle merger runs
        merge_scheduler_segments_before_merge: usize
    );

    delegate!(
        /// Max nodes in merged segments (on commit)
        merge_on_commit_max_nodes_in_merge: usize
    );

    delegate!(
        /// Min segments before on-commit merger runs
        merge_on_commit_segments_before_merge: usize
    );
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
        let settings = Settings::builder().data_path("mydata").inner_build().unwrap();

        assert_eq!(settings.shards_path.to_str().unwrap(), "mydata/shards");
    }

    #[test]
    fn test_settings_with_custom_setter() {
        let settings = Settings::builder()
            .hostname("localhost")
            .sentry_env("prod")
            .with_jaeger_enabled()
            .reader_listen_address("localhost:2020")
            .writer_listen_address("localhost:2021")
            .inner_build()
            .unwrap();

        assert!(Ok(settings.public_ip) == "127.0.0.1".parse() || Ok(settings.public_ip) == "::1".parse());
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
