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
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use lazy_static::lazy_static;
use nucliadb_core::tracing::Level;
pub use providers::{EnvSettingsProvider, SettingsProvider};

use serde::{de, Deserialize, Deserializer};
use std::fmt;

use crate::disk_structure::{METADATA_FILE, SHARDS_DIR};
use crate::replication::NodeRole;
use crate::utils::{parse_log_levels, parse_node_role, reliable_lookup_host};

pub const SENTRY_PROD: &str = "prod";
pub const SENTRY_DEV: &str = "stage";

fn parse_log_levels_serde<'de, D>(d: D) -> Result<Vec<(String, Level)>, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(parse_log_levels(&String::deserialize(d)?))
}

fn parse_duration_seconds<'de, D>(d: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(Duration::from_secs(u64::deserialize(d)?))
}

#[derive(Clone)]
pub struct Settings(Arc<InnerSettings>);

#[derive(Deserialize)]
#[serde(default)]
pub struct InnerSettings {
    // Debug
    pub debug: bool,

    // Data storage and access
    pub data_path: PathBuf,
    pub max_shards_per_node: usize,

    // Index node self data
    pub node_id_path: PathBuf,
    #[serde(skip)]
    pub public_ip: IpAddr,

    // Errors
    pub sentry_enabled: bool,
    pub sentry_url: String,
    pub sentry_env: String,

    // Logs and traces
    #[serde(deserialize_with = "parse_log_levels_serde")]
    pub log_levels: Vec<(String, Level)>,
    #[serde(deserialize_with = "parse_log_levels_serde")]
    pub span_levels: Vec<(String, Level)>,

    pub plain_logs: bool,

    // Telemetry
    pub jaeger_enabled: bool,
    pub jaeger_agent_host: String,
    pub jaeger_agent_port: u16,

    pub reader_listen_address: SocketAddr,
    pub writer_listen_address: SocketAddr,

    pub metrics_port: u16,

    // replications settings
    pub primary_address: String,
    pub node_role: NodeRole,
    #[serde(deserialize_with = "parse_duration_seconds")]
    pub replication_delay: Duration,
    pub replication_max_concurrency: u64,
    // time since last replication for node to be considered healthy
    pub replication_healthy_delay: Duration,

    // max number of replicas per node
    pub max_node_replicas: u64,

    // number of threads to use for rayon
    pub num_global_rayon_threads: usize,

    // merging
    /// Time between scheduler being idle and scheduling free time work. This
    /// setting also affects merge scheduler reaction time
    #[serde(deserialize_with = "parse_duration_seconds")]
    pub merge_scheduler_free_time_work_scheduling_delay: Duration,

    // Two pairs of merge settings applied when running a scheduled merge or
    // a merge triggered after a commit.
    // - max_nodes_in_merge: maximum merged segment size (in number of vectors)
    // - segments_before_merge: minimum number of alive segments before considering a merge
    pub merge_scheduler_max_nodes_in_merge: usize,
    pub merge_scheduler_segments_before_merge: usize,
    pub merge_on_commit_max_nodes_in_merge: usize,
    pub merge_on_commit_segments_before_merge: usize,
}

impl Default for InnerSettings {
    fn default() -> Self {
        Self {
            debug: false,
            data_path: "data".into(),
            max_shards_per_node: 800,
            node_id_path: "host_key".into(),
            public_ip: reliable_lookup_host("localhost").ip(),
            sentry_url: Default::default(),
            sentry_env: String::from(SENTRY_DEV),
            log_levels: parse_log_levels("nucliadb_*=INFO"),
            span_levels: parse_log_levels("nucliadb_node=INFO,nucliadb_core=INFO"),
            plain_logs: false,
            jaeger_enabled: false,
            jaeger_agent_host: "localhost".into(),
            jaeger_agent_port: 6831,
            reader_listen_address: reliable_lookup_host("localhost:40102"),
            writer_listen_address: reliable_lookup_host("localhost:40101"),
            metrics_port: 3030,
            primary_address: "http://localhost:10000".into(),
            node_role: NodeRole::Primary,
            replication_delay: Duration::from_secs(3),
            replication_max_concurrency: 3,
            replication_healthy_delay: Duration::from_secs(30),
            max_node_replicas: 800,
            num_global_rayon_threads: 10,
            merge_scheduler_free_time_work_scheduling_delay: Duration::from_secs(10),
            merge_scheduler_max_nodes_in_merge: 50_000,
            merge_scheduler_segments_before_merge: 2,
            merge_on_commit_max_nodes_in_merge: 10_000,
            merge_on_commit_segments_before_merge: 100,
            sentry_enabled: true,
        }
    }
}

impl Deref for Settings {
    type Target = InnerSettings;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<InnerSettings> for Settings {
    fn from(value: InnerSettings) -> Self {
        Self(Arc::new(value))
    }
}

impl Settings {
    pub fn debug(&self) -> bool {
        self.debug
    }

    /// Path to main directory where all index node data is stored
    pub fn data_path(&self) -> PathBuf {
        self.data_path.clone()
    }

    /// Path to index node metadata file
    pub fn metadata_path(&self) -> PathBuf {
        self.data_path.join(METADATA_FILE)
    }

    /// Path where all shards are stored
    pub fn shards_path(&self) -> PathBuf {
        self.data_path.join(SHARDS_DIR)
    }

    // TODO: rename to `node_id_path` or similar
    /// Path to index node UUID file
    pub fn host_key_path(&self) -> PathBuf {
        self.node_id_path.clone()
    }

    pub fn sentry_url(&self) -> String {
        self.sentry_url.clone()
    }

    pub fn sentry_env(&self) -> String {
        self.sentry_env.clone()
    }

    /// Log levels. Every element is a crate-level pair
    pub fn log_levels(&self) -> &[(String, Level)] {
        &self.log_levels
    }

    /// Span levels. Every element is a crate-level pair
    pub fn span_levels(&self) -> &[(String, Level)] {
        &self.span_levels
    }

    /// Jaeger Agent address used to export traces
    pub fn jaeger_agent_address(&self) -> String {
        let host = &self.jaeger_agent_host;
        let port = &self.jaeger_agent_port;
        format!("{}:{}", host, port)
    }

    /// Address where secondary node read will connect to primary node through
    pub fn primary_address(&self) -> String {
        self.primary_address.clone()
    }
}
