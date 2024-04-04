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

//! Global global_settings() and providers.
//!
//! This module exports a `global_settings()` struct thought as a global context for the
//! application. Using diferent providers, one can obtain a `global_settings()` objects
//! using values from different places.
//!
//! As an example, a `Envglobal_settings()Provider` collects it's values from environment
//! variables.
//!
//! The trait `global_settings()Provider` makes it easy to extend this module with more
//! providers (to parse from CLI for example).

use std::net::SocketAddr;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use anyhow::anyhow;
use nucliadb_core::tracing::Level;

use nucliadb_core::NodeResult;
use serde::{Deserialize, Deserializer};
use tracing::error;

use crate::disk_structure::{METADATA_FILE, SHARDS_DIR};
use crate::replication::NodeRole;
use crate::utils::{parse_log_levels, reliable_lookup_host};

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

static SETTINGS: OnceLock<Settings> = OnceLock::new();

pub fn global_settings() -> Settings {
    SETTINGS.get().expect("Global settings not initialized").clone()
}

pub fn initialize_global_settings() -> NodeResult<()> {
    let settings: EnvSettings = envy::from_env().map_err(|e| anyhow!("Configuration error: {e}"))?;
    let _ = SETTINGS.set(settings.into());
    Ok(())
}

// Allowed sentry environments
const SENTRY_ENVS: [&str; 2] = ["stage", "prod"];
const DEFAULT_ENV: &str = "stage";

#[derive(Clone)]
pub struct Settings {
    env: Arc<EnvSettings>,
}

impl From<EnvSettings> for Settings {
    fn from(value: EnvSettings) -> Self {
        Self {
            env: Arc::new(value),
        }
    }
}

impl Deref for Settings {
    type Target = EnvSettings;

    fn deref(&self) -> &Self::Target {
        &self.env
    }
}

#[derive(Deserialize)]
#[serde(default)]
pub struct EnvSettings {
    // Debug
    pub debug: bool,

    // Data storage and access
    pub data_path: PathBuf,
    pub host_key_path: PathBuf,

    // Errors
    pub sentry_url: String,
    pub running_environment: String,

    // Logs and traces
    #[serde(deserialize_with = "parse_log_levels_serde")]
    pub rust_log: Vec<(String, Level)>,
    #[serde(deserialize_with = "parse_log_levels_serde")]
    pub log_levels: Vec<(String, Level)>,

    // Telemetry
    pub jaeger_enabled: bool,
    pub jaeger_agent_host: String,
    pub jaeger_agent_port: u16,

    pub reader_listen_address: SocketAddr,
    pub writer_listen_address: SocketAddr,

    pub metrics_port: u16,

    // replications global_settings()
    pub primary_address: String,
    pub node_role: NodeRole,
    pub replication_delay_seconds: u64,
    pub replication_max_concurrency: u64,
    // time since last replication for node to be considered healthy
    pub replication_healthy_delay: Duration,

    // number of threads to use for rayon
    pub num_global_rayon_threads: usize,

    // merging
    /// Time between scheduler being idle and scheduling free time work. This
    /// setting also affects merge scheduler reaction time
    #[serde(deserialize_with = "parse_duration_seconds")]
    pub merge_scheduler_free_time_work_scheduling_delay: Duration,

    // Two pairs of merge global_settings() applied when running a scheduled merge or
    // a merge triggered after a commit.
    // - max_nodes_in_merge: maximum merged segment size (in number of vectors)
    // - segments_before_merge: minimum number of alive segments before considering a merge
    pub merge_scheduler_max_nodes_in_merge: usize,
    pub merge_scheduler_segments_before_merge: usize,
    pub merge_on_commit_max_nodes_in_merge: usize,
    pub merge_on_commit_segments_before_merge: usize,
}

impl EnvSettings {
    /// Path to main directory where all index node data is stored
    pub fn data_path(&self) -> PathBuf {
        self.data_path.clone()
    }

    pub fn replication_delay(&self) -> Duration {
        Duration::from_secs(self.replication_delay_seconds)
    }

    /// Path to index node metadata file
    pub fn metadata_path(&self) -> PathBuf {
        self.data_path.join(METADATA_FILE)
    }

    /// Path where all shards are stored
    pub fn shards_path(&self) -> PathBuf {
        self.data_path.join(SHARDS_DIR)
    }

    pub fn sentry_url(&self) -> String {
        self.sentry_url.clone()
    }

    pub fn sentry_env(&self) -> String {
        if SENTRY_ENVS.contains(&self.running_environment.as_str()) {
            self.running_environment.clone()
        } else {
            error!("Invalid sentry environment: {}. Using default one: {DEFAULT_ENV:?}", self.running_environment);
            String::from(DEFAULT_ENV)
        }
    }

    /// Log levels. Every element is a crate-level pair
    pub fn log_levels(&self) -> &[(String, Level)] {
        if self.log_levels.is_empty() {
            &self.rust_log
        } else {
            &self.log_levels
        }
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

impl Default for EnvSettings {
    fn default() -> Self {
        Self {
            debug: false,
            data_path: "data".into(),
            host_key_path: "host_key".into(),
            sentry_url: Default::default(),
            running_environment: DEFAULT_ENV.into(),
            rust_log: parse_log_levels("nucliadb_*=INFO"),
            log_levels: Vec::new(),
            jaeger_enabled: false,
            jaeger_agent_host: "localhost".into(),
            jaeger_agent_port: 6831,
            reader_listen_address: reliable_lookup_host("localhost:40102"),
            writer_listen_address: reliable_lookup_host("localhost:40101"),
            metrics_port: 3030,
            primary_address: "http://localhost:10000".into(),
            node_role: NodeRole::Primary,
            replication_delay_seconds: 3,
            replication_max_concurrency: 3,
            replication_healthy_delay: Duration::from_secs(30),
            num_global_rayon_threads: 10,
            merge_scheduler_free_time_work_scheduling_delay: Duration::from_secs(10),
            merge_scheduler_max_nodes_in_merge: 50_000,
            merge_scheduler_segments_before_merge: 2,
            merge_on_commit_max_nodes_in_merge: 10_000,
            merge_on_commit_segments_before_merge: 100,
        }
    }
}
