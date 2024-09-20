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
//! As an example, a `EnvProvider` collects it's values from environment
//! variables.
//!
//! The trait `Provider` makes it easy to extend this module with more
//! providers (to parse from CLI for example).

use anyhow::anyhow;
use mrflagly::{FlagService, FlagServiceOptions};
use nucliadb_core::tracing::Level;
use object_store::ObjectStore;
use serde::de::Unexpected;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::disk_structure::{METADATA_FILE, SHARDS_DIR};
use crate::replication::NodeRole;
use crate::utils::{parse_log_levels, reliable_lookup_host};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use nucliadb_core::NodeResult;
use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::memory::InMemory;
use serde::{Deserialize, Deserializer};
use tracing::{error, info, warn};

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

fn parse_case_insensitive_bool<'de, D>(d: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    let as_string = String::deserialize(d)?;
    match as_string.to_lowercase().as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        unknown => Err(serde::de::Error::invalid_value(Unexpected::Str(unknown), &"Boolean value")),
    }
}

pub fn load_settings() -> NodeResult<Settings> {
    let settings: EnvSettings = envy::from_env().map_err(|e| anyhow!("Configuration error: {e}"))?;
    Ok(settings.into())
}

// Allowed sentry environments
const SENTRY_ENVS: [&str; 2] = ["stage", "prod"];
const DEFAULT_ENV: &str = "stage";

// Feature flags
pub mod feature_flags {
    pub const TEXTS3: &str = "nucliadb_node_texts3";
}
const DEFAULT_FEATURE_FLAGS: &str = r#"{"nucliadb_node_texts3": {"rollout": 100}}"#;

#[derive(Clone)]
pub struct Settings {
    env: Arc<EnvSettings>,
    pub object_store: Arc<dyn ObjectStore>,
    flags: Arc<FlagService>,
}

impl From<EnvSettings> for Settings {
    fn from(value: EnvSettings) -> Self {
        let object_store = build_object_store_driver(&value);
        let flags = Arc::new(build_flag_service(&value));
        Self {
            env: Arc::new(value),
            object_store,
            flags,
        }
    }
}

impl Settings {
    pub fn has_feature(&self, feature: &str, context: HashMap<String, String>) -> bool {
        let mut extended_context = HashMap::from([("environment".to_string(), self.running_environment.clone())]);
        extended_context.extend(context);
        self.flags.enabled(feature, false, Some(extended_context))
    }
}

fn build_flag_service(settings: &EnvSettings) -> FlagService {
    if let Some(flag_settings_url) = &settings.flag_settings_url {
        FlagService::new(FlagServiceOptions {
            finder_type: mrflagly::FlagFinderType::URL,
            url: Some(flag_settings_url.clone()),
            data: None,
            env_var: None,
            refresh_interval: 300,
        })
    } else {
        FlagService::new(FlagServiceOptions {
            finder_type: mrflagly::FlagFinderType::JSON,
            url: None,
            data: Some(DEFAULT_FEATURE_FLAGS.to_string()),
            env_var: None,
            refresh_interval: 300,
        })
    }
}

pub fn build_object_store_driver(settings: &EnvSettings) -> Arc<dyn ObjectStore> {
    info!("File backend: {:?}", settings.file_backend);
    match settings.file_backend {
        ObjectStoreType::GCS => {
            let mut builder = GoogleCloudStorageBuilder::new().with_bucket_name(settings.gcs_indexing_bucket.clone());
            if !settings.gcs_base64_creds.is_empty() {
                let service_account_key = STANDARD.decode(&settings.gcs_base64_creds).unwrap();
                builder = builder.with_service_account_key(String::from_utf8(service_account_key).unwrap());
            } else if let Some(endpoint) = &settings.gcs_endpoint_url {
                // Anonymous with local endpoint (for testing)
                builder = builder.with_service_account_key(
                    format!(r#"{{"gcs_base_url": "{endpoint}", "disable_oauth": true, "private_key":"","private_key_id":"","client_email":""}}"#),
                );
            }
            Arc::new(builder.build().unwrap())
        }
        ObjectStoreType::S3 => {
            let mut builder = AmazonS3Builder::new()
                .with_region(settings.s3_region_name.clone())
                .with_bucket_name(settings.s3_indexing_bucket.clone());
            // Unless client_id and client_secret are specified, the library will try to use the credentials by looking
            // at the standard AWS_WEB_IDENTITY_TOKEN_FILE environment variable
            if !settings.s3_client_id.is_empty() && !settings.s3_client_secret.is_empty() {
                builder = builder
                    .with_access_key_id(settings.s3_client_id.clone())
                    .with_secret_access_key(settings.s3_client_secret.clone());
            }
            if settings.s3_endpoint.is_some() {
                // This is needed for minio compatibility
                builder = builder.with_endpoint(settings.s3_endpoint.clone().unwrap()).with_allow_http(true);
            }
            Arc::new(builder.build().unwrap())
        }
        ObjectStoreType::AZURE => {
            let builder = MicrosoftAzureBuilder::new()
                .with_allow_http(true)
                .with_url(settings.azure_account_url.clone().unwrap());
            Arc::new(builder.build().unwrap())
        }
        // Any other type is not supported for now
        _ => Arc::new(InMemory::new()),
    }
}

impl Deref for Settings {
    type Target = EnvSettings;

    fn deref(&self) -> &Self::Target {
        &self.env
    }
}

#[derive(Default, Debug, PartialEq)]
pub enum ObjectStoreType {
    #[default]
    NOTSET,
    GCS,
    S3,
    AZURE,
}

impl<'de> Deserialize<'de> for ObjectStoreType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?.to_lowercase();
        match s.as_str() {
            "gcs" => Ok(ObjectStoreType::GCS),
            "s3" => Ok(ObjectStoreType::S3),
            "azure" => Ok(ObjectStoreType::AZURE),
            _ => {
                warn!("Invalid object store type: {}. Using default one", s);
                Ok(ObjectStoreType::NOTSET)
            }
        }
    }
}

#[derive(Deserialize)]
#[serde(default)]
pub struct EnvSettings {
    // Debug
    #[serde(deserialize_with = "parse_case_insensitive_bool")]
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
    #[serde(deserialize_with = "parse_case_insensitive_bool")]
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
    #[serde(deserialize_with = "parse_duration_seconds")]
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
    pub merge_maximum_deleted_entries: usize,

    pub max_open_shards: Option<NonZeroUsize>,

    // Object store settings coming from nucliadb_shared chart
    #[serde(default)]
    pub file_backend: ObjectStoreType,
    pub gcs_indexing_bucket: String,
    pub gcs_base64_creds: String,
    pub gcs_endpoint_url: Option<String>,
    pub s3_client_id: String,
    pub s3_client_secret: String,
    pub s3_region_name: String,
    pub s3_indexing_bucket: String,
    pub s3_endpoint: Option<String>,
    pub azure_account_url: Option<String>,

    // Mr.Flagly
    pub flag_settings_url: Option<String>,
}

impl EnvSettings {
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
            merge_maximum_deleted_entries: 15_000,
            max_open_shards: None,
            file_backend: ObjectStoreType::NOTSET,
            gcs_indexing_bucket: Default::default(),
            gcs_base64_creds: Default::default(),
            gcs_endpoint_url: None,
            s3_client_id: Default::default(),
            s3_client_secret: Default::default(),
            s3_region_name: Default::default(),
            s3_indexing_bucket: Default::default(),
            s3_endpoint: None,
            azure_account_url: Default::default(),
            flag_settings_url: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroUsize, path::PathBuf, time::Duration};

    use tracing::Level;

    use crate::replication::NodeRole;

    use super::{EnvSettings, Settings};

    fn from_pairs(pairs: &[(&str, &str)]) -> anyhow::Result<Settings> {
        Ok(envy::from_iter::<_, EnvSettings>(pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())))?.into())
    }

    #[test]
    fn test_data_paths() {
        let settings = from_pairs(&[("DATA_PATH", "my_little_path")]).unwrap();
        assert_eq!(settings.data_path, PathBuf::from("my_little_path"));
        assert_eq!(settings.shards_path(), PathBuf::from("my_little_path/shards"));
        assert_eq!(settings.metadata_path(), PathBuf::from("my_little_path/metadata.json"));
    }

    #[test]
    fn test_file_backend() {
        let settings = from_pairs(&[("FILE_BACKEND", "gcs")]).unwrap();
        assert_eq!(settings.file_backend, super::ObjectStoreType::GCS);

        let settings = from_pairs(&[("FILE_BACKEND", "s3")]).unwrap();
        assert_eq!(settings.file_backend, super::ObjectStoreType::S3);

        let azure_account_url = "https://myaccount.blob.core.windows.net/mycontainer/myblob";
        let settings = from_pairs(&[("FILE_BACKEND", "azure"), ("azure_account_url", azure_account_url)]).unwrap();
        assert_eq!(settings.file_backend, super::ObjectStoreType::AZURE);

        let settings = from_pairs(&[("FILE_BACKEND", "unknown")]).unwrap();
        assert_eq!(settings.file_backend, super::ObjectStoreType::NOTSET);
    }

    #[test]
    fn test_log_levels() {
        // Default
        let settings = from_pairs(&[]).unwrap();
        assert_eq!(settings.log_levels(), &[(String::from("nucliadb_*"), Level::INFO)]);

        // From RUST_LOG
        let settings = from_pairs(&[("RUST_LOG", "nucliadb_*=INFO,tantivy=WARN")]).unwrap();
        assert_eq!(
            settings.log_levels(),
            &[(String::from("nucliadb_*"), Level::INFO), (String::from("tantivy"), Level::WARN)]
        );

        // From LOG_LEVELS
        let settings = from_pairs(&[("LOG_LEVELS", "nucliadb_*=INFO,tantivy=WARN")]).unwrap();
        assert_eq!(
            settings.log_levels(),
            &[(String::from("nucliadb_*"), Level::INFO), (String::from("tantivy"), Level::WARN)]
        );

        // Priority goes to LOG_LEVELS
        let settings = from_pairs(&[("RUST_LOG", "rust_log=INFO"), ("LOG_LEVELS", "log_levels=INFO")]).unwrap();
        assert_eq!(settings.log_levels(), &[(String::from("log_levels"), Level::INFO)]);

        // Empty variable
        let settings = from_pairs(&[("LOG_LEVELS", "")]).unwrap();
        assert_eq!(settings.log_levels(), &[(String::from("nucliadb_*"), Level::INFO)]);
    }

    #[test]
    fn test_sentry_env() {
        let settings = from_pairs(&[("RUNNING_ENVIRONMENT", "stage")]).unwrap();
        assert_eq!(settings.sentry_env(), "stage");

        let settings = from_pairs(&[("RUNNING_ENVIRONMENT", "prod")]).unwrap();
        assert_eq!(settings.sentry_env(), "prod");

        let settings = from_pairs(&[("RUNNING_ENVIRONMENT", "random")]).unwrap();
        assert_eq!(settings.sentry_env(), "stage");
    }

    #[test]
    fn test_duration_conversion() {
        let settings = from_pairs(&[("MERGE_SCHEDULER_FREE_TIME_WORK_SCHEDULING_DELAY", "17")]).unwrap();
        assert_eq!(settings.merge_scheduler_free_time_work_scheduling_delay, Duration::from_secs(17));

        let settings = from_pairs(&[("REPLICATION_HEALTHY_DELAY", "360")]).unwrap();
        assert_eq!(settings.replication_healthy_delay, Duration::from_secs(360));
    }

    #[test]
    fn test_invalid_type() {
        let settings = from_pairs(&[("DEBUG", "some_string")]);
        let Err(e) = settings else {
            panic!("Expected failure to load settings")
        };
        assert!(e.to_string().contains("invalid value"));
    }

    #[test]
    fn test_enums() {
        let settings = from_pairs(&[("NODE_ROLE", "secondary")]).unwrap();
        assert_eq!(settings.node_role, NodeRole::Secondary);

        let settings = from_pairs(&[("NODE_ROLE", "primary")]).unwrap();
        assert_eq!(settings.node_role, NodeRole::Primary);
    }

    #[test]
    fn test_shards() {
        let settings = from_pairs(&[]).unwrap();
        assert_eq!(settings.max_open_shards, None);

        let settings = from_pairs(&[("MAX_OPEN_SHARDS", "345")]).unwrap();
        assert_eq!(settings.max_open_shards, Some(NonZeroUsize::new(345).unwrap()));

        assert!(from_pairs(&[("MAX_OPEN_SHARDS", "0")]).is_err());
    }
}
