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

use std::collections::HashMap;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as base64;
use config::{Config, Environment};
use object_store::ClientOptions;
use object_store::aws::AmazonS3Builder;
use object_store::limit::LimitStore;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::{DynObjectStore, gcp::GoogleCloudStorageBuilder};
use serde::{Deserialize, Deserializer};

use crate::NidxMetadata;

#[derive(Clone, Deserialize, Debug)]
#[serde(tag = "object_store", rename_all = "lowercase")]
pub enum ObjectStoreKind {
    Memory,
    File {
        file_path: String,
    },
    Gcs {
        bucket: String,
        base64_creds: Option<String>,
        endpoint: Option<String>,
    },
    S3 {
        bucket: String,
        client_id: Option<String>,
        client_secret: Option<String>,
        region_name: String,
        endpoint: Option<String>,
    },
}

fn deserialize_u64<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<u64>, D::Error> {
    Ok(Some(
        String::deserialize(deserializer)?.parse().expect("Expected a number"),
    ))
}

#[derive(Clone, Deserialize, Debug)]
pub struct ObjectStoreConfig {
    #[serde(flatten)]
    kind: ObjectStoreKind,

    #[serde(default, deserialize_with = "deserialize_u64")]
    max_requests: Option<u64>,

    #[serde(default, deserialize_with = "deserialize_u64")]
    timeout: Option<u64>,
}

impl ObjectStoreConfig {
    pub fn client(&self) -> Arc<DynObjectStore> {
        let store: Box<DynObjectStore> = match &self.kind {
            ObjectStoreKind::Gcs {
                bucket,
                base64_creds,
                endpoint,
            } => {
                let mut builder = GoogleCloudStorageBuilder::new().with_bucket_name(bucket.clone());
                match (base64_creds, endpoint) {
                    (Some(creds), _) if !creds.is_empty() => {
                        let service_account_key = base64.decode(creds).unwrap();
                        builder = builder.with_service_account_key(String::from_utf8(service_account_key).unwrap());
                    }
                    (_, Some(endpoint)) => {
                        // Anonymous with local endpoint (for testing)
                        builder = builder.with_service_account_key(
                            format!(r#"{{"gcs_base_url": "{endpoint}", "disable_oauth": true, "private_key":"","private_key_id":"","client_email":""}}"#),
                        );
                    }
                    _ => {}
                };
                if let Some(t) = self.timeout {
                    builder = builder.with_client_options(ClientOptions::new().with_timeout(Duration::from_secs(t)));
                }
                Box::new(builder.build().unwrap())
            }
            ObjectStoreKind::S3 {
                bucket,
                client_id,
                client_secret,
                region_name,
                endpoint,
            } => {
                let mut builder = AmazonS3Builder::new()
                    .with_region(region_name.clone())
                    .with_bucket_name(bucket.clone());
                // Unless client_id and client_secret are specified, the library will try to use the credentials by looking
                // at the standard AWS_WEB_IDENTITY_TOKEN_FILE environment variable
                if let (Some(client_id), Some(client_secret)) = (client_id, client_secret) {
                    builder = builder
                        .with_access_key_id(client_id.clone())
                        .with_secret_access_key(client_secret.clone());
                }
                if endpoint.is_some() {
                    // This is needed for minio compatibility
                    builder = builder.with_endpoint(endpoint.clone().unwrap()).with_allow_http(true);
                }
                if let Some(t) = self.timeout {
                    builder = builder.with_client_options(ClientOptions::new().with_timeout(Duration::from_secs(t)));
                }
                Box::new(builder.build().unwrap())
            }
            ObjectStoreKind::File { file_path } => Box::new(LocalFileSystem::new_with_prefix(file_path).unwrap()),
            ObjectStoreKind::Memory => Box::new(InMemory::new()),
        };

        if let Some(max_requests) = self.max_requests {
            Arc::new(LimitStore::new(store, max_requests as usize))
        } else {
            Arc::new(store)
        }
    }
}

fn deserialize_object_store<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Arc<DynObjectStore>, D::Error> {
    let config = ObjectStoreConfig::deserialize(deserializer)?;
    Ok(config.client())
}

#[derive(Clone, Debug, Deserialize)]
pub struct MetadataSettings {
    pub database_url: String,
}

#[derive(Clone, Deserialize, Debug)]
pub struct IndexerSettings {
    #[serde(flatten, deserialize_with = "deserialize_object_store")]
    pub object_store: Arc<DynObjectStore>,
    pub nats_server: Option<String>,
}

#[derive(Clone, Deserialize, Debug)]
pub struct StorageSettings {
    #[serde(flatten, deserialize_with = "deserialize_object_store")]
    pub object_store: Arc<DynObjectStore>,
}

// Take a look to the merge scheduler for more details about these settings
#[derive(Clone, Deserialize, Debug)]
#[serde(default)]
pub struct MergeSettings {
    pub max_deletions: usize,
    pub log_merge: LogMergeSettings,
    pub vector_merge: VectorMergeSettings,
}

impl Default for MergeSettings {
    fn default() -> Self {
        Self {
            max_deletions: 500,
            log_merge: Default::default(),
            vector_merge: Default::default(),
        }
    }
}

#[derive(Clone, Deserialize, Debug)]
pub struct LogMergeSettings {
    /// Minimum number of segments needed to perform a merge for an index
    pub min_number_of_segments: usize,

    /// Max number of records for a segment to be in the top bucket, i.e.,
    /// elegible for merge. Once a segment becomes bigger, it won't be merged
    /// anymore
    pub top_bucket_max_records: usize,

    /// Max number of records for a segment to be considered in the bottom
    /// bucket. Segments with fewer records won't be further splitted in buckets
    pub bottom_bucket_threshold: usize,

    /// Log value between buckets. Increasing this number implies more segment
    /// sizes to be grouped in the same merge job.
    pub bucket_size_log: f64,
}

impl Default for LogMergeSettings {
    fn default() -> Self {
        Self {
            min_number_of_segments: 4,
            top_bucket_max_records: 10_000_000,
            bottom_bucket_threshold: 10_000,
            bucket_size_log: 1.0,
        }
    }
}

#[derive(Clone, Deserialize, Debug)]
pub struct VectorMergeSettings {
    /// Maximum records in resulting merged segment
    pub max_segment_size: usize,

    /// Minimum number of segments to consider merging
    pub min_number_of_segments: usize,

    /// Size (in records) below which segments are considered small
    pub small_segment_threshold: usize,
}

impl Default for VectorMergeSettings {
    fn default() -> Self {
        Self {
            min_number_of_segments: 4,
            max_segment_size: 200_000,
            small_segment_threshold: 20_000,
        }
    }
}

#[derive(Clone, Deserialize, Debug, Default)]
pub enum LogFormat {
    #[default]
    Pretty,
    Structured,
}

#[derive(Clone, Deserialize, Debug, Default)]
pub struct SentryConfig {
    pub dsn: String,
    pub environment: String,
}

#[derive(Clone, Deserialize, Debug, Default)]
#[serde(default)]
pub struct TelemetrySettings {
    pub log_format: LogFormat,
    pub otlp_collector_url: Option<String>,
    pub sentry: Option<SentryConfig>,
}

fn default_parallel_index_syncs() -> usize {
    2
}

fn default_metadata_refresh_interval() -> f32 {
    1.0
}

#[derive(Clone, Deserialize, Debug)]
pub struct SearcherSettings {
    #[serde(default = "default_parallel_index_syncs")]
    pub parallel_index_syncs: usize,
    #[serde(default)]
    pub shard_partitioning: ShardPartitioningSettings,
    #[serde(default = "default_metadata_refresh_interval")]
    pub metadata_refresh_interval: f32,
}

#[derive(Clone, Deserialize, Debug)]
pub struct ShardPartitioningSettings {
    pub method: ShardPartitioningMethod,
    pub replicas: usize,
}

impl Default for ShardPartitioningSettings {
    fn default() -> Self {
        Self {
            method: ShardPartitioningMethod::Single,
            replicas: 1,
        }
    }
}

#[derive(Clone, Deserialize, Debug)]
pub enum ShardPartitioningMethod {
    Single,
    Kubernetes,
}

impl Default for SearcherSettings {
    fn default() -> Self {
        Self {
            parallel_index_syncs: default_parallel_index_syncs(),
            shard_partitioning: Default::default(),
            metadata_refresh_interval: 1.0,
        }
    }
}

#[derive(Deserialize, Clone, Debug)]
pub struct AuditSettings {
    pub nats_server: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct EnvSettings {
    /// Connection to the metadata database
    /// Mandatory for all components
    pub metadata: Option<MetadataSettings>,

    /// Indexing configuration, should match nucliadb
    /// Required by indexer and scheduler
    pub indexer: Option<IndexerSettings>,

    /// Storage configuration for our segments
    /// Required by indexer, worker, searcher
    pub storage: Option<StorageSettings>,

    /// Searcher-specific configuration
    pub searcher: Option<SearcherSettings>,

    /// Merge scheduling algorithm configuration
    /// Required by scheduler
    #[serde(default)]
    pub merge: MergeSettings,

    /// Telemetry configuration
    #[serde(default)]
    pub telemetry: TelemetrySettings,

    #[serde(default)]
    pub audit: Option<AuditSettings>,

    /// Work path, used by searcher/indexer/worker to create all local files
    /// If not specified, will work with temporary directories inside /tmp
    pub work_path: Option<PathBuf>,

    /// Path to a UNIX socket to control the nidx process
    pub control_socket: Option<PathBuf>,
}

impl EnvSettings {
    pub fn from_env() -> Self {
        Self::from_config_environment(Environment::default())
    }

    fn from_config_environment(env: Environment) -> Self {
        let config = Config::builder().add_source(env.separator("__")).build().unwrap();
        config.try_deserialize().unwrap()
    }

    pub fn from_map(vars: HashMap<String, String>) -> Self {
        let env = Environment::default().source(Some(vars));
        Self::from_config_environment(env)
    }
}

/// Settings wrapper that holds opens a connection to the database
/// Mainly to avoid blocking on the database while parsing settings
#[derive(Clone)]
pub struct Settings {
    pub metadata: NidxMetadata,
    pub settings: EnvSettings,
}

impl Deref for Settings {
    type Target = EnvSettings;

    fn deref(&self) -> &Self::Target {
        &self.settings
    }
}

impl Settings {
    pub async fn from_env() -> anyhow::Result<Self> {
        Self::from_env_settings(EnvSettings::from_env()).await
    }

    pub async fn from_env_settings(settings: EnvSettings) -> anyhow::Result<Self> {
        let metadata = NidxMetadata::new(&settings.metadata.as_ref().expect("DB config required").database_url).await?;
        Ok(Self { metadata, settings })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn test_settings() {
        let env = [
            ("METADATA__DATABASE_URL", "postgresql://localhost"),
            ("INDEXER__OBJECT_STORE", "file"),
            ("INDEXER__FILE_PATH", "/tmp"),
            ("INDEXER__NATS_SERVER", "localhost"),
            ("MERGE__MAX_DELETIONS", "1234"),
        ];
        let settings = EnvSettings::from_map(HashMap::from(env.map(|(k, v)| (k.to_string(), v.to_string()))));
        assert_eq!(&settings.metadata.unwrap().database_url, "postgresql://localhost");
        assert_eq!(settings.indexer.unwrap().nats_server, Some("localhost".to_string()));
        assert_eq!(settings.merge.max_deletions, 1234);
        assert_eq!(
            settings.merge.log_merge.min_number_of_segments,
            LogMergeSettings::default().min_number_of_segments
        );
    }
}
