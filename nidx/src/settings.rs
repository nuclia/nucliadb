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
use std::sync::Arc;

use base64::engine::general_purpose::STANDARD as base64;
use base64::Engine;
use config::{Config, Environment};
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::{gcp::GoogleCloudStorageBuilder, DynObjectStore};
use serde::{Deserialize, Deserializer};

use crate::NidxMetadata;

#[derive(Clone, Deserialize, Debug)]
#[serde(tag = "object_store", rename_all = "lowercase")]
pub enum ObjectStoreConfig {
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
        client_id: String,
        client_secret: String,
        region_name: String,
        endpoint: Option<String>,
    },
}

impl ObjectStoreConfig {
    pub fn client(&self) -> Arc<DynObjectStore> {
        match self {
            Self::Gcs {
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
                Arc::new(builder.build().unwrap())
            }
            Self::S3 {
                bucket,
                client_id,
                client_secret,
                region_name,
                endpoint,
            } => {
                let mut builder =
                    AmazonS3Builder::new().with_region(region_name.clone()).with_bucket_name(bucket.clone());
                // Unless client_id and client_secret are specified, the library will try to use the credentials by looking
                // at the standard AWS_WEB_IDENTITY_TOKEN_FILE environment variable
                if !client_id.is_empty() && !client_secret.is_empty() {
                    builder =
                        builder.with_access_key_id(client_id.clone()).with_secret_access_key(client_secret.clone());
                }
                if endpoint.is_some() {
                    // This is needed for minio compatibility
                    builder = builder.with_endpoint(endpoint.clone().unwrap()).with_allow_http(true);
                }
                Arc::new(builder.build().unwrap())
            }
            Self::File {
                file_path,
            } => Arc::new(LocalFileSystem::new_with_prefix(file_path).unwrap()),
            Self::Memory => Arc::new(InMemory::new()),
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
    pub nats_server: String,
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
    pub min_number_of_segments: u64,
    pub max_segment_size: usize,
}

impl Default for MergeSettings {
    fn default() -> Self {
        Self {
            min_number_of_segments: 4,
            max_segment_size: 100_000,
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

#[derive(Clone, Debug, Deserialize)]
pub struct EnvSettings {
    /// Connection to the metadata database
    /// Mandatory for all components
    pub metadata: MetadataSettings,

    /// Indexing configuration, should match nucliadb
    /// Required by indexer and scheduler
    pub indexer: Option<IndexerSettings>,

    /// Storage configuration for our segments
    /// Required by indexer, worker, searcher
    pub storage: Option<StorageSettings>,

    /// Merge scheduling algorithm configuration
    /// Required by scheduler
    #[serde(default)]
    pub merge: MergeSettings,

    /// Telemetry configuration
    #[serde(default)]
    pub telemetry: TelemetrySettings,
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
        let metadata = NidxMetadata::new(&settings.metadata.database_url).await?;
        Ok(Self {
            metadata,
            settings,
        })
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
            ("MERGE__MIN_NUMBER_OF_SEGMENTS", "1234"),
        ];
        let settings = EnvSettings::from_map(HashMap::from(env.map(|(k, v)| (k.to_string(), v.to_string()))));
        assert_eq!(&settings.metadata.database_url, "postgresql://localhost");
        assert_eq!(&settings.indexer.unwrap().nats_server, "localhost");
        assert_eq!(settings.merge.min_number_of_segments, 1234);
        assert_eq!(settings.merge.max_segment_size, MergeSettings::default().max_segment_size);
    }
}
