use std::sync::Arc;

use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::{gcp::GoogleCloudStorageBuilder, DynObjectStore};
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

use base64::engine::general_purpose::STANDARD as base64;
use base64::Engine;
use serde::Deserialize;
use serde_with::with_prefix;
#[derive(Deserialize, Debug)]
#[serde(tag = "object_store", rename_all = "lowercase")]
pub enum ObjectStoreConfig {
    Memory,
    File {
        file_path: String,
    },
    Gcs {
        bucket: String,
        base64_creds: String,
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
                if !base64_creds.is_empty() {
                    let service_account_key = base64.decode(base64_creds).unwrap();
                    builder = builder.with_service_account_key(String::from_utf8(service_account_key).unwrap());
                } else if let Some(endpoint) = endpoint {
                    // Anonymous with local endpoint (for testing)
                    builder = builder.with_service_account_key(
                        format!(r#"{{"gcs_base_url": "{endpoint}", "disable_oauth": true, "private_key":"","private_key_id":"","client_email":""}}"#),
                    );
                }
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

#[derive(Deserialize, Debug)]
pub struct MetadataSettings {
    pub database_url: String,
}

#[derive(Deserialize, Debug)]
pub struct IndexerSettings {
    #[serde(flatten)]
    pub object_store: ObjectStoreConfig,
    pub nats_server: String,
}

#[derive(Deserialize, Debug)]
struct StorageSettings {
    #[serde(flatten)]
    object_store: ObjectStoreConfig,
}

with_prefix!(metadata "metadata_");
with_prefix!(indexer "indexer_");
with_prefix!(storage "storage_");

#[derive(Deserialize, Debug)]
pub struct Settings {
    #[serde(flatten, with = "metadata")]
    pub metadata: MetadataSettings,
    #[serde(flatten, with = "indexer")]
    pub indexer: Option<IndexerSettings>,
    #[serde(flatten, with = "storage")]
    pub storage: Option<StorageSettings>,
}

impl Settings {
    pub fn from_env() -> Self {
        envy::from_env().expect("Failed to read configuration")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_settings() {
        let env = [
            ("METADATA_DATABASE_URL", "postgresql://localhost"),
            ("INDEXER_OBJECT_STORE", "file"),
            ("INDEXER_FILE_PATH", "a"),
            ("INDEXER_NATS_SERVER", "a"),
        ];
        let settings: Settings = envy::from_iter(env.iter().map(|(a, b)| (a.to_string(), b.to_string()))).unwrap();
        assert_eq!(settings.metadata.database_url, "postgresql://localhost");
    }
}
