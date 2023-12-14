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

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use nucliadb_core::protos::ShardCleaned;
use nucliadb_core::tracing::{debug, error, info};
use nucliadb_core::{node_error, Context, NodeResult};
use tokio::sync::RwLock;

use crate::disk_structure;
use crate::settings::Settings;
use crate::shards::errors::ShardNotFoundError;
use crate::shards::metadata::{ShardMetadata, ShardsMetadataManager};
use crate::shards::providers::AsyncShardWriterProvider;
use crate::shards::writer::ShardWriter;
use crate::shards::ShardId;

#[derive(Default)]
pub struct AsyncUnboundedShardWriterCache {
    cache: RwLock<HashMap<ShardId, Arc<ShardWriter>>>,
    pub shards_path: PathBuf,
    metadata_manager: Arc<ShardsMetadataManager>,
}

impl AsyncUnboundedShardWriterCache {
    pub fn new(settings: Settings) -> Self {
        Self {
            // NOTE: as it's not probable all shards will be written, we don't
            // assign any initial capacity to the HashMap under the
            // consideration a resize blocking is not performance critical while
            // writting.
            cache: RwLock::new(HashMap::new()),
            shards_path: settings.shards_path(),
            metadata_manager: Arc::new(ShardsMetadataManager::new(settings.shards_path())),
        }
    }
}

#[async_trait]
impl AsyncShardWriterProvider for AsyncUnboundedShardWriterCache {
    async fn create(&self, metadata: ShardMetadata) -> NodeResult<Arc<ShardWriter>> {
        let shard_id = metadata.id();
        let metadata = Arc::new(metadata);
        let mmetadata = Arc::clone(&metadata);
        let new_shard = Arc::new(
            tokio::task::spawn_blocking(move || ShardWriter::new(mmetadata))
                .await
                .context("Blocking task panicked")??,
        );
        self.metadata_manager.add_metadata(metadata);
        self.cache.write().await.insert(shard_id, new_shard.clone());
        Ok(new_shard)
    }

    async fn load(&self, id: ShardId) -> NodeResult<Arc<ShardWriter>> {
        let shard_key = id.clone();
        let shard_path = disk_structure::shard_path_by_id(&self.shards_path.clone(), &id);
        let mut cache = self.cache.write().await;

        if let Some(shard) = cache.get(&id) {
            debug!("Shard {shard_path:?} is already on memory");
            return Ok(Arc::clone(shard));
        }

        let metadata_manager = Arc::clone(&self.metadata_manager);
        // Avoid blocking while interacting with the file system
        let shard = tokio::task::spawn_blocking(move || {
            if !ShardMetadata::exists(shard_path.clone()) {
                return Err(node_error!(ShardNotFoundError(
                    "Shard {shard_path:?} is not on disk"
                )));
            }
            let metadata = metadata_manager
                .get(id.clone())
                .expect("Shard metadata not found. This should not happen");
            ShardWriter::open(Arc::clone(&metadata)).map_err(|error| {
                node_error!("Shard {shard_path:?} could not be loaded from disk: {error:?}")
            })
        })
        .await
        .context("Blocking task panicked")??;

        let shard = Arc::new(shard);
        let cache_shard = Arc::clone(&shard);
        cache.insert(shard_key, cache_shard);
        Ok(shard)
    }

    async fn load_all(&self) -> NodeResult<()> {
        let shards_path = self.shards_path.clone();
        let metadata_manager = Arc::clone(&self.metadata_manager);
        let shards = tokio::task::spawn_blocking(move || -> NodeResult<_> {
            let mut shards = HashMap::new();
            for entry in std::fs::read_dir(&shards_path)? {
                let entry = entry?;
                let shard_id = entry.file_name().to_str().unwrap().to_string();
                let shard_path = entry.path();
                if !ShardMetadata::exists(shard_path.clone()) {
                    info!(
                        "Shard {shard_path:?} is not on disk",
                        shard_path = shard_path
                    );
                    continue;
                }
                let metadata = metadata_manager
                    .get(shard_id.clone())
                    .expect("Shard metadata not found. This should not happen");
                match ShardWriter::open(metadata) {
                    Err(err) => error!("Loading shard {shard_path:?} from disk raised {err}"),
                    Ok(shard) => {
                        debug!("Shard loaded: {shard_path:?}");
                        shards.insert(shard_id, Arc::new(shard));
                    }
                }
            }
            Ok(shards)
        })
        .await
        .context("Blocking task panicked")??;

        *self.cache.write().await = shards;
        Ok(())
    }

    async fn get(&self, id: ShardId) -> Option<Arc<ShardWriter>> {
        self.cache.read().await.get(&id).map(Arc::clone)
    }

    async fn delete(&self, id: ShardId) -> NodeResult<()> {
        self.cache.write().await.remove(&id);
        let shard_path = disk_structure::shard_path_by_id(&self.shards_path.clone(), &id);
        tokio::task::spawn_blocking(move || {
            if shard_path.exists() {
                debug!("Deleting shard {shard_path:?}");
                std::fs::remove_dir_all(shard_path)?;
            }
            Ok(())
        })
        .await
        .context("Blocking task panicked")?
    }

    async fn upgrade(&self, id: ShardId) -> NodeResult<ShardCleaned> {
        self.cache.write().await.remove(&id);

        let id_ = id.clone();
        let metadata = self.metadata_manager.get(id.clone());
        let (upgraded, details) = tokio::task::spawn_blocking(move || -> NodeResult<_> {
            let upgraded = ShardWriter::clean_and_create(metadata.unwrap())?;
            let details = ShardCleaned {
                document_service: upgraded.document_version() as i32,
                paragraph_service: upgraded.paragraph_version() as i32,
                vector_service: upgraded.vector_version() as i32,
                relation_service: upgraded.relation_version() as i32,
            };
            Ok((upgraded, details))
        })
        .await
        .context("Blocking task panicked")??;

        self.cache.write().await.insert(id_, Arc::new(upgraded));
        Ok(details)
    }

    fn get_metadata(&self, id: ShardId) -> Option<Arc<ShardMetadata>> {
        self.metadata_manager.get(id)
    }
}
