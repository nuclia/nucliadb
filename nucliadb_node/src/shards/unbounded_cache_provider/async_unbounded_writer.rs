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
use std::sync::Arc;

use async_std::sync::RwLock;
use async_trait::async_trait;
use nucliadb_core::protos::ShardCleaned;
use nucliadb_core::tracing::{debug, error};
use nucliadb_core::{node_error, Context, NodeResult};
use uuid::Uuid;

pub use crate::env;
use crate::shard_metadata::ShardMetadata;
use crate::shards::shards_provider::{AsyncWriterShardsProvider, ShardId};
use crate::shards::ShardWriter;

#[derive(Default)]
pub struct AsyncUnboundedShardWriterCache {
    cache: RwLock<HashMap<ShardId, Arc<ShardWriter>>>,
}

impl AsyncUnboundedShardWriterCache {
    pub fn new() -> Self {
        Self {
            // NOTE: as it's not probable all shards will be written, we don't
            // assign any initial capacity to the HashMap under the
            // consideration a resize blocking is not performance critical while
            // writting.
            cache: RwLock::new(HashMap::new()),
        }
    }

    pub async fn list_loaded_ids(&self) -> Vec<ShardId> {
        self.cache
            .read()
            .await
            .iter()
            .map(|(shard_id, _)| shard_id.clone())
            .collect()
    }
}

#[async_trait]
impl AsyncWriterShardsProvider for AsyncUnboundedShardWriterCache {
    async fn create(&self, metadata: ShardMetadata) -> NodeResult<ShardWriter> {
        let shard_id = Uuid::new_v4().to_string();
        let new_shard = tokio::task::spawn_blocking(move || {
            let shard_path = env::shards_path_id(&shard_id);
            ShardWriter::new(shard_id, &shard_path, metadata)
        })
        .await
        .context("Blocking task panicked")??;
        Ok(new_shard)
    }

    async fn load(&self, id: ShardId) -> NodeResult<()> {
        let shard_path = env::shards_path_id(&id);

        if self.cache.read().await.contains_key(&id) {
            debug!("Shard {shard_path:?} is already on memory");
            return Ok(());
        }

        // Avoid blocking while interacting with the file system
        let id_ = id.clone();
        let shard = tokio::task::spawn_blocking(move || {
            if !shard_path.is_dir() {
                return Err(node_error!("Shard {shard_path:?} is not on disk"));
            }
            ShardWriter::open(id.clone(), &shard_path).map_err(|error| {
                node_error!("Shard {shard_path:?} could not be loaded from disk: {error:?}")
            })
        })
        .await
        .context("Blocking task panicked")??;

        self.cache.write().await.insert(id_, Arc::new(shard));

        Ok(())
    }

    async fn load_all(&self) -> NodeResult<()> {
        let shards_path = env::shards_path();
        let shards = tokio::task::spawn_blocking(move || -> NodeResult<_> {
            let mut shards = HashMap::new();
            for entry in std::fs::read_dir(&shards_path)? {
                let entry = entry?;
                let file_name = entry.file_name().to_str().unwrap().to_string();
                let shard_path = entry.path();
                match ShardWriter::open(file_name.clone(), &shard_path) {
                    Err(err) => error!("Loading shard {shard_path:?} from disk raised {err}"),
                    Ok(shard) => {
                        debug!("Shard loaded: {shard_path:?}");
                        shards.insert(file_name, Arc::new(shard));
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

        tokio::task::spawn_blocking(move || {
            let shard_path = env::shards_path_id(&id);
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
        let (upgraded, details) = tokio::task::spawn_blocking(move || -> NodeResult<_> {
            let shard_path = env::shards_path_id(&id);
            let upgraded = ShardWriter::clean_and_create(id, &shard_path)?;
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
}
