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

use std::collections::{HashMap, HashSet};
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

/// Each shard may be in one of this states
enum ShardCacheStatus {
    /// Not in cache and not being deleted, therefore if found in on disk, loading it is safe.
    NotInCache,
    /// The shard is cached, but there is a task in the process of deleting it.
    BeingDeleted,
    /// The shard is not being deleted and is cached
    InCache(Arc<ShardWriter>),
}

/// This cache allows the user to block shards, ensuring that they will not be loaded from disk.
/// Being able to do so is crucial, otherwise the only source of truth will be disk and that would
/// not be thread-safe.
#[derive(Default)]
struct InnerCache {
    blocked_shards: HashSet<String>,
    active_shards: HashMap<ShardId, Arc<ShardWriter>>,
}

impl InnerCache {
    pub fn new() -> InnerCache {
        Self::default()
    }
    pub fn get_shard(&self, id: &ShardId) -> ShardCacheStatus {
        match self.active_shards.get(id).cloned() {
            _ if self.blocked_shards.contains(id) => ShardCacheStatus::BeingDeleted,
            Some(shard) => ShardCacheStatus::InCache(shard),
            None => ShardCacheStatus::NotInCache,
        }
    }
    pub fn set_being_deleted(&mut self, id: ShardId) {
        self.blocked_shards.insert(id);
    }
    pub fn remove(&mut self, id: &ShardId) {
        self.blocked_shards.remove(id);
        self.active_shards.remove(id);
    }
    pub fn add_active_shard(&mut self, id: ShardId, shard: Arc<ShardWriter>) {
        // It would be a dangerous bug to have a path
        // in the system that leads to this assertion failing.
        assert!(!self.blocked_shards.contains(&id));

        self.active_shards.insert(id, shard);
    }
}

#[derive(Default)]
pub struct AsyncUnboundedShardWriterCache {
    pub shards_path: PathBuf,
    cache: RwLock<InnerCache>,
    metadata_manager: Arc<ShardsMetadataManager>,
}

impl AsyncUnboundedShardWriterCache {
    pub fn new(settings: Settings) -> Self {
        Self {
            // NOTE: as it's not probable all shards will be written, we don't
            // assign any initial capacity to the HashMap under the consideration
            // a resize blocking is not performance critical while writing.
            cache: RwLock::new(InnerCache::default()),
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
        let shard_metadata = Arc::clone(&metadata);
        let shard = tokio::task::spawn_blocking(move || ShardWriter::new(shard_metadata))
            .await
            .context("Blocking task panicked")??;
        let shard = Arc::new(shard);
        let shard_cache_clone = Arc::clone(&shard);
        self.metadata_manager.add_metadata(metadata);

        let mut cache_writer = self.cache.write().await;
        cache_writer.add_active_shard(shard_id, shard_cache_clone);
        Ok(shard)
    }

    async fn load(&self, id: ShardId) -> NodeResult<Arc<ShardWriter>> {
        let shard_key = id.clone();
        let shard_path = disk_structure::shard_path_by_id(&self.shards_path.clone(), &id);
        let mut cache_writer = self.cache.write().await;
        match cache_writer.get_shard(&id) {
            ShardCacheStatus::InCache(shard) => Ok(shard),
            ShardCacheStatus::BeingDeleted => Err(node_error!(ShardNotFoundError(
                "Shard {shard_path:?} is not on disk"
            ))),
            ShardCacheStatus::NotInCache => {
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
                cache_writer.add_active_shard(shard_key, cache_shard);
                Ok(shard)
            }
        }
    }

    async fn load_all(&self) -> NodeResult<()> {
        let shards_path = self.shards_path.clone();
        let metadata_manager = Arc::clone(&self.metadata_manager);
        let shards = tokio::task::spawn_blocking(move || -> NodeResult<_> {
            let mut shards = InnerCache::new();
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
                        shards.add_active_shard(shard_id, Arc::new(shard));
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
        let cache_reader = self.cache.read().await;
        let ShardCacheStatus::InCache(shard) = cache_reader.get_shard(&id) else {
            return None;
        };

        Some(shard)
    }

    async fn delete(&self, id: ShardId) -> NodeResult<()> {
        let mut cache_writer = self.cache.write().await;
        // First the shard must be marked as being deleted, this way
        // concurrent tasks can not make the mistake of trying to use it.
        cache_writer.set_being_deleted(id.clone());

        // Even though the shard was marked as deleted, if it was already in the
        // active shards list there may be operations running on it. We must ensure
        // that all of them have finished before proceeding.
        if let Some(shard) = cache_writer.active_shards.get(&id).cloned() {
            std::mem::drop(cache_writer);
            let blocking_token = shard.block_shard().await;
            // At this point we can ensure that no operations
            // are being performed in this shard. Next operations
            // will require using the cache, where the shard is marked
            // as deleted.
            std::mem::drop(blocking_token);
        } else {
            // Dropping the cache writer because is not needed while deleting the shard.
            std::mem::drop(cache_writer);
        }

        // No need to hold the lock while deletion happens.
        // In case of error while deleting the function will return without removing
        // The deletion flag, this is to avoid accesses to a partially deleted shard.
        let shard_path = disk_structure::shard_path_by_id(&self.shards_path.clone(), &id);
        tokio::task::spawn_blocking(move || -> NodeResult<()> {
            if shard_path.exists() {
                debug!("Deleting shard {shard_path:?}");
                std::fs::remove_dir_all(shard_path)?;
            }
            Ok(())
        })
        .await
        .context("Blocking task panicked")??;

        // If the shard was successfully deleted is safe to remove
        // the entry from the cache.
        self.cache.write().await.remove(&id);

        Ok(())
    }

    async fn upgrade(&self, id: ShardId) -> NodeResult<ShardCleaned> {
        let mut cache_writer = self.cache.write().await;
        // First the shard must be marked as being deleted, this way
        // concurrent tasks can not make the mistake of trying to use it.
        cache_writer.set_being_deleted(id.clone());

        // Even though the shard was marked as deleted, if it was already in the
        // active shards list there may be operations running on it. We must ensure
        // that all of them have finished before proceeding.
        if let Some(shard) = cache_writer.active_shards.get(&id).cloned() {
            std::mem::drop(cache_writer);
            let blocking_token = shard.block_shard().await;
            // At this point we can ensure that no operations
            // are being performed in this shard. Next operations
            // will require using the cache, where the shard is marked
            // as deleted.
            std::mem::drop(blocking_token);
        } else {
            // Dropping the cache writer because is not needed while deleting the shard.
            std::mem::drop(cache_writer);
        }

        let metadata = self.metadata_manager.get(id.clone());
        // If upgrading fails, the safe thing is to keep the being deleted flag
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

        // The shard was upgraded, is safe to allow access again
        let shard = Arc::new(upgraded);
        let mut cache_writer = self.cache.write().await;
        // Old shard is completely removed
        cache_writer.remove(&id);
        // The clean and upgraded version takes its place
        cache_writer.add_active_shard(id, shard);
        Ok(details)
    }

    fn get_metadata(&self, id: ShardId) -> Option<Arc<ShardMetadata>> {
        self.metadata_manager.get(id)
    }
}
