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

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::sleep;
use std::time::Duration;

use nucliadb_core::tracing::debug;
use nucliadb_core::{node_error, NodeResult};

use super::resource_cache::{CacheResult, ResourceCache, ResourceLoadGuard};
use crate::disk_structure;
use crate::settings::Settings;
use crate::shards::errors::ShardNotFoundError;
use crate::shards::metadata::{ShardMetadata, ShardsMetadataManager};
use crate::shards::writer::ShardWriter;
use crate::shards::ShardId;

/// This cache allows the user to block shards, ensuring that they will not be loaded from disk.
/// Being able to do so is crucial, otherwise the only source of truth will be disk and that would
/// not be thread-safe.
struct InnerCache {
    blocked_shards: HashSet<String>,
    active_shards: ResourceCache<ShardId, ShardWriter>,
}

impl InnerCache {
    pub fn new() -> InnerCache {
        Self {
            active_shards: ResourceCache::new_unbounded(),
            blocked_shards: HashSet::new(),
        }
    }

    pub fn peek(&mut self, id: &ShardId) -> Option<Arc<ShardWriter>> {
        if self.blocked_shards.contains(id) {
            return None;
        }

        self.active_shards.get_cached(id)
    }

    pub fn get(&mut self, id: &ShardId) -> NodeResult<CacheResult<ShardId, ShardWriter>> {
        if self.blocked_shards.contains(id) {
            return Err(node_error!(ShardNotFoundError("Shard {shard_path:?} is not on disk")));
        }

        Ok(self.active_shards.get(id))
    }

    pub fn shard_loaded(
        &mut self,
        guard: ResourceLoadGuard<ShardId>,
        shard: Arc<ShardWriter>,
    ) -> NodeResult<Arc<ShardWriter>> {
        if self.blocked_shards.contains(&shard.id) {
            return Err(node_error!(ShardNotFoundError("Shard {shard_path:?} is not on disk")));
        }

        self.active_shards.loaded(guard, &shard);
        Ok(shard)
    }

    pub fn set_being_deleted(&mut self, id: ShardId) {
        self.blocked_shards.insert(id);
    }

    pub fn remove(&mut self, id: &ShardId) {
        self.blocked_shards.remove(id);
        self.active_shards.remove(id);
    }

    pub fn add_active_shard(&mut self, id: &ShardId, shard: &Arc<ShardWriter>) {
        // It would be a dangerous bug to have a path
        // in the system that leads to this assertion failing.
        assert!(!self.blocked_shards.contains(id));

        self.active_shards.insert(id, shard);
    }
}

pub struct ShardWriterCache {
    pub shards_path: PathBuf,
    cache: Mutex<InnerCache>,
    metadata_manager: Arc<ShardsMetadataManager>,
}

impl ShardWriterCache {
    pub fn new(settings: Settings) -> Self {
        Self {
            cache: Mutex::new(InnerCache::new()),
            shards_path: settings.shards_path(),
            metadata_manager: Arc::new(ShardsMetadataManager::new(settings.shards_path())),
        }
    }

    fn cache(&self) -> MutexGuard<InnerCache> {
        self.cache.lock().expect("Poisoned cache lock")
    }

    pub fn create(&self, metadata: ShardMetadata) -> NodeResult<Arc<ShardWriter>> {
        let shard_id = metadata.id();
        let metadata = Arc::new(metadata);
        let shard = Arc::new(ShardWriter::new(metadata.clone())?);

        self.metadata_manager.add_metadata(metadata);
        self.cache().add_active_shard(&shard_id, &shard);

        Ok(shard)
    }

    pub fn peek(&self, id: &ShardId) -> Option<Arc<ShardWriter>> {
        self.cache().peek(id)
    }

    pub fn get(&self, id: &ShardId) -> NodeResult<Arc<ShardWriter>> {
        loop {
            let cached = { self.cache().get(id) }?;
            match cached {
                CacheResult::Cached(shard) => return Ok(shard),
                CacheResult::Wait(waiter) => waiter.wait(),
                CacheResult::Load(guard) => {
                    let loaded = self.load(id)?;
                    return self.cache().shard_loaded(guard, loaded);
                }
            }
        }
    }

    fn load(&self, id: &ShardId) -> NodeResult<Arc<ShardWriter>> {
        let metadata_manager = Arc::clone(&self.metadata_manager);
        let shard_path = disk_structure::shard_path_by_id(&self.shards_path.clone(), id);

        if !ShardMetadata::exists(&shard_path) {
            return Err(node_error!(ShardNotFoundError("Shard {shard_path:?} is not on disk")));
        }
        let metadata = metadata_manager.get(id.clone()).expect("Shard metadata not found. This should not happen");
        let shard = ShardWriter::open(metadata)
            .map_err(|error| node_error!("Shard {shard_path:?} could not be loaded from disk: {error:?}"))?;

        Ok(Arc::new(shard))
    }

    fn mark_as_deleted(&self, id: &ShardId) {
        let shard = {
            // First the shard must be marked as being deleted, this way
            // concurrent tasks can not make the mistake of trying to use it.
            self.cache().set_being_deleted(id.clone());

            // Even though the shard was marked as deleted, it may already be in the active shards
            // list.
            loop {
                match self.cache().active_shards.get(id) {
                    CacheResult::Cached(shard) => {
                        break Some(shard);
                    }
                    CacheResult::Load(_) => break None,         // Not in cache
                    CacheResult::Wait(waiter) => waiter.wait(), // Someone else loading, wait
                }
            }
        };

        if let Some(shard) = shard {
            // The shard was still cached, there may be operations running on it. We must ensure
            // that all of them have finished before proceeding.
            // let _blocking_token = shard.block_shard();
            // At this point we can ensure that no operations are being performed in this shard.
            // Next operations will require using the cache, where the shard is marked as deleted.
            self.cache().active_shards.remove(id);
            let weak = Arc::downgrade(&shard);
            drop(shard);

            // Wait until all tasks using this shard are finished
            loop {
                if weak.strong_count() == 0 {
                    break;
                }
                sleep(Duration::from_millis(100));
            }
        }
    }

    pub fn delete(&self, id: &ShardId) -> NodeResult<()> {
        // Mark the shard as deleted and wait until not in use
        self.mark_as_deleted(id);

        // In case of error while deleting the function will return without removing
        // The deletion flag, this is to avoid accesses to a partially deleted shard.
        let shard_path = disk_structure::shard_path_by_id(&self.shards_path.clone(), id);
        if shard_path.exists() {
            debug!("Deleting shard {shard_path:?}");
            std::fs::remove_dir_all(shard_path)?;
        }

        // If the shard was successfully deleted is safe to remove
        // the entry from the cache.
        self.cache().remove(id);

        Ok(())
    }

    pub fn get_metadata(&self, id: ShardId) -> Option<Arc<ShardMetadata>> {
        self.metadata_manager.get(id)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::Arc;
    use std::thread::sleep;
    use std::time::Duration;

    use crossbeam_utils::thread::scope;
    use tempfile::tempdir;

    use super::ShardWriterCache;
    use crate::settings::{InnerSettings, Settings};
    use crate::shards::metadata::{ShardMetadata, Similarity};
    use crate::shards::ShardId;
}
