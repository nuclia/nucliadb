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
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::sleep;
use std::time::Duration;

use nucliadb_core::tracing::debug;
use nucliadb_core::{node_error, NodeResult};

use super::resource_cache::{CacheResult, ResourceCache, ResourceLoadGuard};
use crate::disk_structure;
use crate::errors::ShardNotFoundError;
use crate::settings::Settings;
use crate::shards::metadata::{ShardMetadata, ShardsMetadataManager};
use crate::shards::writer::{NewShard, ShardWriter};
use crate::shards::ShardId;

/// This cache allows the user to block shards, ensuring that they will not be loaded from disk.
/// Being able to do so is crucial, otherwise the only source of truth will be disk and that would
/// not be thread-safe.
struct InnerCache {
    blocked_shards: HashSet<String>,
    active_shards: ResourceCache<ShardId, ShardWriter>,
}

impl InnerCache {
    pub fn new(capacity: Option<NonZeroUsize>) -> InnerCache {
        let active_shards = match capacity {
            Some(capacity) => ResourceCache::new_with_capacity(capacity),
            None => ResourceCache::new_unbounded(),
        };
        Self {
            active_shards,
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
    settings: Settings,
}

impl ShardWriterCache {
    pub fn new(settings: Settings) -> Self {
        Self {
            cache: Mutex::new(InnerCache::new(settings.max_open_shards)),
            shards_path: settings.shards_path(),
            metadata_manager: Arc::new(ShardsMetadataManager::new(settings.shards_path())),
            settings,
        }
    }

    fn cache(&self) -> MutexGuard<InnerCache> {
        self.cache.lock().expect("Poisoned cache lock")
    }

    pub fn create(&self, new: NewShard) -> NodeResult<Arc<ShardWriter>> {
        let shard_id = new.shard_id.clone();
        let (shard, metadata) = ShardWriter::new(new, &self.shards_path, &self.settings)?;
        let shard = Arc::new(shard);

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
            // XXX: DEAD CODE! this function use to acquire a write lock in the shard writer
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
    use std::collections::HashMap;
    use std::fs;
    use std::sync::Arc;
    use std::thread::sleep;
    use std::time::Duration;

    use crossbeam_utils::thread::scope;
    use nucliadb_vectors::config::VectorConfig;
    use tempfile::tempdir;

    use super::ShardWriterCache;
    use crate::settings::{EnvSettings, Settings};
    use crate::shards::indexes::DEFAULT_VECTORS_INDEX_NAME;
    use crate::shards::metadata::ShardMetadata;
    use crate::shards::writer::NewShard;
    use crate::shards::ShardId;

    #[test]
    fn test_safe_deletion() {
        let data_dir = tempdir().unwrap();
        let settings: Settings = EnvSettings {
            data_path: data_dir.into_path(),
            ..Default::default()
        }
        .into();
        let cache = Arc::new(ShardWriterCache::new(settings.clone()));

        let shard_id_0 = ShardId::from("shard_id_0");
        let shard_0_path = settings.shards_path().join(shard_id_0.clone());
        fs::create_dir(settings.shards_path()).unwrap();

        cache
            .create(NewShard {
                kbid: "kbid".to_string(),
                shard_id: shard_id_0.clone(),
                vector_configs: HashMap::from([(DEFAULT_VECTORS_INDEX_NAME.to_string(), VectorConfig::default())]),
            })
            .unwrap();

        let shard_0 = cache.get(&shard_id_0).unwrap();

        scope(|scope| {
            let cache_clone = cache.clone();
            let shard_id_0_clone = shard_id_0.clone();
            let delete_thread = scope.spawn(move |_| {
                sleep(Duration::from_millis(50));
                cache_clone.delete(&shard_id_0_clone).unwrap();
            });

            // I should still be able to get the shard before deletion starts
            assert!(cache.get(&shard_id_0).is_ok());

            // The other thread will try to delete the shard
            // we will keep using it for a while, making sure
            // it is not deleted until after we are done with it.
            for _ in 0..10 {
                assert!(ShardMetadata::exists(&shard_0_path));
                sleep(Duration::from_millis(50));
            }

            // Shard is under deletion, I should not be able to get it
            assert!(cache.get(&shard_id_0).is_err());

            // Drop the shard Arc so it can be deleted
            drop(shard_0);

            // The other thread should finish now, and it should delete the shard
            delete_thread.join().unwrap();
            assert!(!ShardMetadata::exists(&shard_0_path));
        })
        .unwrap();

        // Shard is deleted, getting it should fail to load
        assert!(cache.get(&shard_id_0).is_err());

        // Recreating the shard should work (i.e: it's not stuck in the deletion state)
        cache
            .create(NewShard {
                kbid: "kbid".to_string(),
                shard_id: shard_id_0.clone(),
                vector_configs: HashMap::from([(DEFAULT_VECTORS_INDEX_NAME.to_string(), VectorConfig::default())]),
            })
            .unwrap();

        assert!(cache.get(&shard_id_0).is_ok());
    }
}
