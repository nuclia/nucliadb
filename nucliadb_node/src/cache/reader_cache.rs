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

use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};

use nucliadb_core::{node_error, NodeResult};

use super::resource_cache::{CacheResult, ResourceCache};
use crate::disk_structure;
use crate::errors::ShardNotFoundError;
use crate::settings::Settings;
use crate::shards::metadata::ShardMetadata;
use crate::shards::reader::ShardReader;
use crate::shards::ShardId;

pub struct ShardReaderCache {
    cache: Mutex<ResourceCache<ShardId, ShardReader>>,
    shards_path: PathBuf,
}

impl ShardReaderCache {
    pub fn new(settings: Settings) -> Self {
        let resource_cache = match settings.max_open_shards {
            Some(capacity) => ResourceCache::new_with_capacity(capacity),
            None => ResourceCache::new_unbounded(),
        };
        Self {
            cache: Mutex::new(resource_cache),
            shards_path: settings.shards_path(),
        }
    }

    fn cache(&self) -> MutexGuard<ResourceCache<ShardId, ShardReader>> {
        self.cache.lock().expect("Poisoned cache lock")
    }

    pub fn peek(&self, id: &ShardId) -> Option<Arc<ShardReader>> {
        self.cache().get_cached(id)
    }

    pub fn get(&self, id: &ShardId) -> NodeResult<Arc<ShardReader>> {
        loop {
            let cached = { self.cache().get(id) };
            match cached {
                CacheResult::Cached(shard) => return Ok(shard),
                CacheResult::Wait(waiter) => waiter.wait(),
                CacheResult::Load(guard) => {
                    let loaded = self.load(id)?;
                    self.cache().loaded(guard, &loaded);
                    return Ok(loaded);
                }
            }
        }
    }

    fn load(&self, id: &ShardId) -> NodeResult<Arc<ShardReader>> {
        let shard_path = disk_structure::shard_path_by_id(&self.shards_path.clone(), id);

        if !ShardMetadata::exists(&shard_path) {
            return Err(node_error!(ShardNotFoundError("Shard {shard_path:?} is not on disk")));
        }
        let shard = ShardReader::new(id.clone(), &shard_path)
            .map_err(|error| node_error!("Shard {shard_path:?} could not be loaded from disk: {error:?}"))?;

        Ok(Arc::new(shard))
    }
}
