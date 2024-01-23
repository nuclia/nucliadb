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
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use nucliadb_core::tracing::{debug, error};
use nucliadb_core::{node_error, NodeResult};

use crate::disk_structure;
use crate::settings::Settings;
use crate::shards::errors::ShardNotFoundError;
use crate::shards::metadata::ShardMetadata;
use crate::shards::reader::ShardReader;
use crate::shards::ShardId;

#[derive(Default)]
pub struct UnboundedShardReaderCache {
    cache: RwLock<HashMap<ShardId, Arc<ShardReader>>>,
    shards_path: PathBuf,
}

impl UnboundedShardReaderCache {
    pub fn new(settings: Settings) -> Self {
        Self {
            // NOTE: we use max shards per node as initial capacity to avoid
            // hashmap resizing, as it would hold the lock while doing it.
            cache: RwLock::new(HashMap::with_capacity(settings.max_shards_per_node())),
            shards_path: settings.shards_path(),
        }
    }

    pub fn read(&self) -> RwLockReadGuard<HashMap<ShardId, Arc<ShardReader>>> {
        self.cache.read().expect("Poisoned lock while reading")
    }

    pub fn write(&self) -> RwLockWriteGuard<HashMap<ShardId, Arc<ShardReader>>> {
        self.cache.write().expect("Poisoned lock while reading")
    }

    pub fn load(&self, id: ShardId) -> NodeResult<Arc<ShardReader>> {
        let shard_path = disk_structure::shard_path_by_id(&self.shards_path.clone(), &id);
        let mut cache_writer = self.write();

        if let Some(shard) = cache_writer.get(&id) {
            debug!("Shard {shard_path:?} is already on memory");
            return Ok(Arc::clone(shard));
        }

        if !ShardMetadata::exists(&shard_path) {
            return Err(node_error!(ShardNotFoundError(
                "Shard {shard_path:?} is not on disk"
            )));
        }
        let shard = ShardReader::new(id.clone(), &shard_path).map_err(|error| {
            node_error!("Shard {shard_path:?} could not be loaded from disk: {error:?}")
        })?;

        let shard = Arc::new(shard);
        cache_writer.insert(id, Arc::clone(&shard));

        Ok(shard)
    }

    pub fn load_all(&self) -> NodeResult<()> {
        let mut cache = self.write();
        let shards_path = self.shards_path.clone();

        for entry in std::fs::read_dir(&shards_path)? {
            let entry = entry?;
            let file_name = entry.file_name().to_str().unwrap().to_string();
            let shard_path = entry.path();
            match ShardReader::new(file_name.clone(), &shard_path) {
                Err(err) => error!("Loading shard {shard_path:?} from disk raised {err}"),
                Ok(shard) => {
                    debug!("Shard loaded: {shard_path:?}");
                    cache.insert(file_name, Arc::new(shard));
                }
            }
        }

        Ok(())
    }

    pub fn get(&self, id: ShardId) -> Option<Arc<ShardReader>> {
        self.read().get(&id).map(Arc::clone)
    }
}
