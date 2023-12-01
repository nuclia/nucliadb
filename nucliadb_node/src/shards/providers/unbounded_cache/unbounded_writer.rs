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

use nucliadb_core::protos::ShardCleaned;
use nucliadb_core::tracing::{debug, error};
use nucliadb_core::{node_error, NodeResult};
use uuid::Uuid;

use crate::disk_structure;
use crate::settings::Settings;
use crate::shards::errors::ShardNotFoundError;
use crate::shards::metadata::ShardMetadata;
use crate::shards::providers::ShardWriterProvider;
use crate::shards::writer::ShardWriter;
use crate::shards::ShardId;

#[derive(Default)]
pub struct UnboundedShardWriterCache {
    cache: RwLock<HashMap<ShardId, Arc<ShardWriter>>>,
    pub shards_path: PathBuf,
}

impl UnboundedShardWriterCache {
    pub fn new(settings: Settings) -> Self {
        Self {
            cache: RwLock::new(HashMap::new()),
            shards_path: settings.shards_path(),
        }
    }

    fn read(&self) -> RwLockReadGuard<HashMap<ShardId, Arc<ShardWriter>>> {
        self.cache.read().expect("Poisoned lock while reading")
    }

    fn write(&self) -> RwLockWriteGuard<HashMap<ShardId, Arc<ShardWriter>>> {
        self.cache.write().expect("Poisoned lock while reading")
    }
}

impl ShardWriterProvider for UnboundedShardWriterCache {
    fn create(&self, metadata: ShardMetadata) -> NodeResult<Arc<ShardWriter>> {
        let shard_id = Uuid::new_v4().to_string();
        let cache_shard_id = shard_id.clone();
        let shard_path = disk_structure::shard_path_by_id(&self.shards_path.clone(), &shard_id);
        let new_shard = ShardWriter::new(shard_id, &shard_path, metadata).map(Arc::new)?;
        let returned_shard = Arc::clone(&new_shard);
        self.write().insert(cache_shard_id, new_shard);
        Ok(returned_shard)
    }

    fn load(&self, id: ShardId) -> NodeResult<Arc<ShardWriter>> {
        let shard_path = disk_structure::shard_path_by_id(&self.shards_path.clone(), &id);
        let mut cache_writer = self.write();

        if let Some(shard) = cache_writer.get(&id) {
            debug!("Shard {shard_path:?} is already on memory");
            return Ok(Arc::clone(shard));
        }

        // Avoid blocking while interacting with the file system
        if !shard_path.is_dir() {
            return Err(node_error!(ShardNotFoundError(
                "Shard {shard_path:?} is not on disk"
            )));
        }
        let shard = ShardWriter::open(id.clone(), &shard_path).map_err(|error| {
            node_error!("Shard {shard_path:?} could not be loaded from disk: {error:?}")
        })?;

        let shard = Arc::new(shard);
        cache_writer.insert(id, Arc::clone(&shard));
        Ok(shard)
    }

    fn load_all(&self) -> NodeResult<()> {
        let mut cache = self.write();
        for entry in std::fs::read_dir(&self.shards_path)? {
            let entry = entry?;
            let file_name = entry.file_name().to_str().unwrap().to_string();
            let shard_path = entry.path();
            match ShardWriter::open(file_name.clone(), &shard_path) {
                Err(err) => error!("Loading shard {shard_path:?} from disk raised {err}"),
                Ok(shard) => {
                    debug!("Shard loaded: {shard_path:?}");
                    cache.insert(file_name, Arc::new(shard));
                }
            }
        }
        Ok(())
    }

    fn get(&self, id: ShardId) -> Option<Arc<ShardWriter>> {
        self.read().get(&id).map(Arc::clone)
    }

    fn delete(&self, id: ShardId) -> NodeResult<()> {
        self.write().remove(&id);

        let shard_path = disk_structure::shard_path_by_id(&self.shards_path.clone(), &id);
        if shard_path.exists() {
            debug!("Deleting shard {shard_path:?}");
            std::fs::remove_dir_all(shard_path)?;
        }
        Ok(())
    }

    fn upgrade(&self, id: ShardId) -> NodeResult<ShardCleaned> {
        self.write().remove(&id);

        let shard_path = disk_structure::shard_path_by_id(&self.shards_path.clone(), &id);
        let upgraded = ShardWriter::clean_and_create(id.clone(), &shard_path)?;
        let details = ShardCleaned {
            document_service: upgraded.document_version() as i32,
            paragraph_service: upgraded.paragraph_version() as i32,
            vector_service: upgraded.vector_version() as i32,
            relation_service: upgraded.relation_version() as i32,
        };

        self.write().insert(id, Arc::new(upgraded));
        Ok(details)
    }
}
