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
use nucliadb_core::tracing::{debug, error};
use nucliadb_core::{node_error, Context, NodeResult};
use tokio::sync::RwLock;

use crate::disk_structure;
use crate::settings::Settings;
use crate::shards::providers::AsyncShardReaderProvider;
use crate::shards::reader::ShardReader;
use crate::shards::ShardId;

#[derive(Default)]
pub struct AsyncUnboundedShardReaderCache {
    cache: RwLock<HashMap<ShardId, Arc<ShardReader>>>,
    shards_path: PathBuf,
}

impl AsyncUnboundedShardReaderCache {
    pub fn new(settings: Settings) -> Self {
        Self {
            // NOTE: we use max shards per node as initial capacity to avoid
            // hashmap resizing, as it would block the current thread while
            // doing it.
            //
            // REVIEW: if resize don't take more than 10µs, it's acceptable
            // (blocking in tokio means CPU bound during 10-100µs)
            cache: RwLock::new(HashMap::with_capacity(settings.max_shards_per_node())),
            shards_path: settings.shards_path(),
        }
    }
}

#[async_trait]
impl AsyncShardReaderProvider for AsyncUnboundedShardReaderCache {
    async fn load(&self, id: ShardId) -> NodeResult<Arc<ShardReader>> {
        let shard_path = disk_structure::shard_path_by_id(&self.shards_path.clone(), &id);
        let mut cache_writer = self.cache.write().await;

        if let Some(shard) = cache_writer.get(&id) {
            debug!("Shard {shard_path:?} is already on memory");
            return Ok(Arc::clone(shard));
        }

        // Avoid blocking while interacting with the file system (reads and
        // writes to disk)
        let id_ = id.clone();
        let shard = tokio::task::spawn_blocking(move || {
            if !shard_path.is_dir() {
                return Err(node_error!("Shard {shard_path:?} is not on disk"));
            }
            ShardReader::new(id.clone(), &shard_path).map_err(|error| {
                node_error!("Shard {shard_path:?} could not be loaded from disk: {error:?}")
            })
        })
        .await
        .context("Blocking task panicked")??;

        let shard = Arc::new(shard);
        cache_writer.insert(id_, Arc::clone(&shard));
        Ok(shard)
    }

    async fn load_all(&self) -> NodeResult<()> {
        let shards_path = self.shards_path.clone();
        let mut shards = tokio::task::spawn_blocking(move || -> NodeResult<_> {
            let mut shards = HashMap::new();
            for entry in std::fs::read_dir(&shards_path)? {
                let entry = entry?;
                let file_name = entry.file_name().to_str().unwrap().to_string();
                let shard_path = entry.path();
                match ShardReader::new(file_name.clone(), &shard_path) {
                    Err(err) => error!("Loading shard {shard_path:?} from disk raised {err}"),
                    Ok(shard) => {
                        debug!("Shard loaded: {shard_path:?}");
                        shards.insert(file_name, shard);
                    }
                }
            }
            Ok(shards)
        })
        .await
        .context("Blocking task panicked")??;

        {
            let mut cache = self.cache.write().await;
            shards.drain().for_each(|(k, v)| {
                cache.insert(k, Arc::new(v));
            });
        }
        Ok(())
    }

    async fn get(&self, id: ShardId) -> Option<Arc<ShardReader>> {
        self.cache.read().await.get(&id).map(Arc::clone)
    }
}
