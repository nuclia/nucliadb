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
use std::slice::Iter;
use std::sync::Arc;

use anyhow::anyhow;
use async_std::sync::RwLock;
use async_trait::async_trait;
use nucliadb_core::tracing::debug;
use nucliadb_core::{env, NodeResult};
use nucliadb_vectors::data_point::Similarity;

pub use crate::services::reader::ShardReaderService as ShardReader;
pub use crate::services::writer::ShardWriterService as ShardWriter;
use crate::utils::nonblocking;

pub type ShardId = String;

#[async_trait]
pub trait ReaderShardsProvider: Send + Sync {
    async fn load(&self, id: ShardId) -> NodeResult<()>;
    async fn load_all(&self) -> NodeResult<()>;

    async fn get(&self, id: ShardId) -> Option<Arc<ShardReader>>;

    // fn iter(&self) -> Iter<'_, ShardReader>;
}

// pub trait WriterShardsProvider {
//     fn create(&self, id: ShardId, kbid: String, similarity: Similarity);

//     fn load(&self, id: ShardId) -> NodeResult<()>;
//     fn load_all(&mut self) -> NodeResult<()>;

//     fn get(&self, id: ShardId) -> Option<&ShardWriter>;
//     fn get_mut(&self, id: ShardId) -> Option<&mut ShardWriter>;

//     fn delete(&self, id: ShardId) -> NodeResult<()>;
// }

pub struct UnboundedShardReaderCache {
    cache: RwLock<HashMap<ShardId, Arc<ShardReader>>>,
}

impl UnboundedShardReaderCache {
    pub fn new() -> Self {
        Self {
            cache: RwLock::new(HashMap::with_capacity(env::max_shards_per_node())),
        }
    }
}

#[async_trait]
impl ReaderShardsProvider for UnboundedShardReaderCache {
    async fn load(&self, id: ShardId) -> NodeResult<()> {
        let shard_path = env::shards_path_id(&id);

        if self.cache.read().await.contains_key(&id) {
            debug!("Shard {shard_path:?} is already on memory");
            return Ok(());
        }

        // Avoid blocking while interacting with the file system (reads and
        // writes to disk)
        let _id = id.clone();
        let shard = nonblocking!({
            if !shard_path.is_dir() {
                return Err(anyhow!("Shard {shard_path:?} is not on disk"));
            }
            let shard = ShardReader::new(id.clone(), &shard_path).map_err(|error| {
                anyhow!("Shard {shard_path:?} could not be loaded from disk: {error:?}")
            })?;

            Ok(shard)
        })?;

        self.cache.write().await.insert(_id, Arc::new(shard));
        Ok(())
    }

    async fn load_all(&self) -> NodeResult<()> {
        todo!()
    }

    async fn get(&self, id: ShardId) -> Option<Arc<ShardReader>> {
        self.cache.read().await.get(&id).map(Arc::clone)
    }

    // fn iter(&self) -> Iter<'_, ShardReader> {
    //     todo!()
    // }
}
