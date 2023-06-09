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


use std::slice::Iter;
use dashmap::DashMap;
use anyhow::anyhow;

use nucliadb_core::NodeResult;
use nucliadb_core::env;
use nucliadb_core::tracing::debug;
use nucliadb_vectors::data_point::Similarity;

use crate::services::reader::ShardReaderService as ShardReader;
use crate::services::writer::ShardWriterService as ShardWriter;

pub type ShardId = String;


pub trait ReaderShardsProvider: Send + Sync {
    fn load(&self, id: ShardId) -> NodeResult<()>;
    fn load_all(&self) -> NodeResult<()>;

    fn get(&self, id: ShardId) -> Option<ShardReaderRef>;

    fn iter(&self) -> Iter<'_, ShardReader>;
}

pub trait WriterShardsProvider {
    fn create(&self, id: ShardId, kbid: String, similarity: Similarity);

    fn load(&self, id: ShardId) -> NodeResult<()>;
    fn load_all(&mut self) -> NodeResult<()>;

    fn get(&self, id: ShardId) -> Option<&ShardWriter>;
    fn get_mut(&self, id: ShardId) -> Option<&mut ShardWriter>;

    fn delete(&self, id: ShardId) -> NodeResult<()>;
}

pub enum ShardReaderRef<'a> {
    DashMap(dashmap::mapref::one::Ref<'a, ShardId, ShardReader>)
}

impl std::ops::Deref for ShardReaderRef<'_> {
    type Target = ShardReader;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::DashMap(r) => r.value(),
        }
    }
}

pub struct UnboundedShardReaderCache {
    cache: DashMap<ShardId, ShardReader>
}

impl UnboundedShardReaderCache {
    pub fn new() -> Self {
        Self {
            cache: DashMap::new(),
        }
    }
}

impl ReaderShardsProvider for UnboundedShardReaderCache {
   fn load(&self, id: ShardId) -> NodeResult<()> {
       let shard_path = env::shards_path_id(&id);

       if self.cache.contains_key(&id) {
           debug!("Shard {shard_path:?} is already on memory");
           return Ok(());
       }

       if !shard_path.is_dir() {
           return Err(anyhow!("Shard {shard_path:?} is not on disk"))
       }

       let shard = ShardReader::new(id.clone(), &shard_path)
           .map_err(|error| {
               anyhow!("Shard {shard_path:?} could not be loaded from disk: {error:?}")
           })?;

       self.cache.insert(id, shard);
       Ok(())
   }

    fn load_all(&self) -> NodeResult<()> {
        todo!()
    }

    fn get(&self, id: ShardId) -> Option<ShardReaderRef> {
        self.cache.get(&id).map(|item| ShardReaderRef::DashMap(item))
    }

    fn iter(&self) -> Iter<'_, ShardReader> {
        todo!()
    }
}
