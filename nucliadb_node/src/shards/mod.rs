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

use tokio::sync::RwLock;
use anyhow::anyhow;
use lazy_static::lazy_static;

use nucliadb_core::NodeResult;
use nucliadb_core::env;
use nucliadb_core::tracing::debug;

use crate::services::reader::ShardReaderService as ShardReader;
use crate::services::writer::ShardWriterService as ShardWriter;

pub enum ShardMode {
    Read,
    Write,
}

pub type ShardId = String;

lazy_static! {
    static ref READER_SHARDS_PROVIDER: Arc<RwLock<UnboundedShardReaderCache>> = Arc::new(RwLock::new(UnboundedShardReaderCache::new()));
}

fn reader_shards_provider() -> Arc<RwLock<impl ReaderShardsProvider>> {
    READER_SHARDS_PROVIDER.clone()
}

// fn writer_shards_provider() -> RwLock<impl WriterShardsProvider> {

// }

pub trait ReaderShardsProvider {
    fn load(&mut self, id: ShardId) -> NodeResult<()>;
    fn load_all(&mut self) -> NodeResult<()>;

    fn get(&self, id: ShardId) -> Option<&ShardReader>;

    fn iter(&self) -> Iter<'_, ShardReader>;
}

pub trait WriterShardsProvider {
    fn load(&mut self, id: ShardId) -> NodeResult<()>;
    fn load_all(&mut self) -> NodeResult<()>;

    fn get(&self, id: ShardId) -> Option<&ShardWriter>;
    fn get_mut(&self, id: ShardId) -> Option<&mut ShardWriter>;

}

struct UnboundedShardReaderCache {
    cache: HashMap<ShardId, ShardReader>
}

impl UnboundedShardReaderCache {
    pub fn new() -> Self {
        Self {
            cache: HashMap::new(),
        }
    }
}

impl ReaderShardsProvider for UnboundedShardReaderCache {
   fn load(&mut self, id: ShardId) -> NodeResult<()> {
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

    fn load_all(&mut self) -> NodeResult<()> {
        todo!()
    }

    fn get(&self, id: ShardId) -> Option<&ShardReader> {
        todo!()
    }

    fn iter(&self) -> Iter<'_, ShardReader> {
        todo!()
    }
}
