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
//

// This module is to manage shard change state.
// One of the main purposes of this module is to avoid fully loading a shard
// into memory but still still be able to inspect and manage aspects of it.

use crate::disk_structure::GENERATION_FILE;
use std::sync::{Arc, RwLock};
use std::{collections::HashMap, path::PathBuf};

#[derive(Default, Debug)]
pub struct ShardChangeStateManager {
    shard_path: PathBuf,
    // A generation id is a way to track if a shard has changed.
    // A new id means that something in the shard has changed.
    // This is used by replication to track which shards have changed
    // and to efficiently replicate them.
    generation_id: RwLock<Option<String>>,
}

#[derive(Default, Debug)]
pub struct ShardsChangeStateManager {
    shards_path: PathBuf,
    shards: RwLock<HashMap<String, Arc<ShardChangeStateManager>>>,
}

impl ShardsChangeStateManager {
    pub fn new(shards_path: PathBuf) -> Self {
        Self {
            shards: RwLock::new(HashMap::new()),
            shards_path,
        }
    }

    pub fn get(&self, shard_id: String) -> Option<Arc<ShardChangeStateManager>> {
        if let Ok(shards) = self.shards.read() {
            if shards.contains_key(&shard_id) {
                return Some(Arc::clone(shards.get(&shard_id).unwrap()));
            }
        }
        if let Ok(mut shards) = self.shards.write() {
            let sm = Arc::new(ShardChangeStateManager::new(
                self.shards_path.join(shard_id.clone()),
            ));
            shards.insert(shard_id.clone(), Arc::clone(&sm));
            return Some(sm);
        }
        None
    }
}

impl ShardChangeStateManager {
    pub fn new(shard_path: PathBuf) -> Self {
        Self {
            shard_path,
            generation_id: RwLock::new(None),
        }
    }

    pub fn get_generation_id(&self) -> Option<String> {
        let gen_id_read = self.generation_id.read().unwrap();
        match &*gen_id_read {
            Some(value) => return Some(value.clone()),
            None => {}
        }

        let filepath = self.shard_path.join(GENERATION_FILE);
        // check if file does not exist
        if filepath.exists() {
            let gen_id = std::fs::read_to_string(filepath).unwrap();
            let mut gen_id_write = self.generation_id.write().unwrap();
            *gen_id_write = Some(gen_id.clone());
            Some(gen_id)
        } else {
            None
        }
    }

    pub fn new_generation_id(&self) -> String {
        let generation_id = uuid::Uuid::new_v4().to_string();
        self.set_generation_id(generation_id.clone());
        generation_id
    }

    pub fn set_generation_id(&self, generation_id: String) {
        let filepath = self.shard_path.join(GENERATION_FILE);
        std::fs::write(filepath, generation_id.clone()).unwrap();
        let mut gen_id_write = self.generation_id.write().unwrap();
        *gen_id_write = Some(generation_id);
    }
}
