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

use nucliadb_index;
use nucliadb_index::core::Context;
use nucliadb_index::index;

use std::path::PathBuf;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

pub struct ShardManager {
    shards: HashMap<String, Arc<Mutex<index::Index>>>,
    data_dir: PathBuf,
}

impl ShardManager {
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            shards: HashMap::new(),
            data_dir: data_dir,
        }
    }

    pub fn load(&mut self) -> Result<(), String> {
        // go through every directory in data_dir and load shards
        for entry in std::fs::read_dir(self.data_dir.as_path()).unwrap() {
            let entry = entry.unwrap();
            let shard_id = entry.file_name().into_string().unwrap();
            self.load_shard(shard_id.as_str())?;
        }

        Ok(())
    }

    pub fn load_shard(&mut self, shard_id: &str) -> Result<(), String> {
        if self.exists(shard_id) {
            // already loaded
            return Ok(());
        }

        let shard_path = self.data_dir.join(shard_id);
        let context = Context::new(shard_path.as_path());
        let idx = index::Index::open(context).unwrap();
        self.shards
            .insert(shard_id.to_string(), Arc::new(Mutex::new(idx)));

        Ok(())
    }

    pub fn create_shard(&mut self, shard_id: &str) -> Result<(), String> {
        if self.exists(shard_id) {
            return Err(format!("Shard {} already exists", shard_id));
        }

        let shard_path = self.data_dir.join(shard_id);
        let context = Context::new(shard_path.as_path());
        if !shard_path.exists() {
            // create folder if it does not exist
            std::fs::create_dir_all(shard_path.as_path()).unwrap();
        }
        let idx = index::Index::open(context).unwrap();
        self.shards
            .insert(shard_id.to_string(), Arc::new(Mutex::new(idx)));

        Ok(())
    }

    pub fn delete_shard(&mut self, shard_id: &str) -> Result<(), String> {
        if !self.exists(shard_id) {
            return Err(format!("Shard {} does not exist", shard_id));
        }

        self.shards.remove(shard_id);

        Ok(())
    }

    pub fn exists(&self, shard_id: &str) -> bool {
        return self.shards.contains_key(shard_id);
    }

    pub fn get_shard(&self, shard_id: &str) -> Result<Arc<Mutex<index::Index>>, String> {
        if !self.exists(shard_id) {
            return Err(format!("Shard {} does not exist", shard_id));
        }

        Ok(self.shards.get(shard_id).unwrap().clone())
    }

    pub fn get_shard_ids(&self) -> impl Iterator<Item = String> + '_ {
        self.shards.keys().cloned().into_iter()
    }
}
