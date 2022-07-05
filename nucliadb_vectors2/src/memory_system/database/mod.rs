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

use std::path::Path;

use heed::flags::Flags;
use heed::types::{ByteSlice, Str, Unit};
use heed::{Database, Env, EnvOpenOptions, RoTxn, RwTxn};

use crate::memory_system::elements::*;

const LMDB_ENV: &str = "ENV_lmdb";
const DB_NODES: &str = "NODES_lmdb";
const DB_NODES_INV: &str = "NODES_INV_lmdb";
const DB_LABELS: &str = "LABELS_lmdb";
const MAP_SIZE: usize = 1048576 * 100000;
const MAX_DBS: u32 = 3000;

pub struct LMBDStorage {
    env: Env,
    // (String, ())
    label_db: Database<Str, Unit>,
    // (String, Node)
    node_db: Database<Str, ByteSlice>,
    // (Node, String)
    node_inv_db: Database<ByteSlice, Str>,
}

impl LMBDStorage {
    pub fn create(path: &Path) -> LMBDStorage {
        let env_path = path.join(LMDB_ENV);
        let mut env_builder = EnvOpenOptions::new();
        env_builder.max_dbs(MAX_DBS);
        env_builder.map_size(MAP_SIZE);
        unsafe {
            env_builder.flag(Flags::MdbNoLock);
        }
        let env = env_builder.open(&env_path).unwrap();
        let label_db = env.create_database(Some(DB_LABELS)).unwrap();
        let node_db = env.create_database(Some(DB_NODES)).unwrap();
        let node_inv_db = env.create_database(Some(DB_NODES_INV)).unwrap();
        LMBDStorage {
            env,
            label_db,
            node_db,
            node_inv_db,
        }
    }
    pub fn open(path: &Path) -> LMBDStorage {
        let mut env_builder = EnvOpenOptions::new();
        unsafe {
            env_builder.flag(Flags::MdbNoLock);
        }
        env_builder.max_dbs(MAX_DBS);
        env_builder.map_size(MAP_SIZE);
        let env_path = path.join(LMDB_ENV);
        let env = env_builder.open(&env_path).unwrap();
        let label_db = env.open_database(Some(DB_LABELS)).unwrap().unwrap();
        let node_db = env.open_database(Some(DB_NODES)).unwrap().unwrap();
        let node_inv_db = env.open_database(Some(DB_NODES_INV)).unwrap().unwrap();
        LMBDStorage {
            env,
            label_db,
            node_db,
            node_inv_db,
        }
    }
    pub fn ro_txn(&self) -> RoTxn<'_> {
        self.env.read_txn().unwrap()
    }
    pub fn rw_txn(&self) -> RwTxn<'_, '_> {
        self.env.write_txn().unwrap()
    }
    pub fn get_node(&self, txn: &RoTxn<'_>, vector: &str) -> Option<Node> {
        let v = self.node_db.get(txn, vector).unwrap();
        v.map(bincode::deserialize).transpose().unwrap()
    }
    pub fn get_node_key<'a>(&self, txn: &'a RoTxn, node: Node) -> Option<&'a str> {
        self.node_inv_db
            .get(txn, &bincode::serialize(&node).unwrap())
            .unwrap()
    }
    pub fn get_keys(&self, txn: &RoTxn) -> Vec<String> {
        let mut result = vec![];
        let mut it = self.node_db.iter(txn).unwrap();
        while let Some((key, _)) = it.next().transpose().unwrap() {
            result.push(key.to_string());
        }
        result
    }
    pub fn has_label(&self, txn: &RoTxn<'_>, key: &str, label: &str) -> bool {
        let path = format!("{}/{}", key, label);
        let exist = self.label_db.get(txn, path.as_str()).unwrap();
        exist.is_some()
    }
    pub fn add_node(&self, txn: &mut RwTxn<'_, '_>, key: &str, node: Node) {
        let node = bincode::serialize(&node).unwrap();
        self.node_db.put(txn, key, &node).unwrap();
        self.node_inv_db.put(txn, &node, key).unwrap();
    }
    pub fn add_label(&self, txn: &mut RwTxn<'_, '_>, key: &str, label: &str) {
        let path = format!("{}/{}", key, label);
        self.label_db.put(txn, path.as_str(), &()).unwrap();
    }
    pub fn get_prefixed(&self, txn: &RoTxn, prefix: &str) -> Vec<String> {
        let mut result = vec![];
        let mut iter = self.node_db.prefix_iter(txn, prefix).unwrap();
        while let Some((k, _)) = iter.next().transpose().unwrap() {
            result.push(k.to_string());
        }
        result
    }
}
