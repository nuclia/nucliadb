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

use crate::hnsw::*;
use heed::flags::Flags;
use heed::types::{SerdeBincode, Str, Unit};
use heed::{Database, Env, EnvOpenOptions, RoIter, RoPrefix, RoTxn, RwTxn};
use std::path::Path;

mod db_names {
    pub const DB_NODES: &str = "NODES_lmdb";
    pub const DB_NODES_INV: &str = "NODES_INV_lmdb";
    pub const DB_LABELS: &str = "LABELS_lmdb";
}

mod env_params {
    pub const LMDB_ENV: &str = "ENV_lmdb";
    pub const MAP_SIZE: usize = 1048576 * 100000;
    pub const MAX_DBS: u32 = 3000;
}

pub struct LMBDStorage {
    env: Env,
    // (String, ())
    label_db: Database<Str, Unit>,
    // (String, Node)
    node_db: Database<Str, SerdeBincode<Node>>,
    // (Node, String)
    node_inv_db: Database<SerdeBincode<Node>, Str>,
}

impl LMBDStorage {
    pub fn create(path: &Path) -> LMBDStorage {
        let env_path = path.join(env_params::LMDB_ENV);
        let mut env_builder = EnvOpenOptions::new();
        env_builder.max_dbs(env_params::MAX_DBS);
        env_builder.map_size(env_params::MAP_SIZE);
        unsafe {
            env_builder.flag(Flags::MdbNoLock);
        }
        let env = env_builder.open(&env_path).unwrap();
        let label_db = env.create_database(Some(db_names::DB_LABELS)).unwrap();
        let node_db = env.create_database(Some(db_names::DB_NODES)).unwrap();
        let node_inv_db = env.create_database(Some(db_names::DB_NODES_INV)).unwrap();
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
        env_builder.max_dbs(env_params::MAX_DBS);
        env_builder.map_size(env_params::MAP_SIZE);
        let env_path = path.join(env_params::LMDB_ENV);
        let env = env_builder.open(&env_path).unwrap();
        let label_db = env.open_database(Some(db_names::DB_LABELS)).unwrap().unwrap();
        let node_db = env.open_database(Some(db_names::DB_NODES)).unwrap().unwrap();
        let node_inv_db = env.open_database(Some(db_names::DB_NODES_INV)).unwrap().unwrap();
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
        self.node_db.get(txn, vector).unwrap()
    }
    pub fn get_node_key<'a>(&self, txn: &'a RoTxn, node: Node) -> Option<&'a str> {
        self.node_inv_db.get(txn, &node).unwrap()
    }
    pub fn has_label(&self, txn: &RoTxn<'_>, key: &str, label: &str) -> bool {
        let path = format!("[{}/{}]", key, label);
        let exist = self.label_db.get(txn, path.as_str()).unwrap();
        exist.is_some()
    }
    pub fn add_node(&self, txn: &mut RwTxn<'_, '_>, key: &str, node: Node) {
        self.node_db.put(txn, key, &node).unwrap();
        self.node_inv_db.put(txn, &node, key).unwrap();
    }
    pub fn add_label(&self, txn: &mut RwTxn<'_, '_>, key: &str, label: &str) {
        let path = format!("[{}/{}]", key, label);
        self.label_db.put(txn, path.as_str(), &()).unwrap();
    }
    pub fn get_keys<'a>(&'a self, txn: &'a RoTxn) -> RoIter<Str, SerdeBincode<Node>> {
        self.node_db.iter(txn).unwrap()
    }
    pub fn get_prefixed<'a>(
        &'a self,
        txn: &'a RoTxn,
        prefix: &str,
    ) -> RoPrefix<Str, SerdeBincode<Node>> {
        self.node_db.prefix_iter(txn, prefix).unwrap()
    }
}
