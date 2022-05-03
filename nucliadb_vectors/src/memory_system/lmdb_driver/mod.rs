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

use heed::types::{ByteSlice, Str, Unit};
use heed::{Database, Env, EnvOpenOptions, RoTxn, RwTxn};

use crate::memory_system::elements::*;

const LMDB_ENV: &str = "ENV";
const DB_NODES: &str = "NODES";
const DB_LABELS: &str = "LABELS";
const DB_LAYERS_OUT: &str = "LAYERS_OUT";
const DB_LAYERS_IN: &str = "LAYERS_OUT";
const DB_LOG: &str = "LOG";
const DB_DELETED: &str = "DELETED";
const STAMP: &str = "stamp.nuclia";
const MAP_SIZE: usize = 1048576 * 100000;
const MAX_DBS: u32 = 3000;
#[derive(Copy, Clone)]
pub enum LogField {
    VersionNumber = 0,
    EntryPoint,
}

impl ByteRpr for LogField {
    fn serialize(&self) -> Vec<u8> {
        vec![*self as u8]
    }
    fn deserialize(bytes: &[u8]) -> Self {
        use LogField::*;
        match bytes[0] {
            0 => VersionNumber,
            1 => EntryPoint,
            _ => panic!("Unknown LogField: {bytes:?}"),
        }
    }
}

impl FixedByteLen for LogField {
    fn segment_len() -> usize {
        1
    }
}

pub struct LMBDStorage {
    env: Env,
    // (String, ())
    label_db: Database<Str, Unit>,
    // (String, Node)
    node_db: Database<Str, ByteSlice>,
    // (u64, GraphLayer)
    layer_out_db: Database<ByteSlice, ByteSlice>,
    // (u64, GraphLayer)
    layer_in_db: Database<ByteSlice, ByteSlice>,
    // (LogField, serialized data)
    log: Database<ByteSlice, ByteSlice>,
    // (u128, Vec<Node>)
    deleted_log: Database<ByteSlice, ByteSlice>,
}

impl LMBDStorage {
    pub fn create(path: &Path) -> LMBDStorage {
        let env_path = path.join(LMDB_ENV);
        if !env_path.exists() {
            std::fs::create_dir_all(&env_path).unwrap();
            let mut env_builder = EnvOpenOptions::new();
            env_builder.max_dbs(MAX_DBS);
            env_builder.map_size(MAP_SIZE);
            let env = env_builder.open(&env_path).unwrap();
            let label_db = env.create_database(Some(DB_LABELS)).unwrap();
            let node_db = env.create_database(Some(DB_NODES)).unwrap();
            let layer_out_db = env.create_database(Some(DB_LAYERS_OUT)).unwrap();
            let layer_in_db = env.create_database(Some(DB_LAYERS_IN)).unwrap();
            let log = env.create_database(Some(DB_LOG)).unwrap();
            let deleted_log = env.create_database(Some(DB_DELETED)).unwrap();
            let lmdb = LMBDStorage {
                env,
                label_db,
                node_db,
                layer_out_db,
                layer_in_db,
                deleted_log,
                log,
            };
            let mut w_txn = lmdb.rw_txn();
            for i in 0..hnsw_params::no_layers() {
                lmdb.insert_layer_out(&mut w_txn, i as u64, GraphLayer::new());
                lmdb.insert_layer_in(&mut w_txn, i as u64, GraphLayer::new());
            }
            let log = GraphLog {
                version_number: 0,
                entry_point: None,
            };
            lmdb.insert_log(&mut w_txn, log);
            w_txn.commit().unwrap();
            std::fs::File::create(&path.join(STAMP)).unwrap();
            lmdb
        } else {
            LMBDStorage::open(path)
        }
    }
    pub fn open(path: &Path) -> LMBDStorage {
        let sleep_time = std::time::Duration::from_millis(20);
        let env_path = path.join(LMDB_ENV);
        let stamp_path = path.join(STAMP);
        while !stamp_path.exists() {
            std::thread::sleep(sleep_time);
        }
        let mut env_builder = EnvOpenOptions::new();
        env_builder.max_dbs(MAX_DBS);
        env_builder.map_size(MAP_SIZE);
        let env = env_builder.open(&env_path).unwrap();
        let label_db = env.open_database(Some(DB_LABELS)).unwrap().unwrap();
        let node_db = env.open_database(Some(DB_NODES)).unwrap().unwrap();
        let layer_out_db = env.open_database(Some(DB_LAYERS_OUT)).unwrap().unwrap();
        let layer_in_db = env.open_database(Some(DB_LAYERS_IN)).unwrap().unwrap();
        let log = env.open_database(Some(DB_LOG)).unwrap().unwrap();
        let deleted_log = env.create_database(Some(DB_DELETED)).unwrap();
        LMBDStorage {
            env,
            label_db,
            node_db,
            layer_out_db,
            layer_in_db,
            deleted_log,
            log,
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
        v.map(Node::deserialize)
    }
    pub fn has_label(&self, txn: &RoTxn<'_>, key: &str, label: &str) -> bool {
        let path = format!("{}/{}", key, label);
        let exist = self.label_db.get(txn, path.as_str()).unwrap();
        exist.is_some()
    }
    pub fn add_node(&self, txn: &mut RwTxn<'_, '_>, key: String, node: Node) {
        let node = node.serialize();
        self.node_db.put(txn, key.as_str(), &node).unwrap();
    }
    pub fn add_label(&self, txn: &mut RwTxn<'_, '_>, key: String, label: String) {
        let path = format!("{}/{}", key, label);
        self.label_db.put(txn, path.as_str(), &()).unwrap();
    }
    pub fn remove_vector(&self, txn: &mut RwTxn<'_, '_>, vector: &str) {
        self.node_db.delete(txn, vector).unwrap();
        let mut iter = self.label_db.prefix_iter_mut(txn, vector).unwrap();
        while iter.next().transpose().unwrap().is_some() {
            iter.del_current().unwrap();
        }
    }
    pub fn get_prefixed(&self, txn: &RoTxn, prefix: &str) -> Vec<String> {
        let mut result = vec![];
        let mut iter = self.node_db.prefix_iter(txn, prefix).unwrap();
        while let Some((k, _)) = iter.next().transpose().unwrap() {
            result.push(k.to_string());
        }
        result
    }
    pub fn insert_layer_out(&self, txn: &mut RwTxn<'_, '_>, id: u64, layer: GraphLayer) {
        self.layer_out_db
            .put(txn, &id.serialize(), &layer.serialize())
            .unwrap();
    }
    pub fn insert_layer_in(&self, txn: &mut RwTxn<'_, '_>, id: u64, layer: GraphLayer) {
        self.layer_in_db
            .put(txn, &id.serialize(), &layer.serialize())
            .unwrap();
    }
    pub fn get_layer_out(&self, txn: &RoTxn<'_>, layer: u64) -> Option<GraphLayer> {
        self.layer_out_db
            .get(txn, &layer.serialize())
            .unwrap()
            .map(GraphLayer::deserialize)
    }
    pub fn get_layer_in(&self, txn: &RoTxn<'_>, layer: u64) -> Option<GraphLayer> {
        self.layer_in_db
            .get(txn, &layer.serialize())
            .unwrap()
            .map(GraphLayer::deserialize)
    }
    pub fn insert_log(&self, txn: &mut RwTxn<'_, '_>, log: GraphLog) {
        self.log
            .put(
                txn,
                &LogField::EntryPoint.serialize(),
                &log.entry_point.serialize(),
            )
            .unwrap();
        self.log
            .put(
                txn,
                &LogField::VersionNumber.serialize(),
                &log.version_number.serialize(),
            )
            .unwrap();
    }
    pub fn marked_deleted(&self, txn: &mut RwTxn<'_, '_>, time_stamp: u128, rmv: Vec<Node>) {
        self.deleted_log
            .put(txn, &time_stamp.serialize(), &rmv.serialize())
            .unwrap();
    }
    pub fn clear_deleted(&self, txn: &mut RwTxn<'_, '_>, time_stamp: u128) -> Vec<Node> {
        let delete = self
            .deleted_log
            .get(txn, &time_stamp.serialize())
            .unwrap()
            .map(Vec::deserialize)
            .unwrap_or_default();
        self.deleted_log
            .delete(txn, &time_stamp.serialize())
            .unwrap();
        delete
    }
    pub fn get_log(&self, txn: &RoTxn<'_>) -> GraphLog {
        let version_number = self
            .log
            .get(txn, &LogField::VersionNumber.serialize())
            .unwrap()
            .map(u128::deserialize)
            .unwrap();
        let entry_point = self
            .log
            .get(txn, &LogField::EntryPoint.serialize())
            .unwrap()
            .map(Option::deserialize)
            .unwrap();
        GraphLog {
            version_number,
            entry_point,
        }
    }
}
