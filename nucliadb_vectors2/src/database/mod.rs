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

use crate::index::Location;
use heed::flags::Flags;
use heed::types::{SerdeBincode, Str, Unit};
use heed::{Database, Env, EnvOpenOptions};
pub use heed::{Error as DBErr, Result as DBResult, RoIter, RoPrefix, RoTxn, RwTxn};

mod db_names {
    pub const DB_NODES: &str = "NODES";
    pub const DB_NODES_INV: &str = "NODES_INV";
    pub const DB_LABELS: &str = "LABELS";
}

mod env_params {
    pub const MAP_SIZE: usize = 1048576 * 100000;
    pub const MAX_DBS: u32 = 3;
}

pub struct VectorDB {
    env: Env,
    // (String, ())
    label_db: Database<Str, Unit>,
    // (String, Location)
    node_db: Database<Str, SerdeBincode<Location>>,
    // (Location, String)
    node_inv_db: Database<SerdeBincode<Location>, Str>,
}

impl VectorDB {
    pub fn new<P: AsRef<Path>>(env_path: P) -> DBResult<VectorDB> {
        let mut env_builder = EnvOpenOptions::new();
        env_builder.max_dbs(env_params::MAX_DBS);
        env_builder.map_size(env_params::MAP_SIZE);
        unsafe {
            env_builder.flag(Flags::MdbNoLock);
        }
        let env = env_builder.open(&env_path)?;
        let label_db = env.create_database(Some(db_names::DB_LABELS))?;
        let node_db = env.create_database(Some(db_names::DB_NODES))?;
        let node_inv_db = env.create_database(Some(db_names::DB_NODES_INV))?;
        Ok(VectorDB {
            env,
            label_db,
            node_db,
            node_inv_db,
        })
    }

    // Transactions

    /// Return a new read-only transaction that can be used to perform
    /// read operation on the database.
    pub fn ro_txn(&self) -> DBResult<RoTxn> {
        self.env.read_txn()
    }

    /// Return a new read-write transaction that can be used to
    /// perform write operations on the database.
    pub fn rw_txn(&self) -> DBResult<RwTxn> {
        self.env.write_txn()
    }

    // Operations

    pub fn get_node(&self, txn: &RoTxn, vector: &str) -> DBResult<Option<Location>> {
        self.node_db.get(txn, vector)
    }

    pub fn get_node_key<'a>(&self, txn: &'a RoTxn, node: Location) -> DBResult<Option<&'a str>> {
        self.node_inv_db.get(txn, &node)
    }

    pub fn add_node(&self, txn: &mut RwTxn, key: &str, node: Location) -> DBResult<()> {
        self.node_db.put(txn, key, &node)?;
        self.node_inv_db.put(txn, &node, key)?;
        Ok(())
    }

    pub fn rmv_node(&self, txn: &mut RwTxn, key: &str) -> DBResult<()> {
        if let Some(node) = self.node_db.get(txn, key)? {
            self.node_db.delete(txn, key)?;
            self.node_inv_db.delete(txn, &node)?;
            let labels_prefix = self.labels_prefix(key);
            let mut iter = self.label_db.prefix_iter_mut(txn, &labels_prefix)?;
            while (iter.next().transpose()?).is_some() {
                iter.del_current()?;
            }
        }
        Ok(())
    }

    pub fn add_label(&self, txn: &mut RwTxn, key: &str, label: &str) -> DBResult<()> {
        let path = self.label_entry(key, label);
        self.label_db.put(txn, path.as_str(), &())
    }

    pub fn has_label(&self, txn: &RoTxn, key: &str, label: &str) -> DBResult<bool> {
        let path = self.label_entry(key, label);
        self.label_db.get(txn, path.as_str()).map(|v| v.is_some())
    }

    pub fn get_keys<'a>(&'a self, txn: &'a RoTxn) -> DBResult<RoIter<Str, SerdeBincode<Location>>> {
        self.node_db.iter(txn)
    }

    pub fn nodes_prefixed_with<'a>(
        &'a self,
        txn: &'a RoTxn,
        prefix: &str,
    ) -> DBResult<RoPrefix<Str, SerdeBincode<Location>>> {
        self.node_db.prefix_iter(txn, prefix)
    }

    fn labels_prefix(&self, key: &str) -> String {
        format!("[{key}/]")
    }

    fn label_entry(&self, key: &str, label: &str) -> String {
        format!("[{key}/{label}]")
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use tempfile::tempdir;

    use crate::index::SegmentSlice;

    use super::*;

    #[test]
    fn test_vectordb_nodes() {
        let dir = tempdir().unwrap();
        let vectordb = VectorDB::new(dir.path()).unwrap();

        let node_1 = Location {
            txn_id: 1,
            slice: SegmentSlice { start: 0, end: 99 },
        };

        let node_2 = Location {
            txn_id: 2,
            slice: SegmentSlice {
                start: 100,
                end: 199,
            },
        };

        let unsaved_node = Location {
            txn_id: 3,
            slice: SegmentSlice {
                start: 200,
                end: 299,
            },
        };

        {
            let mut rw_txn = vectordb.rw_txn().unwrap();
            vectordb.add_node(&mut rw_txn, "node_1", node_1).unwrap();
            vectordb.add_node(&mut rw_txn, "node_2", node_2).unwrap();
            rw_txn.commit().unwrap();
        }

        let ro_txn = vectordb.ro_txn().unwrap();
        assert_eq!(
            node_1,
            vectordb.get_node(&ro_txn, "node_1").unwrap().unwrap()
        );
        assert_eq!(
            node_2,
            vectordb.get_node(&ro_txn, "node_2").unwrap().unwrap()
        );
        assert!(vectordb.get_node(&ro_txn, "inexistent").unwrap().is_none());

        assert_eq!(
            "node_1",
            vectordb.get_node_key(&ro_txn, node_1).unwrap().unwrap()
        );
        assert!(vectordb
            .get_node_key(&ro_txn, unsaved_node)
            .unwrap()
            .is_none());

        let keys = vectordb
            .get_keys(&ro_txn)
            .unwrap()
            .map(|item| {
                let (key, _) = item.unwrap();
                key
            })
            .collect::<HashSet<&str>>();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains("node_1"));
        assert!(keys.contains("node_2"));
        assert!(!keys.contains("inexistent"));

        {
            let mut rw_txn = vectordb.rw_txn().unwrap();
            vectordb.rmv_node(&mut rw_txn, "node_1").unwrap();
            rw_txn.commit().unwrap();
        }

        // As LMDB uses MVCC, the open read-only transaction will
        // continue returning the deleted value
        assert!(vectordb.get_node(&ro_txn, "node_1").unwrap().is_some());
        // Starting a new transaction will give the updated view
        drop(ro_txn);
        let ro_txn = vectordb.ro_txn().unwrap();
        assert!(vectordb.get_node(&ro_txn, "node_1").unwrap().is_none());
    }

    #[test]
    fn test_vectordb_labels() {
        let dir = tempdir().unwrap();
        let vectordb = VectorDB::new(dir.path()).unwrap();

        let node = Location {
            txn_id: 1,
            slice: SegmentSlice { start: 0, end: 100 },
        };

        let key = "key_1";
        let label = "label_1";

        {
            let mut rw_txn = vectordb.rw_txn().unwrap();
            vectordb.add_node(&mut rw_txn, "node", node).unwrap();
            rw_txn.commit().unwrap();
        }

        {
            let ro_txn = vectordb.ro_txn().unwrap();
            assert!(!vectordb.has_label(&ro_txn, key, label).unwrap());
        }

        {
            let mut rw_txn = vectordb.rw_txn().unwrap();
            vectordb.add_label(&mut rw_txn, key, label).unwrap();
            rw_txn.commit().unwrap();
        }

        {
            let ro_txn = vectordb.ro_txn().unwrap();
            assert!(vectordb.has_label(&ro_txn, key, label).unwrap());
        }
    }

    #[test]
    fn test_vectordb_iter_by_node_prefix() {
        let dir = tempdir().unwrap();
        let vectordb = VectorDB::new(dir.path()).unwrap();

        let node_a1 = Location {
            txn_id: 1,
            slice: SegmentSlice { start: 0, end: 49 },
        };

        let node_a2 = Location {
            txn_id: 2,
            slice: SegmentSlice {
                start: 50,
                end: 149,
            },
        };

        let node_b = Location {
            txn_id: 3,
            slice: SegmentSlice {
                start: 150,
                end: 300,
            },
        };

        let mut rw_txn = vectordb.rw_txn().unwrap();
        vectordb.add_node(&mut rw_txn, "node_A1", node_a1).unwrap();
        vectordb.add_node(&mut rw_txn, "node_A2", node_a2).unwrap();
        vectordb.add_node(&mut rw_txn, "node_B1", node_b).unwrap();
        rw_txn.commit().unwrap();

        let ro_txn = vectordb.ro_txn().unwrap();
        assert_eq!(
            vectordb
                .nodes_prefixed_with(&ro_txn, "node")
                .unwrap()
                .count(),
            3
        );
        assert_eq!(
            vectordb
                .nodes_prefixed_with(&ro_txn, "node_A")
                .unwrap()
                .count(),
            2
        );
        assert_eq!(
            vectordb
                .nodes_prefixed_with(&ro_txn, "node_B")
                .unwrap()
                .count(),
            1
        );
        assert_eq!(
            vectordb
                .nodes_prefixed_with(&ro_txn, "abc")
                .unwrap()
                .count(),
            0
        );
    }
}
