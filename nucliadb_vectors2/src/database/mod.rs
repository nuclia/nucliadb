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
use heed::types::{SerdeBincode, Str, Unit};
use heed::{Database, Env, EnvOpenOptions};
pub use heed::{Error as DBErr, Result as DBResult, RoIter, RoPrefix, RoTxn, RwTxn};

use crate::index::Address;

mod db_names {
    pub const DB_ADDRESS: &str = "ADDRESS";
    pub const DB_ADDRESS_INV: &str = "ADDRESS_INV";
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
    // (String, Address)
    address_db: Database<Str, SerdeBincode<Address>>,
    // (Address, String)
    address_inv_db: Database<SerdeBincode<Address>, Str>,
}

impl VectorDB {
    fn labels_prefix(&self, key: &str) -> String {
        format!("[{key}/]")
    }
    fn label_entry(&self, key: &str, label: &str) -> String {
        format!("[{key}/{label}]")
    }
    fn add_label(
        &self,
        txn: &mut RwTxn,
        key: impl AsRef<str>,
        label: impl AsRef<str>,
    ) -> DBResult<()> {
        let path = self.label_entry(key.as_ref(), label.as_ref());
        self.label_db.put(txn, path.as_str(), &())?;
        Ok(())
    }
    pub fn new<P: AsRef<Path>>(env_path: P) -> DBResult<VectorDB> {
        let mut env_builder = EnvOpenOptions::new();
        env_builder.max_dbs(env_params::MAX_DBS);
        env_builder.map_size(env_params::MAP_SIZE);
        unsafe {
            env_builder.flag(Flags::MdbNoLock);
        }
        let env = env_builder.open(&env_path)?;
        let label_db = env.create_database(Some(db_names::DB_LABELS))?;
        let address_db = env.create_database(Some(db_names::DB_ADDRESS))?;
        let address_inv_db = env.create_database(Some(db_names::DB_ADDRESS_INV))?;
        Ok(VectorDB {
            env,
            label_db,
            address_db,
            address_inv_db,
        })
    }
    pub fn ro_txn(&self) -> DBResult<RoTxn> {
        self.env.read_txn()
    }
    pub fn rw_txn(&self) -> DBResult<RwTxn> {
        self.env.write_txn()
    }
    pub fn get_address(&self, txn: &RoTxn, key: impl AsRef<str>) -> DBResult<Option<Address>> {
        self.address_db.get(txn, key.as_ref())
    }
    pub fn get_address_key<'a>(
        &self,
        txn: &'a RoTxn,
        address: Address,
    ) -> DBResult<Option<&'a str>> {
        self.address_inv_db.get(txn, &address)
    }
    pub fn has_label(
        &self,
        txn: &RoTxn,
        key: impl AsRef<str>,
        label: impl AsRef<str>,
    ) -> DBResult<bool> {
        let path = self.label_entry(key.as_ref(), label.as_ref());
        self.label_db.get(txn, path.as_str()).map(|v| v.is_some())
    }
    pub fn add_address(
        &self,
        txn: &mut RwTxn,
        key: impl AsRef<str>,
        address: Address,
        labels: &[impl AsRef<str>],
    ) -> DBResult<()> {
        let key_exists = self.get_address(txn, key.as_ref())?.is_some();
        let addr_exists = self.get_address_key(txn, address)?.is_some();
        if !key_exists && !addr_exists {
            self.address_db.put(txn, key.as_ref(), &address)?;
            self.address_inv_db.put(txn, &address, key.as_ref())?;
            labels
                .iter()
                .try_for_each(|label| self.add_label(txn, key.as_ref(), label.as_ref()))?;
        }
        Ok(())
    }
    pub fn rmv_address(&self, txn: &mut RwTxn, key: impl AsRef<str>) -> DBResult<()> {
        if let Some(address) = self.address_db.get(txn, key.as_ref())? {
            self.address_db.delete(txn, key.as_ref())?;
            self.address_inv_db.delete(txn, &address)?;
            let labels_prefix = self.labels_prefix(key.as_ref());
            let mut iter = self.label_db.prefix_iter_mut(txn, &labels_prefix)?;
            while (iter.next().transpose()?).is_some() {
                iter.del_current()?;
            }
        }
        Ok(())
    }
    pub fn get_keys<'a>(&'a self, txn: &'a RoTxn) -> DBResult<RoIter<Str, SerdeBincode<Address>>> {
        self.address_db.iter(txn)
    }
    pub fn addresses_prefixed_with<'a>(
        &'a self,
        txn: &'a RoTxn,
        prefix: impl AsRef<str>,
    ) -> DBResult<RoPrefix<Str, SerdeBincode<Address>>> {
        self.address_db.prefix_iter(txn, prefix.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use tempfile::tempdir;

    use super::*;
    use crate::index::SegmentSlice;

    const NO_LABELS: &[&str] = &[];
    #[test]
    fn test_vectordb_nodes() {
        let dir = tempdir().unwrap();
        let vectordb = VectorDB::new(dir.path()).unwrap();

        let node_1 = Address {
            txn_id: 1,
            slice: SegmentSlice { start: 0, end: 99 },
        };

        let node_2 = Address {
            txn_id: 2,
            slice: SegmentSlice {
                start: 100,
                end: 199,
            },
        };

        let unsaved_node = Address {
            txn_id: 3,
            slice: SegmentSlice {
                start: 200,
                end: 299,
            },
        };
        let mut rw_txn = vectordb.rw_txn().unwrap();
        vectordb
            .add_address(&mut rw_txn, "node_1", node_1, NO_LABELS)
            .unwrap();
        vectordb
            .add_address(&mut rw_txn, "node_2", node_2, NO_LABELS)
            .unwrap();
        rw_txn.commit().unwrap();

        let ro_txn = vectordb.ro_txn().unwrap();
        assert_eq!(
            node_1,
            vectordb.get_address(&ro_txn, "node_1").unwrap().unwrap()
        );
        assert_eq!(
            node_2,
            vectordb.get_address(&ro_txn, "node_2").unwrap().unwrap()
        );
        assert!(vectordb
            .get_address(&ro_txn, "inexistent")
            .unwrap()
            .is_none());

        assert_eq!(
            "node_1",
            vectordb.get_address_key(&ro_txn, node_1).unwrap().unwrap()
        );
        assert!(vectordb
            .get_address_key(&ro_txn, unsaved_node)
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

        let mut rw_txn = vectordb.rw_txn().unwrap();
        vectordb.rmv_address(&mut rw_txn, "node_1").unwrap();
        rw_txn.commit().unwrap();

        // As LMDB uses MVCC, the open read-only transaction will
        // continue returning the deleted value
        assert!(vectordb.get_address(&ro_txn, "node_1").unwrap().is_some());
        // Starting a new transaction will give the updated view
        drop(ro_txn);
        let ro_txn = vectordb.ro_txn().unwrap();
        assert!(vectordb.get_address(&ro_txn, "node_1").unwrap().is_none());
    }

    #[test]
    fn test_vectordb_labels() {
        let dir = tempdir().unwrap();
        let vectordb = VectorDB::new(dir.path()).unwrap();

        let addr1 = Address {
            txn_id: 1,
            slice: SegmentSlice { start: 0, end: 100 },
        };
        let addr2 = Address {
            txn_id: 2,
            slice: SegmentSlice { start: 0, end: 100 },
        };
        let key1 = "key_1";
        let key2 = "key_2";
        let label = "label_1";

        let mut rw_txn = vectordb.rw_txn().unwrap();
        vectordb
            .add_address(&mut rw_txn, key1, addr1, NO_LABELS)
            .unwrap();
        vectordb
            .add_address(&mut rw_txn, key2, addr2, &[label])
            .unwrap();
        rw_txn.commit().unwrap();

        let ro_txn = vectordb.ro_txn().unwrap();
        assert!(!vectordb.has_label(&ro_txn, key1, label).unwrap());
        assert!(vectordb.has_label(&ro_txn, key2, label).unwrap());
    }

    #[test]
    fn test_vectordb_iter_by_node_prefix() {
        let dir = tempdir().unwrap();
        let vectordb = VectorDB::new(dir.path()).unwrap();

        let node_a1 = Address {
            txn_id: 1,
            slice: SegmentSlice { start: 0, end: 49 },
        };

        let node_a2 = Address {
            txn_id: 2,
            slice: SegmentSlice {
                start: 50,
                end: 149,
            },
        };

        let node_b = Address {
            txn_id: 3,
            slice: SegmentSlice {
                start: 150,
                end: 300,
            },
        };

        let mut rw_txn = vectordb.rw_txn().unwrap();
        vectordb
            .add_address(&mut rw_txn, "node_A1", node_a1, NO_LABELS)
            .unwrap();
        vectordb
            .add_address(&mut rw_txn, "node_A2", node_a2, NO_LABELS)
            .unwrap();
        vectordb
            .add_address(&mut rw_txn, "node_B1", node_b, NO_LABELS)
            .unwrap();
        rw_txn.commit().unwrap();

        let ro_txn = vectordb.ro_txn().unwrap();
        assert_eq!(
            vectordb
                .addresses_prefixed_with(&ro_txn, "node")
                .unwrap()
                .count(),
            3
        );
        assert_eq!(
            vectordb
                .addresses_prefixed_with(&ro_txn, "node_A")
                .unwrap()
                .count(),
            2
        );
        assert_eq!(
            vectordb
                .addresses_prefixed_with(&ro_txn, "node_B")
                .unwrap()
                .count(),
            1
        );
        assert_eq!(
            vectordb
                .addresses_prefixed_with(&ro_txn, "abc")
                .unwrap()
                .count(),
            0
        );
    }
}
