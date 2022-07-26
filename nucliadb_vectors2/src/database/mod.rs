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
    pub fn ro_txn(&self) -> DBResult<RoTxn> {
        self.env.read_txn()
    }
    pub fn rw_txn(&self) -> DBResult<RwTxn> {
        self.env.write_txn()
    }
    pub fn get_node(&self, txn: &RoTxn, vector: &str) -> DBResult<Option<Location>> {
        self.node_db.get(txn, vector)
    }
    pub fn get_node_key<'a>(&self, txn: &'a RoTxn, node: Location) -> DBResult<Option<&'a str>> {
        self.node_inv_db.get(txn, &node)
    }
    pub fn has_label(&self, txn: &RoTxn, key: &str, label: &str) -> DBResult<bool> {
        let path = self.label_entry(key, label);
        self.label_db.get(txn, path.as_str()).map(|v| v.is_some())
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
            while let Some(_) = iter.next().transpose()? {
                iter.del_current()?;
            }
        }
        Ok(())
    }
    pub fn add_label(&self, txn: &mut RwTxn, key: &str, label: &str) -> DBResult<()> {
        let path = self.label_entry(key, label);
        self.label_db.put(txn, path.as_str(), &())
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
