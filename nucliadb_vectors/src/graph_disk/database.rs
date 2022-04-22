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

use std::marker::PhantomData;
use std::path::Path;

use heed::flags::Flags;
use heed::types::ByteSlice;
use heed::{Database, Env, EnvOpenOptions};

use crate::graph_disk::db_elems::*;
use crate::trace_utils::*;

const MAP_SIZE: usize = 1048576 * 100000;
const MAX_DBS: u32 = 3000;

#[derive(Clone)]
pub struct InternalDB<K, V> {
    env: Env,
    db: Database<ByteSlice, ByteSlice>,
    key_type: PhantomData<K>,
    value_type: PhantomData<V>,
}
unsafe impl<K, V> Sync for InternalDB<K, V> {}
unsafe impl<K, V> Send for InternalDB<K, V> {}

impl<K, V> InternalDB<K, V>
where
    K: DBElem,
    V: DBElem,
{
    #[named]
    pub fn new(path: &Path, db: &str) -> InternalDB<K, V> {
        entry!();
        let mut env_builder = EnvOpenOptions::new();
        env_builder.max_dbs(MAX_DBS);
        env_builder.map_size(MAP_SIZE);
        unsafe {
            env_builder.flag(Flags::MdbNoLock);
            // env_builder.flag(Flags::MdbNoSync);
        }
        let env = unwrapping!(env_builder.open(path));
        let db = match env.open_database(Some(db)) {
            Err(_) => unwrapping!(env.create_database(Some(db))),
            Ok(Some(db)) => db,
            Ok(None) => unwrapping!(env.create_database(Some(db))),
        };
        exit!(InternalDB {
            env,
            db,
            key_type: PhantomData,
            value_type: PhantomData
        })
    }
    #[named]
    pub fn atomic_insert(&mut self, key: &K, value: &V) {
        entry!();
        let mut w_txn = unwrapping!(self.env.write_txn());
        unwrapping!(self
            .db
            .put(&mut w_txn, &key.serialize(), &value.serialize()));
        unwrapping!(w_txn.commit());
        exit!();
    }
    #[named]
    pub fn atomic_delete(&mut self, key: &K) {
        entry!();
        let mut w_txn = unwrapping!(self.env.write_txn());
        unwrapping!(self.db.delete(&mut w_txn, &key.serialize()));
        unwrapping!(w_txn.commit());
        exit!();
    }
    #[named]
    pub fn get(&self, key: &K) -> Option<V> {
        entry!();
        let r_txn = unwrapping!(self.env.read_txn());
        let raw_result = unwrapping!(self.db.get(&r_txn, &key.serialize()));
        let result = raw_result.map(|v| V::deserialize(v));
        unwrapping!(r_txn.abort());
        exit!(result)
    }

    #[named]
    pub fn get_prefixed(&self, prefix: &K) -> Vec<K> {
        entry!();
        let mut prefixed = Vec::new();
        let r_txn = unwrapping!(self.env.read_txn());
        let iter = unwrapping!(self.db.prefix_iter(&r_txn, &prefix.serialize()));
        for v in iter {
            let (key, _) = unwrapping!(v);
            prefixed.push(K::deserialize(key));
        }
        unwrapping!(r_txn.abort());
        exit!(prefixed)
    }
    #[named]
    pub fn rmv_with_prefix(&self, prefix: &K) -> Vec<(K, V)> {
        entry!();
        let mut removed = Vec::new();
        let mut w_txn = unwrapping!(self.env.write_txn());
        let mut iter = unwrapping!(self.db.prefix_iter_mut(&mut w_txn, &prefix.serialize()));
        while let Some(v) = iter.next() {
            let (key, value) = unwrapping!(v);
            removed.push((K::deserialize(key), V::deserialize(value)));
            unwrapping!(iter.del_current());
        }
        std::mem::drop(iter);
        unwrapping!(w_txn.commit());
        exit!(removed)
    }
    #[named]
    pub fn len(&self) -> usize {
        entry!();
        let r_txn = unwrapping!(self.env.read_txn());
        let result = unwrapping!(self.db.len(&r_txn));
        unwrapping!(r_txn.abort());
        exit!(result as usize)
    }
    #[named]
    pub fn is_empty(&self) -> bool {
        entry!();
        let r_txn = unwrapping!(self.env.read_txn());
        let result = unwrapping!(self.db.len(&r_txn));
        unwrapping!(r_txn.abort());
        exit!(result == 0)
    }
}

#[cfg(test)]
mod test_internals_database {
    use serde::{Deserialize, Serialize};

    use super::*;
    #[derive(Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd, Debug, Copy, Clone)]
    struct DummyK(usize);
    impl DBElem for DummyK {
        fn serialize(&self) -> Vec<u8> {
            bincode::serialize(self).unwrap()
        }
        fn deserialize(bytes: &[u8]) -> Self {
            bincode::deserialize(bytes).unwrap()
        }
    }

    #[derive(Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd, Debug, Copy, Clone)]
    struct DummyV(usize);
    impl DBElem for DummyV {
        fn serialize(&self) -> Vec<u8> {
            bincode::serialize(self).unwrap()
        }
        fn deserialize(bytes: &[u8]) -> Self {
            bincode::deserialize(bytes).unwrap()
        }
    }

    const DUMMY_DB: &str = "DUMMY";
    #[test]
    fn test_internals_database_read_write() {
        let dir = tempfile::tempdir().unwrap();
        let mut database = InternalDB::new(dir.path(), DUMMY_DB);
        database.atomic_insert(&DummyK(0), &DummyV(0));
        let value = database.get(&DummyK(0));
        assert_eq!(value, Some(DummyV(0)));
        database.atomic_delete(&DummyK(0));
        assert_eq!(database.get(&DummyK(0)), None);
    }
    #[test]
    fn test_internals_database_wrong_use() {
        let dir = tempfile::tempdir().unwrap();
        let mut database = InternalDB::new(dir.path(), DUMMY_DB);
        database.atomic_insert(&DummyK(0), &DummyV(0));
        let value = database.get(&DummyK(12));
        database.atomic_insert(&DummyK(12), &DummyV(0));
        assert_eq!(value, None);
    }
    #[test]
    fn test_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let mut database: InternalDB<String, usize> = InternalDB::new(dir.path(), DUMMY_DB);
        database.atomic_insert(&"K_1".to_string(), &0);
        database.atomic_insert(&"K_2".to_string(), &1);
        database.atomic_insert(&"K_3".to_string(), &2);
        let prefixed = database.get_prefixed(&"K".to_string());
        assert_eq!(prefixed.len(), 3);
    }
}
