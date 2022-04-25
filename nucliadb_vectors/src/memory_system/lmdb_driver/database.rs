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

use crate::memory_system::elements::ByteRpr;

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
    K: ByteRpr,
    V: ByteRpr,
{
    pub fn new(path: &Path, db: &str) -> InternalDB<K, V> {
        let mut env_builder = EnvOpenOptions::new();
        env_builder.max_dbs(MAX_DBS);
        env_builder.map_size(MAP_SIZE);
        unsafe {
            env_builder.flag(Flags::MdbNoLock);
            env_builder.flag(Flags::MdbNoSync);
        }
        let env = env_builder.open(path).unwrap();
        let db = match env.open_database(Some(db)) {
            Err(_) => env.create_database(Some(db)).unwrap(),
            Ok(Some(db)) => db,
            Ok(None) => env.create_database(Some(db)).unwrap(),
        };
        InternalDB {
            env,
            db,
            key_type: PhantomData,
            value_type: PhantomData,
        }
    }
    pub fn atomic_insert(&mut self, key: &K, value: &V) {
        let mut w_txn = self.env.write_txn().unwrap();
        let key = key.serialize();
        let value = value.serialize();
        self.db.put(&mut w_txn, &key, &value).unwrap();
        w_txn.commit().unwrap();
    }
    pub fn atomic_delete(&mut self, key: &K) {
        let mut w_txn = self.env.write_txn().unwrap();
        self.db.delete(&mut w_txn, &key.serialize()).unwrap();
        w_txn.commit().unwrap();
    }
    pub fn get(&self, key: &K) -> Option<V> {
        let r_txn = self.env.read_txn().unwrap();
        let raw_result = self.db.get(&r_txn, &key.serialize()).unwrap();
        let result = raw_result.map(|v| V::deserialize(v));
        r_txn.abort().unwrap();
        result
    }
    pub fn get_prefixed(&self, prefix: &K) -> Vec<(K, V)> {
        let mut prefixed = Vec::new();
        let r_txn = self.env.read_txn().unwrap();
        let iter = self.db.prefix_iter(&r_txn, &prefix.serialize()).unwrap();
        for v in iter {
            let (key, value) = v.unwrap();
            let key = K::deserialize(key);
            let value = V::deserialize(value);
            prefixed.push((key, value));
        }
        r_txn.abort().unwrap();
        prefixed
    }
    pub fn rmv_with_prefix(&self, prefix: &K) -> Vec<(K, V)> {
        let mut removed = Vec::new();
        let mut w_txn = self.env.write_txn().unwrap();
        let mut iter = self
            .db
            .prefix_iter_mut(&mut w_txn, &prefix.serialize())
            .unwrap();
        while let Some(v) = iter.next() {
            let (key, value) = v.unwrap();
            removed.push((K::deserialize(key), V::deserialize(value)));
            iter.del_current().unwrap();
        }
        std::mem::drop(iter);
        w_txn.commit().unwrap();
        removed
    }
    pub fn len(&self) -> usize {
        let r_txn = self.env.read_txn().unwrap();
        let result = self.db.len(&r_txn).unwrap();
        r_txn.abort().unwrap();
        result as usize
    }
}

#[cfg(test)]
mod test_internals_database {
    use super::*;
    const DUMMY_DB: &str = "DUMMY";
    #[test]
    fn test_internals_database_read_write() {
        let dir = tempfile::tempdir().unwrap();
        let mut database = InternalDB::new(dir.path(), DUMMY_DB);
        database.atomic_insert(&0u64, &0u64);
        let value = database.get(&0u64);
        assert_eq!(value, Some(0u64));
        database.atomic_delete(&0u64);
        assert_eq!(database.get(&0u64), None);
    }
    #[test]
    fn test_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let mut database: InternalDB<String, u64> = InternalDB::new(dir.path(), DUMMY_DB);
        database.atomic_insert(&"K_1".to_string(), &0);
        database.atomic_insert(&"K_2".to_string(), &1);
        database.atomic_insert(&"K_3".to_string(), &2);
        let prefixed = database.get_prefixed(&"K".to_string());
        assert_eq!(prefixed.len(), 3);
    }
}
