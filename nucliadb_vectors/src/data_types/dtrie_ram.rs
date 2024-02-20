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

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use std::hash::Hash;

#[derive(Default, Clone, Serialize, Deserialize)]
pub struct DTrie<T> {
    value: Option<T>,
    go_table: HashMap<u8, Box<DTrie<T>>>,
}
impl<T: Ord + Copy> DTrie<T> {
    fn inner_get(&self, key: &[u8], current: Option<T>) -> Option<T> {
        let current = std::cmp::max(current, self.value);
        let [head, tail @ ..] = key else {
            return current;
        };
        let Some(node) = self.go_table.get(head) else {
            return current;
        };
        node.inner_get(tail, current)
    }
    fn inner_prune(&mut self, time: T) -> bool {
        self.value = self.value.filter(|v| *v > time);
        self.go_table = std::mem::take(&mut self.go_table)
            .into_iter()
            .map(|(k, mut v)| (v.inner_prune(time), k, v))
            .filter(|v| !v.0)
            .map(|v| (v.1, v.2))
            .collect();
        self.value.is_none() && self.go_table.is_empty()
    }
    pub fn new() -> Self {
        Self {
            value: None,
            go_table: HashMap::default(),
        }
    }
    pub fn insert(&mut self, key: &[u8], value: T) {
        match key {
            [] => {
                self.value = Some(value);
                self.go_table.clear();
            }
            [head, tail @ ..] => {
                self.go_table.entry(*head).or_insert_with(|| Box::new(DTrie::new())).as_mut().insert(tail, value);
            }
        }
    }
    pub fn get(&self, key: &[u8]) -> Option<T> {
        self.inner_get(key, None)
    }
    pub fn prune(&mut self, time: T) {
        self.inner_prune(time);
    }
}

impl<T: Ord + Copy + Hash> DTrie<T> {
    pub fn convert<U>(&self, mapper: &impl Fn(&T) -> U) -> DTrie<U> {
        let new = self.value.as_ref().map(mapper);
        DTrie {
            value: new,
            go_table: HashMap::from_iter(self.go_table.iter().map(|(k, v)| (*k, Box::new(v.convert(mapper))))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &str = "key";
    const N0: &str = "key_0";
    const N1: &str = "key_1";
    const N2: &str = "key_2";

    #[test]
    fn insert_search() {
        let tplus0 = 0;
        let tplus1 = 1;
        let tplus2 = 2;
        let tplus3 = 3;

        // Time matches the prefix order
        let mut trie = DTrie::new();
        trie.insert(KEY.as_bytes(), tplus0);
        trie.insert(N0.as_bytes(), tplus1);
        trie.insert(N1.as_bytes(), tplus2);
        trie.insert(N2.as_bytes(), tplus3);
        assert_eq!(trie.get(N0.as_bytes()), Some(tplus1));
        assert_eq!(trie.get(N1.as_bytes()), Some(tplus2));
        assert_eq!(trie.get(N2.as_bytes()), Some(tplus3));
        assert_eq!(trie.get(KEY.as_bytes()), Some(tplus0));

        // Prefixes overwrite previous values
        let mut trie = DTrie::new();
        trie.insert(N0.as_bytes(), tplus1);
        trie.insert(KEY.as_bytes(), tplus0);
        trie.insert(N1.as_bytes(), tplus2);
        trie.insert(N2.as_bytes(), tplus3);
        assert_eq!(trie.get(KEY.as_bytes()), Some(tplus0));
        assert_eq!(trie.get(N0.as_bytes()), Some(tplus0));
        assert_eq!(trie.get(N1.as_bytes()), Some(tplus2));
        assert_eq!(trie.get(N2.as_bytes()), Some(tplus3));

        let mut trie = DTrie::new();
        trie.insert(N0.as_bytes(), tplus1);
        trie.insert(N1.as_bytes(), tplus2);
        trie.insert(KEY.as_bytes(), tplus0);
        trie.insert(N2.as_bytes(), tplus3);
        assert_eq!(trie.get(KEY.as_bytes()), Some(tplus0));
        assert_eq!(trie.get(N0.as_bytes()), Some(tplus0));
        assert_eq!(trie.get(N1.as_bytes()), Some(tplus0));
        assert_eq!(trie.get(N2.as_bytes()), Some(tplus3));

        let mut trie = DTrie::new();
        trie.insert(N0.as_bytes(), tplus1);
        trie.insert(N1.as_bytes(), tplus2);
        trie.insert(KEY.as_bytes(), tplus0);
        trie.insert(N2.as_bytes(), tplus0);
        assert_eq!(trie.get(KEY.as_bytes()), Some(tplus0));
        assert_eq!(trie.get(N0.as_bytes()), Some(tplus0));
        assert_eq!(trie.get(N1.as_bytes()), Some(tplus0));
        assert_eq!(trie.get(N2.as_bytes()), Some(tplus0));
    }
    #[test]
    fn prune() {
        let tplus0 = 0;
        let tplus1 = 1;
        let tplus2 = 2;
        let tplus3 = 3;

        let mut trie = DTrie::new();
        trie.insert(KEY.as_bytes(), tplus0);
        trie.insert(N0.as_bytes(), tplus1);
        trie.insert(N1.as_bytes(), tplus2);
        trie.insert(N2.as_bytes(), tplus3);
        trie.prune(tplus0);
        assert_eq!(trie.get(N2.as_bytes()), Some(tplus3));
        assert_eq!(trie.get(N1.as_bytes()), Some(tplus2));
        assert_eq!(trie.get(N0.as_bytes()), Some(tplus1));
        assert_eq!(trie.get(KEY.as_bytes()), None);

        let mut trie = DTrie::new();
        trie.insert(KEY.as_bytes(), tplus0);
        trie.insert(N0.as_bytes(), tplus1);
        trie.insert(N1.as_bytes(), tplus2);
        trie.insert(N2.as_bytes(), tplus3);
        trie.prune(tplus1);
        assert_eq!(trie.get(N2.as_bytes()), Some(tplus3));
        assert_eq!(trie.get(N1.as_bytes()), Some(tplus2));
        assert_eq!(trie.get(N0.as_bytes()), None);
        assert_eq!(trie.get(KEY.as_bytes()), None);

        let mut trie = DTrie::new();
        trie.insert(KEY.as_bytes(), tplus0);
        trie.insert(N0.as_bytes(), tplus1);
        trie.insert(N1.as_bytes(), tplus2);
        trie.insert(N2.as_bytes(), tplus3);
        trie.prune(tplus2);
        assert_eq!(trie.get(N2.as_bytes()), Some(tplus3));
        assert_eq!(trie.get(N1.as_bytes()), None);
        assert_eq!(trie.get(N0.as_bytes()), None);
        assert_eq!(trie.get(KEY.as_bytes()), None);

        let mut trie = DTrie::new();
        trie.insert(KEY.as_bytes(), tplus0);
        trie.insert(N0.as_bytes(), tplus1);
        trie.insert(N1.as_bytes(), tplus2);
        trie.insert(N2.as_bytes(), tplus3);
        trie.prune(tplus3);
        assert_eq!(trie.get(N2.as_bytes()), None);
        assert_eq!(trie.get(N1.as_bytes()), None);
        assert_eq!(trie.get(N0.as_bytes()), None);
        assert_eq!(trie.get(KEY.as_bytes()), None);
    }
}
