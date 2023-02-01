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

#[derive(Clone, Serialize, Deserialize)]
pub struct DTrie<Prop> {
    value: Option<Prop>,
    go_table: HashMap<u8, Box<DTrie<Prop>>>,
}

impl<Prop> Default for DTrie<Prop> {
    fn default() -> Self {
        DTrie::new()
    }
}
impl<Prop> DTrie<Prop> {
    fn inner_delete(&mut self, key: &[u8]) -> bool {
        match key {
            [] => {
                self.value = None;
                self.go_table.is_empty()
            }
            [head, tail @ ..] => self
                .go_table
                .get_mut(head)
                .map(|node| node.inner_delete(tail))
                .filter(|removed| *removed)
                .map(|_| self.go_table.remove(head))
                .map(|_| self.go_table.is_empty() && self.value.is_none())
                .unwrap_or_default(),
        }
    }
    fn is_value(&self) -> bool {
        self.value.is_some()
    }
    pub fn new() -> DTrie<Prop> {
        DTrie {
            value: None,
            go_table: HashMap::new(),
        }
    }
    pub fn insert(&mut self, key: &[u8], value: Prop) {
        match key {
            [] => self.value = Some(value),
            [head, tail @ ..] => {
                self.go_table
                    .entry(*head)
                    .or_insert_with(|| Box::new(DTrie::new()))
                    .as_mut()
                    .insert(tail, value);
            }
        }
    }
    pub fn get(&self, key: &[u8]) -> Option<&Prop> {
        match key {
            [] => self.value.as_ref(),
            [head, tail @ ..] => self
                .go_table
                .get(head)
                .and_then(|n| n.get(tail))
                .or(self.value.as_ref()),
        }
    }
    pub fn iter(&self) -> DTrieIter<Prop> {
        DTrieIter::new(self)
    }
    pub fn delete(&mut self, key: &[u8]) {
        self.inner_delete(key);
    }
}

pub struct DTrieIter<'a, V> {
    stack: Vec<(Vec<u8>, &'a DTrie<V>)>,
    crnt_key: Vec<u8>,
    crnt_trie: &'a DTrie<V>,
}
impl<'a, V> DTrieIter<'a, V> {
    fn new(trie: &'a DTrie<V>) -> DTrieIter<'a, V> {
        DTrieIter {
            stack: Vec::new(),
            crnt_key: Vec::new(),
            crnt_trie: trie,
        }
    }
}
impl<'a, V> Iterator for DTrieIter<'a, V> {
    type Item = (Vec<u8>, &'a V);
    fn next(&mut self) -> Option<Self::Item> {
        for (index, child) in self.crnt_trie.go_table.iter() {
            let mut key = self.crnt_key.clone();
            key.push(*index);
            self.stack.push((key, child));
        }
        match self.stack.pop() {
            None => None,
            Some((new_key, new_trie)) => {
                self.crnt_key = new_key;
                self.crnt_trie = new_trie;
                if !self.crnt_trie.is_value() {
                    self.next()
                } else {
                    self.crnt_trie
                        .value
                        .as_ref()
                        .map(|v| (self.crnt_key.clone(), v))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    const VALUES: &[usize] = &[12, 0, 1, 2];
    const KEY: (&str, usize) = ("key", VALUES[0]);
    const N0: (&str, usize) = ("key_0", VALUES[1]);
    const N1: (&str, usize) = ("key_1", VALUES[2]);
    const N2: (&str, usize) = ("key_2", VALUES[3]);

    #[test]
    fn insert_search() {
        let mut trie = DTrie::new();
        trie.insert(N0.0.as_bytes(), N0.1);
        trie.insert(N1.0.as_bytes(), N1.1);
        trie.insert(N2.0.as_bytes(), N2.1);
        trie.insert(KEY.0.as_bytes(), KEY.1);
        assert_eq!(trie.get(N0.0.as_bytes()).copied(), Some(N0.1));
        assert_eq!(trie.get(N1.0.as_bytes()).copied(), Some(N1.1));
        assert_eq!(trie.get(N2.0.as_bytes()).copied(), Some(N2.1));
        assert_eq!(trie.get(KEY.0.as_bytes()).copied(), Some(KEY.1));
        let expected = VALUES.iter().copied();
        let got = trie.iter().map(|(_, v)| v).copied();
        assert_eq!(
            expected.collect::<HashSet<_>>(),
            got.collect::<HashSet<_>>()
        );
    }
    #[test]
    fn delete_search() {
        let mut trie = DTrie::new();
        trie.insert(N0.0.as_bytes(), N0.1);
        trie.insert(N1.0.as_bytes(), N1.1);
        trie.insert(N2.0.as_bytes(), N2.1);
        trie.insert(KEY.0.as_bytes(), KEY.1);

        let mut t1 = trie.clone();
        t1.delete(N0.0.as_bytes());
        assert_eq!(t1.get(N0.0.as_bytes()).copied(), Some(KEY.1));
        assert_eq!(t1.get(N1.0.as_bytes()).copied(), Some(N1.1));
        assert_eq!(t1.get(N2.0.as_bytes()).copied(), Some(N2.1));
        assert_eq!(t1.get(KEY.0.as_bytes()).copied(), Some(KEY.1));

        let mut t2 = trie.clone();
        t2.delete(N1.0.as_bytes());
        assert_eq!(t2.get(N0.0.as_bytes()).copied(), Some(N0.1));
        assert_eq!(t2.get(N1.0.as_bytes()).copied(), Some(KEY.1));
        assert_eq!(t2.get(N2.0.as_bytes()).copied(), Some(N2.1));
        assert_eq!(t2.get(KEY.0.as_bytes()).copied(), Some(KEY.1));

        let mut t3 = trie.clone();
        t3.delete(KEY.0.as_bytes());
        assert_eq!(t3.get(N0.0.as_bytes()).copied(), Some(N0.1));
        assert_eq!(t3.get(N1.0.as_bytes()).copied(), Some(N1.1));
        assert_eq!(t3.get(N2.0.as_bytes()).copied(), Some(N2.1));
        assert_eq!(t3.get(KEY.0.as_bytes()).copied(), None);
        t3.delete(N0.0.as_bytes());
        assert_eq!(t3.get(N0.0.as_bytes()).copied(), None);
        t3.delete(N1.0.as_bytes());
        assert_eq!(t3.get(N1.0.as_bytes()).copied(), None);
        t3.delete(N2.0.as_bytes());
        assert_eq!(t3.get(N2.0.as_bytes()).copied(), None);
    }
}
