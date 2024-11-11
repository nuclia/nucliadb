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

use nidx_types::Seq;

#[derive(Default, Clone)]
pub struct DTrie {
    value: Option<Seq>,
    go_table: HashMap<u8, Box<DTrie>>,
}
impl DTrie {
    fn inner_get(&self, key: &[u8], current: Option<Seq>) -> Option<Seq> {
        let current = std::cmp::max(current, self.value);
        let [head, tail @ ..] = key else {
            return current;
        };
        let Some(node) = self.go_table.get(head) else {
            return current;
        };
        node.inner_get(tail, current)
    }
    #[cfg(test)]
    fn inner_prune(&mut self, time: Seq) -> bool {
        self.value = self.value.filter(|v| *v > time);
        self.go_table = std::mem::take(&mut self.go_table)
            .into_iter()
            .map(|(k, mut v)| (v.inner_prune(time), k, v))
            .filter(|v| !v.0)
            .map(|v| (v.1, v.2))
            .collect();
        self.value.is_none() && self.go_table.is_empty()
    }
    pub fn new() -> DTrie {
        DTrie::default()
    }
    pub fn insert(&mut self, key: &[u8], value: Seq) {
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
    pub fn get(&self, key: &[u8]) -> Option<Seq> {
        self.inner_get(key, None)
    }
    #[cfg(test)]
    pub fn prune(&mut self, time: Seq) {
        self.inner_prune(time);
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
        let tplus0 = 100i64.into();
        let tplus1 = 101i64.into();
        let tplus2 = 102i64.into();
        let tplus3 = 103i64.into();

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
        let tplus0 = 100i64.into();
        let tplus1 = 101i64.into();
        let tplus2 = 102i64.into();
        let tplus3 = 103i64.into();

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
