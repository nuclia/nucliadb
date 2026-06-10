// Copyright 2021 Bosutech XXI S.L.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

use std::collections::HashMap;

const START: usize = 0;
pub type GoTable = HashMap<u8, usize>;
pub type Trie = Vec<(bool, GoTable)>;

fn traverse(buf: &[u8], node: usize, trie: &Trie) -> Result<usize, (usize, usize)> {
    match buf {
        [head, tail @ ..] if trie[node].1.contains_key(head) => traverse(tail, trie[node].1[head], trie),
        [_head, ..] => Err((buf.len(), node)),
        [] => Ok(node),
    }
}
fn trie_insert(buf: &[u8], node: usize, trie: &mut Trie) -> usize {
    if let [head, tail @ ..] = buf {
        let new_node = trie.len();
        trie.push((false, GoTable::new()));
        trie[node].1.insert(*head, new_node);
        trie_insert(tail, new_node, trie)
    } else {
        trie[node].0 = true;
        node
    }
}

pub fn create_trie<L: AsRef<[u8]>>(contents: &[L]) -> Trie {
    let mut trie = vec![(false, GoTable::new())];
    for content in contents {
        let content = content.as_ref();
        match traverse(content, START, &trie) {
            Ok(node) => {
                trie[node].0 = true;
            }
            Err((len, node)) => {
                let start = content.len() - len;
                trie_insert(&content[start..], node, &mut trie);
            }
        }
    }
    trie
}

#[cfg(test)]
pub fn has_word(trie: &Trie, word: &[u8]) -> bool {
    match traverse(word, START, trie) {
        Ok(node) => trie[node].0,
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn create_and_search_test() {
        let dictionary = [
            b"WORD1".as_slice(),
            b"WORD2".as_slice(),
            b"WORD3".as_slice(),
            b"ORD1".as_slice(),
            b"BAD".as_slice(),
            b"GOOD".as_slice(),
        ];
        let not_in_dictionary = [
            b"WO1D1".as_slice(),
            b"LORD".as_slice(),
            b"BAF".as_slice(),
            b"WOR".as_slice(),
        ];

        let trie = create_trie(&dictionary);
        assert!(dictionary.iter().all(|w| has_word(&trie, w)));
        assert!(not_in_dictionary.iter().all(|w| !has_word(&trie, w)));
    }
}
