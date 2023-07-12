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

use std::io;

use super::trie_ram::*;
use super::usize_utils::*;

const ADJ_HEADER: usize = 1 + USIZE_LEN;
const EDGE_LEN: usize = 1 + USIZE_LEN;

const IS_FINAL: (usize, usize) = (0, 1);
const LENGTH: (usize, usize) = (IS_FINAL.1, IS_FINAL.1 + USIZE_LEN);
const TABLE: usize = LENGTH.1;

fn get_node_ptr(trie: &[u8], node: usize) -> usize {
    let start = trie.len() - ((node + 1) * USIZE_LEN);
    usize_from_slice_le(&trie[start..(start + USIZE_LEN)])
}

// A serialized trie is an value section and a index section and the length.
// -> len: usize little endian
// Value section:
// -> Is final: 1 byte, if the value stored is 1 then is final.
// -> no_connexions:  usize little endian.
// per connexion:
// -> 1 byte for the edge label
// -> 1 usize little endian for the targeted node.
// Index section: 1 usize in little per node in order, containing the
// address of the value section.
pub fn serialize_into<W: io::Write>(mut buf: W, trie: Trie) -> io::Result<()> {
    use std::collections::HashMap;
    let no_nodes = trie.len();
    let len = serialized_len(&trie);
    let mut indexing = HashMap::new();
    let mut byte_offset = 0;
    buf.write_all(&len.to_le_bytes())?;
    byte_offset += USIZE_LEN;
    for (node, (is_final, adjacency)) in trie.into_iter().enumerate() {
        indexing.insert(node, byte_offset);
        buf.write_all(&[u8::from(is_final)])?;
        buf.write_all(&adjacency.len().to_le_bytes())?;
        byte_offset += ADJ_HEADER;
        for (edge, node) in adjacency {
            buf.write_all(&edge.to_le_bytes())?;
            buf.write_all(&node.to_le_bytes())?;
            byte_offset += EDGE_LEN;
        }
    }
    for node in (0..no_nodes).rev() {
        let is_in = indexing[&node];
        buf.write_all(&is_in.to_le_bytes())?;
        byte_offset += USIZE_LEN;
    }
    buf.flush()
}
pub fn serialized_len(trie: &Trie) -> usize {
    USIZE_LEN
        + trie
            .iter()
            .map(|(_, table)| EDGE_LEN * table.len())
            .map(|table_len| table_len + ADJ_HEADER)
            .map(|value| value + USIZE_LEN)
            .sum::<usize>()
}
pub fn serialize(trie: Trie) -> Vec<u8> {
    let mut buf = vec![];
    serialize_into(&mut buf, trie).unwrap();
    buf
}
pub fn has_word(trie: &[u8], word: &[u8]) -> bool {
    let len = usize_from_slice_le(&trie[0..USIZE_LEN]);
    search(&trie[0..len], 0, word)
}

pub fn decompress(trie: &[u8]) -> Vec<String> {
    let mut collector = vec![];
    let mut current = vec![];
    let len = usize_from_slice_le(&trie[0..USIZE_LEN]);
    decompress_labels(&trie[0..len], 0, &mut collector, &mut current);
    collector
}

fn decompress_labels(trie: &[u8], node: usize, collector: &mut Vec<String>, current: &mut Vec<u8>) {
    let node_ptr = get_node_ptr(trie, node);
    if trie[node_ptr] == 1 {
        let label = String::from_utf8_lossy(current).to_string();
        collector.push(label);
    }
    let offset = &trie[node_ptr..];
    let length = usize_from_slice_le(&offset[LENGTH.0..LENGTH.1]);
    let adjacency = &offset[TABLE..];
    let mut i = 0;
    while i < length {
        let position = i * EDGE_LEN;
        let number_s = position + 1;
        let number_e = number_s + USIZE_LEN;
        let new_byte = adjacency[position];
        let new_node = usize_from_slice_le(&adjacency[number_s..number_e]);
        current.push(new_byte);
        decompress_labels(trie, new_node, collector, current);
        current.pop();
        i += 1;
    }
}

fn search(trie: &[u8], node: usize, word: &[u8]) -> bool {
    let node_ptr = get_node_ptr(trie, node);
    match word {
        [] => trie[node_ptr] == 1,
        [head, tail @ ..] => {
            let offset = &trie[node_ptr..];
            let length = usize_from_slice_le(&offset[LENGTH.0..LENGTH.1]);
            let adjacency = &offset[TABLE..];
            let mut i = 0;
            let mut goes_to = None;
            while i < length && goes_to.is_none() {
                let position = i * EDGE_LEN;
                if *head == adjacency[position] {
                    let number_s = position + 1;
                    let number_e = number_s + USIZE_LEN;
                    goes_to = Some(usize_from_slice_le(&adjacency[number_s..number_e]));
                }
                i += 1;
            }
            match goes_to {
                Some(new_node) => search(trie, new_node, tail),
                None => false,
            }
        }
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
        let trie = serialize(trie);
        let labels = super::decompress(&trie);
        assert!(dictionary.iter().all(|w| has_word(&trie, w)));
        assert!(not_in_dictionary.iter().all(|w| !has_word(&trie, w)));

        assert_eq!(labels.len(), dictionary.len());
        assert!(labels.iter().all(|w| dictionary.contains(&w.as_bytes())));
    }
}
