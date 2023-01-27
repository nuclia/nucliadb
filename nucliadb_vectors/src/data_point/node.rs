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

use crate::data_types::key_value::Slot;
use crate::data_types::trie;
use crate::data_types::usize_utils::*;

// Nodes are the main element of the system. The following data is stored inside them:
// -> vector: Vec<u8> used for building a hnsw index with them (is a serialized Vec<f32>).
// -> key: String assigned by the user to identify the node.
// Once a node is persisted by the system it will be dentified by a pointer. This pointer represents
// the start of the serialized node in the file (or other element) it is serialized into. Therefore
// nodes are used by the system in their serialized form, which is:
// len: number of bytes representing this node. (usize in little endian)
// vector_start: byte where the vector segment starts. (usize in little endian)
// key_start: byte where the key segment starts. (usize in little endian)
// label_start: byte where the label segment starts. (usize in little endian)
// vector segment:
// - len: size of the segment. (usize in little endian)
// - value: the vector.
// string segment:
// - len: size of the segment. (usize in little endian)
// - value: the serialized string.
// label segment: trie

const LEN: (usize, usize) = (0, USIZE_LEN);
const VECTOR_START: (usize, usize) = (LEN.1, LEN.1 + USIZE_LEN);
const KEY_START: (usize, usize) = (VECTOR_START.1, VECTOR_START.1 + USIZE_LEN);
const LABEL_START: (usize, usize) = (KEY_START.1, KEY_START.1 + USIZE_LEN);
const HEADER_LEN: usize = 4 * USIZE_LEN;

#[derive(Clone, Copy)]
pub struct Node;
impl Node {
    pub fn serialized_len<S, V, T>(key: S, vector: V, trie: T) -> usize
    where
        S: AsRef<[u8]>,
        V: AsRef<[u8]>,
        T: AsRef<[u8]>,
    {
        let skey = key.as_ref();
        let svector = vector.as_ref();
        let strie = trie.as_ref();
        let svector_len = svector.len() + USIZE_LEN;
        let skey_len = skey.len() + USIZE_LEN;
        let slabels_len = strie.len();
        HEADER_LEN + svector_len + skey_len + slabels_len
    }
    pub fn serialize<S, V, T>(key: S, vector: V, labels: T) -> Vec<u8>
    where
        S: AsRef<[u8]>,
        V: AsRef<[u8]>,
        T: AsRef<[u8]>,
    {
        let mut buf = vec![];
        Node::serialize_into(&mut buf, key, vector, labels).unwrap();
        buf
    }
    // labels must be sorted.
    pub fn serialize_into<W, S, V, T>(mut w: W, key: S, vector: V, trie: T) -> io::Result<()>
    where
        W: io::Write,
        S: AsRef<[u8]>,
        V: AsRef<[u8]>,
        T: AsRef<[u8]>,
    {
        let skey = key.as_ref();
        let svector = vector.as_ref();
        let strie = trie.as_ref();
        let svector_len = svector.len() + USIZE_LEN;
        let skey_len = skey.len() + USIZE_LEN;
        let slabels_len = strie.len();
        let len = HEADER_LEN + svector_len + skey_len + slabels_len;
        let vector_start = HEADER_LEN;
        let key_start = vector_start + svector_len;
        let labels_start = key_start + skey_len;

        w.write_all(&len.to_le_bytes())?;
        w.write_all(&vector_start.to_le_bytes())?;
        w.write_all(&key_start.to_le_bytes())?;
        w.write_all(&labels_start.to_le_bytes())?;
        w.write_all(&svector.len().to_le_bytes())?;
        w.write_all(svector)?;
        w.write_all(&skey.len().to_le_bytes())?;
        w.write_all(skey)?;
        w.write_all(strie)?;
        w.flush()
    }
    // x must be serialized using Node, may have trailing bytes.
    pub fn key(x: &[u8]) -> &[u8] {
        let xkey_ptr = usize_from_slice_le(&x[KEY_START.0..KEY_START.1]);
        let xkey_len = usize_from_slice_le(&x[xkey_ptr..(xkey_ptr + USIZE_LEN)]);
        let xkey_start = xkey_ptr + USIZE_LEN;
        &x[xkey_start..(xkey_start + xkey_len)]
    }
    // x must be serialized using Node, may have trailing bytes.
    pub fn vector(x: &[u8]) -> &[u8] {
        let xvec_ptr = usize_from_slice_le(&x[VECTOR_START.0..VECTOR_START.1]);
        let xvec_len = usize_from_slice_le(&x[xvec_ptr..(xvec_ptr + USIZE_LEN)]);
        let xvec_start = xvec_ptr + USIZE_LEN;
        &x[xvec_start..(xvec_start + xvec_len)]
    }
    // x must be serialized using Node, may have trailing bytes.
    pub fn has_label(x: &[u8], label: &[u8]) -> bool {
        let xlabel_ptr = usize_from_slice_le(&x[LABEL_START.0..LABEL_START.1]);
        trie::has_word(&x[xlabel_ptr..], label)
    }
}
impl Slot for Node {
    fn cmp_keys(&self, x: &[u8], key: &[u8]) -> std::cmp::Ordering {
        let xkey = self.get_key(x);
        xkey.cmp(key)
    }
    fn get_key<'a>(&self, x: &'a [u8]) -> &'a [u8] {
        Self::key(x)
    }
    fn read_exact<'a>(&self, x: &'a [u8]) -> (/* head */ &'a [u8], /* tail */ &'a [u8]) {
        let len = usize_from_slice_le(&x[LEN.0..LEN.1]);
        (&x[0..len], &x[len..])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_types::{trie_ram, vector};
    lazy_static::lazy_static! {
        static ref NO_LABELS_TRIE: Vec<u8> = trie::serialize(trie_ram::create_trie(&NO_LABELS));
        static ref LABELS_TRIE: Vec<u8> = trie::serialize(trie_ram::create_trie(&LABELS));
    }
    const NO_LABELS: [&[u8]; 0] = [];
    const LABELS: [&[u8]; 3] = [b"L1", b"L2", b"L3"];
    #[test]
    fn create_test() {
        let key = b"NODE1";
        let vector = vector::encode_vector(&[12.; 1000]);
        let mut buf = Vec::new();
        Node::serialize_into(&mut buf, key, &vector, NO_LABELS_TRIE.clone()).unwrap();
        let len = usize_from_slice_le(&buf[LEN.0..LEN.1]);
        let vector_start = usize_from_slice_le(&buf[VECTOR_START.0..VECTOR_START.1]);
        let key_start = usize_from_slice_le(&buf[KEY_START.0..KEY_START.1]);
        let vector_len = usize_from_slice_le(&buf[vector_start..(vector_start + USIZE_LEN)]);
        let key_len = usize_from_slice_le(&buf[key_start..(key_start + USIZE_LEN)]);
        let svector = (vector_start + USIZE_LEN)..(vector_start + USIZE_LEN + vector_len);
        let skey = (key_start + USIZE_LEN)..(key_start + USIZE_LEN + key_len);
        assert_eq!(len, buf.len());
        assert_eq!(vector_len, vector.len());
        assert_eq!(key_len, key.len());
        assert_eq!(&buf[svector], &vector);
        assert_eq!(&buf[skey], key.as_slice());
        assert_eq!(Node::vector(&buf), &vector);
        assert_eq!(Node::key(&buf), key);

        let key = b"NODE2";
        let vector = vector::encode_vector(&[13.; 1000]);
        let mut buf = Vec::new();
        Node::serialize_into(&mut buf, key, &vector, LABELS_TRIE.clone()).unwrap();
        let len = usize_from_slice_le(&buf[LEN.0..LEN.1]);
        let vector_start = usize_from_slice_le(&buf[VECTOR_START.0..VECTOR_START.1]);
        let key_start = usize_from_slice_le(&buf[KEY_START.0..KEY_START.1]);
        let vector_len = usize_from_slice_le(&buf[vector_start..(vector_start + USIZE_LEN)]);
        let key_len = usize_from_slice_le(&buf[key_start..(key_start + USIZE_LEN)]);
        let svector = (vector_start + USIZE_LEN)..(vector_start + USIZE_LEN + vector_len);
        let skey = (key_start + USIZE_LEN)..(key_start + USIZE_LEN + key_len);
        assert_eq!(len, buf.len());
        assert_eq!(vector_len, vector.len());
        assert_eq!(key_len, key.len());
        assert_eq!(&buf[svector], &vector);
        assert_eq!(&buf[skey], key.as_slice());
        assert_eq!(Node::vector(&buf), &vector);
        assert_eq!(Node::key(&buf), key);
        assert!(LABELS.iter().all(|l| Node::has_label(&buf, l)));
    }

    #[test]
    fn look_up_test() {
        let mut buf = Vec::new();
        let key1 = b"NODE1";
        let vector1 = vector::encode_vector(&[12.; 1000]);
        let node1 = buf.len();
        Node::serialize_into(&mut buf, key1, &vector1, NO_LABELS_TRIE.clone()).unwrap();
        let key2 = b"NODE2";
        let vector2 = vector::encode_vector(&[15.; 1000]);
        let node2 = buf.len();
        Node::serialize_into(&mut buf, key2, &vector2, NO_LABELS_TRIE.clone()).unwrap();
        assert_eq!(Node::key(&buf[node1..]), key1);
        assert_eq!(Node::key(&buf[node2..]), key2);
        assert_eq!(Node::vector(&buf[node1..]), vector1);
        assert_eq!(Node::vector(&buf[node2..]), vector2);
        assert!(Node.keep_in_merge(&buf[node1..]));
        assert!(Node.keep_in_merge(&buf[node2..]));
        assert_eq!(Node.cmp_slot(&buf[node1..], &buf[node2..]), key1.cmp(key2));
        assert_eq!(
            Node.read_exact(&buf[node1..]),
            (&buf[node1..node2], &buf[node2..])
        );
        assert_eq!(
            Node.read_exact(&buf[node2..]),
            (&buf[node2..], [].as_slice())
        );
    }
}
