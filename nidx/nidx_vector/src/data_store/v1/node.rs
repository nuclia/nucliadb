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

use super::trie;
use crate::data_types::usize_utils::*;
use std::io;

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
// [Block of metadata, may be empty]: Everything between the header and the content is consider metadata.
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

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Ord, Eq)]
pub struct Node<'a>(&'a [u8]);
impl<'a> Node<'a> {
    pub fn new(start: &'a [u8]) -> Node<'a> {
        let len = usize_from_slice_le(&start[LEN.0..LEN.1]);
        Self(&start[0..len])
    }

    pub fn bytes(&self) -> &[u8] {
        self.0
    }

    // labels must be sorted.
    pub fn serialize_into<W, S, V, T, M>(
        mut w: W,
        key: S,
        vector: V,
        alignment: usize,
        trie: T,
        metadata: Option<M>,
    ) -> io::Result<()>
    where
        W: io::Write,
        S: AsRef<str>,
        V: AsRef<[u8]>,
        T: AsRef<[u8]>,
        M: AsRef<[u8]>,
    {
        let skey = key.as_ref();
        let svector = vector.as_ref();
        let strie = trie.as_ref();

        // Reading lens
        let svector_len = svector.len() + USIZE_LEN;
        let skey_len = skey.len() + USIZE_LEN;
        let slabels_len = strie.len();
        let metadata_len = metadata.as_ref().map(|m| m.as_ref().len()).unwrap_or_default();

        // Pointer computations
        let vector_start = HEADER_LEN + metadata_len;
        let vector_pad = if vector_start % alignment > 0 {
            alignment - (vector_start % alignment)
        } else {
            0
        };
        let key_start = vector_start + svector_len + vector_pad;
        let labels_start = key_start + skey_len;

        let len = HEADER_LEN + vector_pad + svector_len + skey_len + slabels_len + metadata_len;

        // Write pointers
        w.write_all(&len.to_le_bytes())?;
        w.write_all(&vector_start.to_le_bytes())?;
        w.write_all(&key_start.to_le_bytes())?;
        w.write_all(&labels_start.to_le_bytes())?;
        // Metadata segment
        metadata.map_or(Ok(()), |m| w.write_all(m.as_ref()))?;
        // Values
        w.write_all(&(svector.len() as u32).to_le_bytes())?;
        w.write_all(&(vector_pad as u32).to_le_bytes())?;
        w.write_all(&[0].repeat(vector_pad))?;
        w.write_all(svector)?;
        w.write_all(&skey.len().to_le_bytes())?;
        w.write_all(skey.as_bytes())?;
        w.write_all(strie)?;
        w.flush()
    }
    // x must be serialized using Node, may have trailing bytes.
    pub fn metadata(&self) -> &[u8] {
        // The metadata starts just after the header ends.
        let metadata_start = LABEL_START.1;
        // The metadata ends when the vector segment starts.
        let metadata_end = usize_from_slice_le(&self.0[VECTOR_START.0..VECTOR_START.1]);
        &self.0[metadata_start..metadata_end]
    }
    // x must be serialized using Node, may have trailing bytes.
    // This function will decompress the trie data structure that contains the
    // labels. Use only if you need all the labels.
    pub fn labels(&self) -> Vec<String> {
        let xlabel_ptr = usize_from_slice_le(&self.0[LABEL_START.0..LABEL_START.1]);
        trie::decompress(&self.0[xlabel_ptr..])
    }
    // x must be serialized using Node, may have trailing bytes.
    pub fn key(&self) -> &str {
        let xkey_ptr = usize_from_slice_le(&self.0[KEY_START.0..KEY_START.1]);
        let xkey_len = usize_from_slice_le(&self.0[xkey_ptr..(xkey_ptr + USIZE_LEN)]);
        let xkey_start = xkey_ptr + USIZE_LEN;
        std::str::from_utf8(&self.0[xkey_start..(xkey_start + xkey_len)]).unwrap()
    }
    // x must be serialized using Node, may have trailing bytes.
    pub fn vector(&self) -> &'a [u8] {
        let xvec_ptr = usize_from_slice_le(&self.0[VECTOR_START.0..VECTOR_START.1]);
        let xvec_len = u32_from_slice_le(&self.0[xvec_ptr..(xvec_ptr + U32_LEN)]) as usize;
        let xvec_pad = u32_from_slice_le(&self.0[(xvec_ptr + U32_LEN)..(xvec_ptr + 2 * U32_LEN)]);
        let xvec_start = xvec_ptr + 2 * U32_LEN + xvec_pad as usize;
        &self.0[xvec_start..(xvec_start + xvec_len)]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{data_store::v1::trie_ram, vector_types::dense_f32};
    lazy_static::lazy_static! {
        static ref NO_LABELS_TRIE: Vec<u8> = trie::serialize(trie_ram::create_trie(&NO_LABELS));
        static ref LABELS_TRIE: Vec<u8> = trie::serialize(trie_ram::create_trie(&LABELS));
    }
    const NO_LABELS: [&[u8]; 0] = [];
    const LABELS: [&[u8]; 3] = [b"L1", b"L2", b"L3"];
    const NO_METADATA: Option<&[u8]> = None;

    #[test]
    fn create_test() {
        let key = "NODE1";
        let vector = dense_f32::encode_vector(&[12.; 1000]);
        let mut buf = Vec::new();
        Node::serialize_into(&mut buf, key, &vector, 1, NO_LABELS_TRIE.clone(), NO_METADATA).unwrap();
        let len = usize_from_slice_le(&buf[LEN.0..LEN.1]);
        let vector_start = usize_from_slice_le(&buf[VECTOR_START.0..VECTOR_START.1]);
        let key_start = usize_from_slice_le(&buf[KEY_START.0..KEY_START.1]);
        let vector_len = usize_from_slice_le(&buf[vector_start..(vector_start + USIZE_LEN)]);
        let key_len = usize_from_slice_le(&buf[key_start..(key_start + USIZE_LEN)]);
        let svector = (vector_start + USIZE_LEN)..(vector_start + USIZE_LEN + vector_len);
        let skey = (key_start + USIZE_LEN)..(key_start + USIZE_LEN + key_len);

        let node = Node::new(&buf);
        let metadata = node.metadata();
        assert_eq!(metadata.len(), 0);
        assert_eq!(len, buf.len());
        assert_eq!(vector_len, vector.len());
        assert_eq!(key_len, key.len());
        assert_eq!(&buf[svector], &vector);
        assert_eq!(&buf[skey], key.as_bytes());
        assert_eq!(node.vector(), &vector);
        assert_eq!(node.key(), key);

        let key = "NODE2";
        let metadata = b"THIS ARE THE METADATA CONTENTS";
        let vector = dense_f32::encode_vector(&[13.; 1000]);
        let mut buf = Vec::new();
        Node::serialize_into(&mut buf, key, &vector, 1, LABELS_TRIE.clone(), Some(metadata)).unwrap();
        let len = usize_from_slice_le(&buf[LEN.0..LEN.1]);
        let vector_start = usize_from_slice_le(&buf[VECTOR_START.0..VECTOR_START.1]);
        let key_start = usize_from_slice_le(&buf[KEY_START.0..KEY_START.1]);
        let vector_len = usize_from_slice_le(&buf[vector_start..(vector_start + USIZE_LEN)]);
        let key_len = usize_from_slice_le(&buf[key_start..(key_start + USIZE_LEN)]);
        let svector = (vector_start + USIZE_LEN)..(vector_start + USIZE_LEN + vector_len);
        let skey = (key_start + USIZE_LEN)..(key_start + USIZE_LEN + key_len);

        let node = Node::new(&buf);
        let smetadata = node.metadata();
        assert_eq!(smetadata, metadata.as_slice());
        assert_eq!(len, buf.len());
        assert_eq!(vector_len, vector.len());
        assert_eq!(key_len, key.len());
        assert_eq!(&buf[svector], &vector);
        assert_eq!(&buf[skey], key.as_bytes());
        assert_eq!(node.vector(), &vector);
        assert_eq!(node.key(), key);
        assert!(
            LABELS
                .iter()
                .all(|l| node.labels().contains(&String::from_utf8_lossy(l).to_string()))
        );
    }

    #[test]
    fn look_up_test() {
        let mut buf = Vec::new();
        let key1 = "NODE1";
        let metadata1 = b"The node 1 has metadata";
        let vector1 = dense_f32::encode_vector(&[12.; 1000]);
        let node1 = buf.len();
        Node::serialize_into(&mut buf, key1, &vector1, 1, NO_LABELS_TRIE.clone(), Some(&metadata1)).unwrap();
        let key2 = "NODE2";
        let metadata2 = b"Tuns out node 2 also has metadata";
        let vector2 = dense_f32::encode_vector(&[15.; 1000]);
        let node2 = buf.len();
        Node::serialize_into(&mut buf, key2, &vector2, 1, NO_LABELS_TRIE.clone(), Some(&metadata2)).unwrap();
        assert_eq!(Node::new(&buf[node1..]).key(), key1);
        assert_eq!(Node::new(&buf[node2..]).key(), key2);
        assert_eq!(Node::new(&buf[node1..]).vector(), vector1);
        assert_eq!(Node::new(&buf[node2..]).vector(), vector2);
        assert_eq!(Node::new(&buf[node1..]).metadata(), metadata1);
        assert_eq!(Node::new(&buf[node2..]).metadata(), metadata2);
    }
}
