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
use std::marker::PhantomData;

pub type NodeIDGen = IDGenerator<Node>;
pub type NodeID = ID<Node>;

pub trait ByteRpr {
    fn serialize(&self) -> Vec<u8>;
    fn deserialize(bytes: &[u8]) -> Self;
}

pub trait FixedByteLen: ByteRpr {
    fn segment_len() -> usize;
}
pub trait Distance {
    fn cosine(i: &Self, j: &Self) -> f32;
}

#[derive(Clone)]
pub struct HNSWParams {
    pub no_layers: usize,
    pub m_max: usize,
    pub m: usize,
    pub ef_construction: usize,
    pub k_neighbours: usize,
}
impl Default for HNSWParams {
    fn default() -> Self {
        HNSWParams {
            no_layers: 4,
            m_max: 16,
            m: 16,
            ef_construction: 100,
            k_neighbours: 10,
        }
    }
}
impl HNSWParams {
    pub const fn no_layers() -> usize {
        4
    }
    pub const fn m_max() -> usize {
        16
    }
    pub const fn m() -> usize {
        16
    }
    pub const fn ef_construction() -> usize {
        100
    }
    pub const fn k_neighbours() -> usize {
        10
    }
}

#[derive(Debug)]
pub struct ID<T> {
    uuid: u128,
    of_type: PhantomData<T>,
}
impl<T> std::fmt::Display for ID<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ID({})", self.uuid)
    }
}
impl<T> Copy for ID<T> {}
impl<T> Eq for ID<T> {}
impl<T> Ord for ID<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}
impl<T> PartialEq for ID<T> {
    fn eq(&self, other: &Self) -> bool {
        self.uuid == other.uuid
    }
}
impl<T> PartialOrd for ID<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.uuid.cmp(&other.uuid))
    }
}
impl<T> Clone for ID<T> {
    fn clone(&self) -> Self {
        ID {
            uuid: self.uuid,
            of_type: self.of_type,
        }
    }
}
impl<T> std::hash::Hash for ID<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.uuid.hash(state);
    }
}
impl<T> ByteRpr for ID<T> {
    fn serialize(&self) -> Vec<u8> {
        self.uuid.to_le_bytes().to_vec()
    }
    fn deserialize(bytes: &[u8]) -> Self {
        ID {
            uuid: u128::deserialize(&bytes[..16]),
            of_type: PhantomData,
        }
    }
}
impl<T> FixedByteLen for ID<T> {
    fn segment_len() -> usize {
        u128::segment_len()
    }
}
impl<T> ID<T> {
    fn new(uuid: u128) -> ID<T> {
        ID {
            uuid,
            of_type: PhantomData,
        }
    }
    fn value(&self) -> u128 {
        self.uuid
    }
}

#[cfg(test)]
mod id_test_serialization {
    use super::*;
    #[test]
    fn serialize() {
        let id_0 = NodeID {
            uuid: 0,
            of_type: PhantomData,
        };
        assert_eq!(id_0.serialize().len(), NodeID::segment_len());
        assert_eq!(NodeID::deserialize(&id_0.serialize()), id_0);
    }
}

pub struct IDGenerator<T> {
    fresh: ID<T>,
}
impl<T> Default for IDGenerator<T> {
    fn default() -> Self {
        Self::new()
    }
}
impl<T> IDGenerator<T> {
    pub fn new() -> IDGenerator<T> {
        IDGenerator { fresh: ID::new(0) }
    }
    pub fn from_seed(id: ID<T>) -> IDGenerator<T> {
        IDGenerator { fresh: id }
    }
    pub fn produce_id(&mut self) -> ID<T> {
        let prev = self.fresh;
        self.fresh = ID::new(prev.value() + 1);
        prev
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct EntryPoint {
    pub node: NodeID,
    pub layer: u64,
}

impl ByteRpr for EntryPoint {
    fn serialize(&self) -> Vec<u8> {
        let mut node = self.node.serialize();
        let mut layer = self.layer.serialize();
        let mut result = Vec::new();
        result.append(&mut node);
        result.append(&mut layer);
        result
    }
    fn deserialize(bytes: &[u8]) -> Self {
        let node_start = 0;
        let node_end = node_start + NodeID::segment_len();
        let layer_start = node_end;
        let layer_end = layer_start + u64::segment_len();
        EntryPoint {
            node: NodeID::deserialize(&bytes[node_start..node_end]),
            layer: u64::deserialize(&bytes[layer_start..layer_end]),
        }
    }
}

impl FixedByteLen for EntryPoint {
    fn segment_len() -> usize {
        let node_id_len = NodeID::segment_len();
        let layer_len = u64::segment_len();
        node_id_len + layer_len
    }
}

#[cfg(test)]
mod entry_point_test_serialization {
    use super::*;
    #[test]
    fn serialize() {
        let id_0 = NodeID {
            uuid: 0,
            of_type: PhantomData,
        };
        let ep = EntryPoint {
            node: id_0,
            layer: 0,
        };
        assert_eq!(NodeID::deserialize(&id_0.serialize()), id_0);
        assert_eq!(ep.serialize().len(), EntryPoint::segment_len());
        assert_eq!(ep, EntryPoint::deserialize(&ep.serialize()));
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct FileSegment {
    pub start: u64,
    pub end: u64,
}

impl ByteRpr for FileSegment {
    fn serialize(&self) -> Vec<u8> {
        let mut result = vec![];
        result.append(&mut self.start.serialize());
        result.append(&mut self.end.serialize());
        result
    }
    fn deserialize(bytes: &[u8]) -> Self {
        let start_s = 0;
        let start_e = start_s + u64::segment_len();
        let end_s = start_e;
        let end_e = end_s + u64::segment_len();
        FileSegment {
            start: u64::deserialize(&bytes[start_s..start_e]),
            end: u64::deserialize(&bytes[end_s..end_e]),
        }
    }
}

impl FixedByteLen for FileSegment {
    fn segment_len() -> usize {
        2 * u64::segment_len()
    }
}

#[cfg(test)]
mod file_segment_test_serialization {
    use super::*;
    #[test]
    fn serialize() {
        let id_0 = NodeID {
            uuid: 0,
            of_type: PhantomData,
        };
        let fs = FileSegment { start: 0, end: 0 };
        assert_eq!(fs.serialize().len(), FileSegment::segment_len());
        assert_eq!(FileSegment::deserialize(&fs.serialize()), fs);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Eq, Ord)]
pub struct Node {
    pub key: FileSegment,
    pub vector: FileSegment,
}
impl ByteRpr for Node {
    fn serialize(&self) -> Vec<u8> {
        let mut result = Vec::new();
        result.append(&mut self.key.serialize());
        result.append(&mut self.vector.serialize());
        result
    }
    fn deserialize(bytes: &[u8]) -> Self {
        let key_start = 0;
        let key_end = key_start + FileSegment::segment_len();
        let vector_start = key_end;
        let vector_end = vector_start + FileSegment::segment_len();
        Node {
            key: FileSegment::deserialize(&bytes[key_start..key_end]),
            vector: FileSegment::deserialize(&bytes[vector_start..vector_end]),
        }
    }
}
impl FixedByteLen for Node {
    fn segment_len() -> usize {
        2 * FileSegment::segment_len()
    }
}

#[cfg(test)]
mod node_test_serialization {
    use super::*;
    #[test]
    fn serialize() {
        let node = Node {
            key: FileSegment { start: 0, end: 0 },
            vector: FileSegment { start: 2, end: 2 },
        };
        assert_eq!(node.serialize().len(), Node::segment_len());
        assert_eq!(Node::deserialize(&node.serialize()), node);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd)]
pub struct Edge {
    pub from: NodeID,
    pub to: NodeID,
    pub dist: f32,
}

impl ByteRpr for Edge {
    fn serialize(&self) -> Vec<u8> {
        let mut result = Vec::new();
        result.append(&mut self.from.serialize());
        result.append(&mut self.to.serialize());
        result.append(&mut self.dist.to_le_bytes().to_vec());
        result
    }
    fn deserialize(bytes: &[u8]) -> Self {
        let from_start = 0;
        let from_end = from_start + NodeID::segment_len();
        let to_start = from_end;
        let to_end = to_start + NodeID::segment_len();
        let dist_start = to_end;
        let dist_end = dist_start + f32::segment_len();
        Edge {
            from: NodeID::deserialize(&bytes[from_start..from_end]),
            to: NodeID::deserialize(&bytes[to_start..to_end]),
            dist: f32::deserialize(&bytes[dist_start..dist_end]),
        }
    }
}

impl FixedByteLen for Edge {
    fn segment_len() -> usize {
        let node_id_len = NodeID::segment_len();
        let f32_len = f32::segment_len();
        (2 * node_id_len) + f32_len
    }
}

#[cfg(test)]
mod edge_test_serialization {
    use super::*;
    #[test]
    fn serialize() {
        let edge = Edge {
            from: NodeID {
                uuid: 0,
                of_type: PhantomData,
            },
            to: NodeID {
                uuid: 1,
                of_type: PhantomData,
            },
            dist: 1.2,
        };
        assert_eq!(edge.serialize().len(), Edge::segment_len());
        assert_eq!(Edge::deserialize(&edge.serialize()), edge);
    }
}

#[derive(Clone, PartialEq, PartialOrd, Debug)]
pub struct Vector {
    raw: Vec<f32>,
}

impl From<Vec<f32>> for Vector {
    fn from(raw: Vec<f32>) -> Self {
        Vector { raw }
    }
}

impl From<Vector> for Vec<f32> {
    fn from(v: Vector) -> Self {
        v.raw
    }
}

impl ByteRpr for Vector {
    fn serialize(&self) -> Vec<u8> {
        self.raw.serialize()
    }
    fn deserialize(bytes: &[u8]) -> Self {
        Vector {
            raw: Vec::deserialize(bytes),
        }
    }
}

#[cfg(test)]
mod vector_test_serialization {
    use super::*;
    #[test]
    fn serialize() {
        let vector = Vector { raw: vec![2.0; 3] };
        assert_eq!(Vector::deserialize(&vector.serialize()), vector);
    }
}

pub struct GraphLayer {
    cnx: HashMap<NodeID, Vec<Edge>>,
}
impl Default for GraphLayer {
    fn default() -> Self {
        GraphLayer::new()
    }
}
impl ByteRpr for GraphLayer {
    fn serialize(&self) -> Vec<u8> {
        let mut serialized = vec![];
        for (k, v) in &self.cnx {
            let mut serialized_key = k.serialize();
            let mut no_elems = (v.len() as u64).serialize();
            let mut connexions = v.serialize();
            serialized.append(&mut serialized_key);
            serialized.append(&mut no_elems);
            serialized.append(&mut connexions);
        }
        serialized
    }
    fn deserialize(bytes: &[u8]) -> Self {
        let mut cnx = HashMap::new();
        let key_len = NodeID::segment_len();
        let value_len = Edge::segment_len();
        let len_len = u64::segment_len();
        let mut segment_start = 0;
        while segment_start < bytes.len() {
            let key_start = segment_start;
            let key_end = key_start + key_len;
            let key = NodeID::deserialize(&bytes[key_start..key_end]);
            let no_elems_start = key_end;
            let no_elems_end = no_elems_start + len_len;
            let no_elems = u64::deserialize(&bytes[no_elems_start..no_elems_end]) as usize;
            let edges_start = no_elems_end;
            let edges_end = edges_start + (no_elems * value_len);
            let edges = Vec::deserialize(&bytes[edges_start..edges_end]);
            cnx.insert(key, edges);
            segment_start = edges_end;
        }
        GraphLayer { cnx }
    }
}
impl std::ops::Index<(NodeID, usize)> for GraphLayer {
    type Output = Edge;
    fn index(&self, (node, edge): (NodeID, usize)) -> &Self::Output {
        self.cnx.get(&node).map(|v| &v[edge]).unwrap()
    }
}

impl GraphLayer {
    pub fn new() -> GraphLayer {
        GraphLayer {
            cnx: HashMap::new(),
        }
    }
    pub fn add_node(&mut self, node: NodeID) {
        self.cnx.insert(node, vec![]);
    }
    pub fn add_edge(&mut self, node: NodeID, edge: Edge) {
        let edges = self.cnx.entry(node).or_insert(vec![]);
        edges.push(edge);
    }
    pub fn get_edge(&self, node: NodeID, id: usize) -> Option<Edge> {
        self.cnx.get(&node).map(|v| v[id])
    }
    pub fn no_edges(&self, node: NodeID) -> Option<usize> {
        self.cnx.get(&node).map(|v| v.len())
    }
}

impl ByteRpr for String {
    fn serialize(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }
    fn deserialize(bytes: &[u8]) -> Self {
        std::str::from_utf8(bytes).unwrap().to_string()
    }
}

impl ByteRpr for Vec<u8> {
    fn serialize(&self) -> Vec<u8> {
        self.clone()
    }
    fn deserialize(bytes: &[u8]) -> Self {
        bytes.to_vec()
    }
}

#[cfg(test)]
mod graph_layer_test_serialization {
    use super::*;
    #[test]
    fn serialize() {
        let node_0 = NodeID {
            uuid: 0,
            of_type: PhantomData,
        };
        let node_1 = NodeID {
            uuid: 1,
            of_type: PhantomData,
        };
        let edge = Edge {
            from: node_0,
            to: node_1,
            dist: 1.2,
        };
        let grap = GraphLayer {
            cnx: [(node_0, vec![edge]), (node_1, vec![edge])]
                .into_iter()
                .collect(),
        };
        let tested = GraphLayer::deserialize(&grap.serialize());
        assert_eq!(grap.no_edges(node_0), tested.no_edges(node_0));
        assert_eq!(grap.no_edges(node_1), tested.no_edges(node_1));
        assert_eq!(grap[(node_0, 0)], grap[(node_0, 0)]);
        assert_eq!(grap[(node_1, 0)], grap[(node_1, 0)]);
    }
}

impl<T> ByteRpr for Vec<T>
where T: ByteRpr + FixedByteLen
{
    fn serialize(&self) -> Vec<u8> {
        let mut result = vec![];
        for elem in self {
            result.append(&mut elem.serialize());
        }
        result
    }
    fn deserialize(bytes: &[u8]) -> Self {
        let segment_len = T::segment_len();
        let mut deserealized = vec![];
        let mut start = 0;
        let mut end = segment_len;
        while start < bytes.len() {
            deserealized.push(T::deserialize(&bytes[start..end]));
            start = end;
            end = start + segment_len;
        }
        deserealized
    }
}

#[cfg(test)]
mod vec_test_serialization {
    use super::*;
    #[test]
    fn serialize() {
        let vector: Vec<u64> = vec![12; 7];
        let tested: Vec<u64> = Vec::deserialize(&vector.serialize());
        assert_eq!(tested, vector);
    }
}

impl<K, V> ByteRpr for std::collections::HashMap<K, V>
where
    K: std::hash::Hash + Eq + ByteRpr + FixedByteLen,
    V: ByteRpr + FixedByteLen,
{
    fn serialize(&self) -> Vec<u8> {
        let mut result = vec![];
        for (k, v) in self {
            result.append(&mut k.serialize());
            result.append(&mut v.serialize());
        }
        result
    }
    fn deserialize(bytes: &[u8]) -> Self {
        let segment_len = K::segment_len() + V::segment_len();
        let mut deserealized = std::collections::HashMap::new();
        let mut start = 0;
        let mut end = segment_len;
        while start < bytes.len() {
            let key_start = start;
            let key_end = key_start + K::segment_len();
            let value_start = key_end;
            let value_end = value_start + V::segment_len();
            let key = K::deserialize(&bytes[key_start..key_end]);
            let value = V::deserialize(&bytes[value_start..value_end]);
            deserealized.insert(key, value);
            start = end;
            end = start + segment_len;
        }
        deserealized
    }
}

#[cfg(test)]
mod hashmap_test_serialization {
    use super::*;
    #[test]
    fn serialize() {
        let map: HashMap<u64, u64> = [(0, 0), (1, 1), (2, 2)].into_iter().collect();
        let tested: HashMap<u64, u64> = HashMap::deserialize(&map.serialize());
        assert_eq!(tested, map);
    }
}

impl ByteRpr for u64 {
    fn serialize(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
    fn deserialize(bytes: &[u8]) -> Self {
        let mut buff: [u8; 8] = [0; 8];
        buff.copy_from_slice(bytes);
        u64::from_le_bytes(buff)
    }
}
impl FixedByteLen for u64 {
    fn segment_len() -> usize {
        8
    }
}

impl ByteRpr for u128 {
    fn serialize(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
    fn deserialize(bytes: &[u8]) -> Self {
        let mut buff: [u8; 16] = [0; 16];
        buff.copy_from_slice(bytes);
        u128::from_le_bytes(buff)
    }
}
impl FixedByteLen for u128 {
    fn segment_len() -> usize {
        16
    }
}
impl ByteRpr for f32 {
    fn serialize(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
    fn deserialize(bytes: &[u8]) -> Self {
        let mut buff: [u8; 4] = [0; 4];
        buff.copy_from_slice(bytes);
        f32::from_le_bytes(buff)
    }
}
impl FixedByteLen for f32 {
    fn segment_len() -> usize {
        4
    }
}

impl ByteRpr for () {
    fn serialize(&self) -> Vec<u8> {
        Vec::with_capacity(0)
    }
    fn deserialize(_: &[u8]) -> Self {}
}
impl FixedByteLen for () {
    fn segment_len() -> usize {
        0
    }
}
