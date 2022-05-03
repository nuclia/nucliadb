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
use crate::memory_system::mmap_driver::*;
use std::collections::HashMap;

pub const VECTORS_DIR: &str = "vectors";
pub const KEYS_DIR: &str = "keys";

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

pub mod hnsw_params {
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
    pub const fn vector_length() -> usize {
        178
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct EntryPoint {
    pub node: Node,
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
        let node_end = node_start + Node::segment_len();
        let layer_start = node_end;
        let layer_end = layer_start + u64::segment_len();
        EntryPoint {
            node: Node::deserialize(&bytes[node_start..node_end]),
            layer: u64::deserialize(&bytes[layer_start..layer_end]),
        }
    }
}

impl FixedByteLen for EntryPoint {
    fn segment_len() -> usize {
        let node_id_len = Node::segment_len();
        let layer_len = u64::segment_len();
        node_id_len + layer_len
    }
}
impl From<(Node, usize)> for EntryPoint {
    fn from((node, layer): (Node, usize)) -> EntryPoint {
        EntryPoint {
            node,
            layer: layer as u64,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
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

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Eq, Ord, Hash)]
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

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd)]
pub struct Edge {
    pub from: Node,
    pub to: Node,
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
        let from_end = from_start + Node::segment_len();
        let to_start = from_end;
        let to_end = to_start + Node::segment_len();
        let dist_start = to_end;
        let dist_end = dist_start + f32::segment_len();
        Edge {
            from: Node::deserialize(&bytes[from_start..from_end]),
            to: Node::deserialize(&bytes[to_start..to_end]),
            dist: f32::deserialize(&bytes[dist_start..dist_end]),
        }
    }
}

impl FixedByteLen for Edge {
    fn segment_len() -> usize {
        let node_id_len = Node::segment_len();
        let f32_len = f32::segment_len();
        (2 * node_id_len) + f32_len
    }
}

#[derive(Clone, PartialEq, PartialOrd, Debug)]
pub struct Vector {
    pub raw: Vec<f32>,
}

impl From<Vec<f32>> for Vector {
    fn from(mut raw: Vec<f32>) -> Self {
        while raw.len() < hnsw_params::vector_length() {
            raw.push(0.0);
        }
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
        let raw_start = 0;
        let raw_end = raw_start + (hnsw_params::vector_length() * f32::segment_len());
        Vector {
            raw: Vec::deserialize(&bytes[raw_start..raw_end]),
        }
    }
}
impl FixedByteLen for Vector {
    fn segment_len() -> usize {
        hnsw_params::vector_length() * f32::segment_len()
    }
}

pub fn semi_mapped_consine_similarity(x: &[f32], y: Node, storage: &Storage) -> f32 {
    let f32_len = f32::segment_len() as u64;
    let mut sum = 0.;
    let mut dem_x = 0.;
    let mut dem_y = 0.;
    let mut y_cursor = y.vector.start;
    for x_value in x.iter().take(hnsw_params::vector_length()) {
        let y_i = FileSegment {
            start: y_cursor,
            end: y_cursor + f32_len,
        };
        let y_value = storage.read(y_i).map(f32::deserialize).unwrap();
        sum += x_value * y_value;
        dem_x += x_value * x_value;
        dem_y += y_value * y_value;
        y_cursor = y_i.end;
    }
    sum / (f32::sqrt(dem_x) * f32::sqrt(dem_y))
}

#[derive(Clone)]
pub struct GraphLayer {
    pub cnx: HashMap<Node, HashMap<Node, Edge>>,
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
            let mut serialized_value = v.serialize();
            let mut len = (serialized_value.len() as u64).serialize();
            serialized.append(&mut serialized_key);
            serialized.append(&mut len);
            serialized.append(&mut serialized_value);
        }
        serialized
    }
    fn deserialize(bytes: &[u8]) -> Self {
        let mut cnx = HashMap::new();
        let mut segment_start = 0;
        while segment_start < bytes.len() {
            let key_start = segment_start;
            let key_end = key_start + Node::segment_len();
            let len_start = key_end;
            let len_end = len_start + u64::segment_len();
            let key = Node::deserialize(&bytes[key_start..key_end]);
            let hash_block = u64::deserialize(&bytes[len_start..len_end]) as usize;
            let edges_start = len_end;
            let edges_end = edges_start + hash_block;
            let edges = HashMap::deserialize(&bytes[edges_start..edges_end]);
            cnx.insert(key, edges);
            segment_start = edges_end;
        }
        GraphLayer { cnx }
    }
}
impl std::ops::Index<(Node, Node)> for GraphLayer {
    type Output = Edge;
    fn index(&self, (from, to): (Node, Node)) -> &Self::Output {
        &self.cnx[&from][&to]
    }
}

impl GraphLayer {
    pub fn new() -> GraphLayer {
        GraphLayer {
            cnx: HashMap::new(),
        }
    }
    pub fn has_node(&self, node: Node) -> bool {
        self.cnx.contains_key(&node)
    }
    pub fn add_node(&mut self, node: Node) {
        self.cnx.insert(node, HashMap::new());
    }
    pub fn add_edge(&mut self, node: Node, edge: Edge) {
        let edges = self.cnx.entry(node).or_insert_with(HashMap::new);
        edges.insert(edge.to, edge);
    }
    pub fn remove_node(&mut self, node: Node) {
        self.cnx.remove(&node);
    }
    pub fn get_edges(&self, from: Node) -> HashMap<Node, Edge> {
        self.cnx[&from].clone()
    }
    #[cfg(test)]
    pub fn no_edges(&self, node: Node) -> Option<usize> {
        self.cnx.get(&node).map(|v| v.len())
    }
    pub fn no_nodes(&self) -> usize {
        self.cnx.len()
    }
    pub fn remove_edge(&mut self, from: Node, to: Node) {
        let edges = self.cnx.get_mut(&from).unwrap();
        edges.remove(&to);
    }
    pub fn some_edge(&self) -> Option<Node> {
        self.cnx.keys().next().cloned()
    }
    pub fn is_empty(&self) -> bool {
        self.cnx.len() == 0
    }
}

pub struct GraphLog {
    pub version_number: u128,
    pub entry_point: Option<EntryPoint>,
}

impl<T> ByteRpr for Option<T>
where
    T: ByteRpr + FixedByteLen,
{
    fn serialize(&self) -> Vec<u8> {
        let mut buff = vec![0];
        match self {
            Some(e) => {
                buff[0] = 1;
                buff.append(&mut e.serialize());
            }
            None => {
                buff.append(&mut vec![0; T::segment_len()]);
            }
        }
        buff
    }
    fn deserialize(bytes: &[u8]) -> Self {
        match bytes[0] {
            1 => Some(T::deserialize(&bytes[1..])),
            0 => None,
            _ => panic!("Invalid byte pattern"),
        }
    }
}

impl<T> FixedByteLen for Option<T>
where
    T: ByteRpr + FixedByteLen,
{
    fn segment_len() -> usize {
        T::segment_len() + 1
    }
}

impl<T> ByteRpr for Vec<T>
where
    T: ByteRpr + FixedByteLen,
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
        let mut deserealized = HashMap::new();
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

impl ByteRpr for String {
    fn serialize(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }
    fn deserialize(bytes: &[u8]) -> Self {
        String::from_utf8(bytes.to_vec()).unwrap()
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
    pub fn test_layer(len: usize) -> (Vec<Node>, Vec<GraphLayer>) {
        let mut layers = Vec::with_capacity(len);
        let nodes = node_test_serialization::test_nodes(2);
        for _ in 0..len {
            let edge = Edge {
                from: nodes[0],
                to: nodes[1],
                dist: 1.2,
            };
            let mut layer = GraphLayer {
                cnx: HashMap::new(),
            };
            layer.add_node(nodes[0]);
            layer.add_node(nodes[1]);
            layer.add_edge(nodes[0], edge);
            layers.push(layer);
        }
        (nodes, layers)
    }
    #[test]
    fn serialize() {
        let (nodes, mut graph) = test_layer(1);
        let graph = graph.pop().unwrap();
        let tested = GraphLayer::deserialize(&graph.serialize());
        assert_eq!(graph.no_edges(nodes[0]), tested.no_edges(nodes[0]));
        assert_eq!(graph.no_edges(nodes[1]), tested.no_edges(nodes[1]));
        assert_eq!(graph[(nodes[0], nodes[1])], tested[(nodes[0], nodes[1])]);
    }
}

#[cfg(test)]
mod file_segment_test_serialization {
    use super::*;
    pub fn test_segments(len: usize) -> Vec<FileSegment> {
        let mut segments = Vec::with_capacity(len);
        for i in 0..len {
            segments.push(FileSegment {
                start: i as u64,
                end: i as u64,
            });
        }
        segments
    }
    #[test]
    fn serialize() {
        let fs = test_segments(1);
        assert_eq!(fs[0].serialize().len(), FileSegment::segment_len());
        assert_eq!(FileSegment::deserialize(&fs[0].serialize()), fs[0]);
    }
}

#[cfg(test)]
mod option_test_serialization {
    use super::*;
    #[test]
    fn serialize() {
        let elem = Some(0u64);
        let none_elem: Option<u64> = None;
        assert_eq!(Option::deserialize(&elem.serialize()), Some(0u64));
        assert_eq!(Option::deserialize(&none_elem.serialize()), none_elem);
        assert_eq!(elem.serialize().len(), Option::<u64>::segment_len());
        assert_eq!(none_elem.serialize().len(), Option::<u64>::segment_len());
    }
}

#[cfg(test)]
mod entry_point_test_serialization {
    use super::*;
    #[test]
    fn serialize() {
        let id_0 = node_test_serialization::test_nodes(1).pop().unwrap();
        let ep = EntryPoint {
            node: id_0,
            layer: 0,
        };
        assert_eq!(Node::deserialize(&id_0.serialize()), id_0);
        assert_eq!(ep.serialize().len(), EntryPoint::segment_len());
        assert_eq!(ep, EntryPoint::deserialize(&ep.serialize()));
    }
}

#[cfg(test)]
mod node_test_serialization {
    use super::*;
    pub fn test_nodes(len: usize) -> Vec<Node> {
        let mut nodes = vec![];
        for i in 0..len {
            let i = i as u64;
            let node = Node {
                key: FileSegment { start: i, end: i },
                vector: FileSegment { start: i, end: i },
            };
            nodes.push(node);
        }
        nodes
    }
    #[test]
    fn serialize() {
        let node = test_nodes(1).pop().unwrap();
        assert_eq!(node.serialize().len(), Node::segment_len());
        assert_eq!(Node::deserialize(&node.serialize()), node);
    }
}

#[cfg(test)]
mod edge_test_serialization {
    use super::*;
    #[test]
    fn serialize() {
        let nodes = node_test_serialization::test_nodes(2);
        let edge = Edge {
            from: nodes[0],
            to: nodes[1],
            dist: 1.2,
        };
        assert_eq!(edge.serialize().len(), Edge::segment_len());
        assert_eq!(Edge::deserialize(&edge.serialize()), edge);
    }
}

#[cfg(test)]
mod vector_test_serialization {
    use super::*;
    #[test]
    fn serialize() {
        let vector = Vector::from(vec![2.0; 3]);
        assert_eq!(Vector::deserialize(&vector.serialize()), vector);
        assert_eq!(vector.serialize().len(), Vector::segment_len());
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
