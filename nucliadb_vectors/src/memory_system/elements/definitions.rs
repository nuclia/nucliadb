use std::collections::HashMap;

use serde::{Deserialize, Serialize};

pub const VECTORS_DIR: &str = "vectors";
pub const KEYS_DIR: &str = "keys";

pub trait ByteRpr {
    fn as_byte_rpr(&self) -> Vec<u8>;
    fn from_byte_rpr(bytes: &[u8]) -> Self;
}

pub trait Distance {
    fn cosine(i: &Self, j: &Self) -> f32;
}

pub mod hnsw_params {
    pub fn level_factor() -> f64 {
        1.0 / (m() as f64).ln()
    }
    pub const fn m_max() -> usize {
        30
    }
    pub const fn m() -> usize {
        30
    }
    pub const fn ef_construction() -> usize {
        100
    }
    pub const fn k_neighbours() -> usize {
        10
    }
}

#[derive(Copy, Clone, Serialize, Deserialize)]
pub enum LogField {
    VersionNumber = 0,
    EntryPoint,
    NoLayers,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct EntryPoint {
    pub node: Node,
    pub layer: u64,
}

impl From<(Node, usize)> for EntryPoint {
    fn from((node, layer): (Node, usize)) -> EntryPoint {
        EntryPoint {
            node,
            layer: layer as u64,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct FileSegment {
    pub start: u64,
    pub end: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Eq, Ord, Hash, Serialize, Deserialize)]
pub struct Node {
    pub key: FileSegment,
    pub vector: FileSegment,
}

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Edge {
    pub from: Node,
    pub to: Node,
    pub dist: f32,
}

#[derive(Clone, PartialEq, PartialOrd, Debug, Serialize, Deserialize)]
pub struct Vector {
    pub raw: Vec<f32>,
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

#[derive(Clone, Serialize, Deserialize)]
pub struct GraphLayer {
    pub cnx: HashMap<Node, HashMap<Node, Edge>>,
}

impl Default for GraphLayer {
    fn default() -> Self {
        GraphLayer::new()
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
    #[allow(unused)]
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
    pub fn some_node(&self) -> Option<Node> {
        self.cnx.keys().next().cloned()
    }
    pub fn is_empty(&self) -> bool {
        self.cnx.len() == 0
    }
}
#[derive(Clone, Serialize, Deserialize)]
pub struct GraphLog {
    pub version_number: u128,
    pub max_layer: u64,
    pub entry_point: Option<EntryPoint>,
}
