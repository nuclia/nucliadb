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

use std::collections::{BTreeMap, HashMap};
use std::io::{Read, Write};

use memmap2::Mmap;
use serde::{Deserialize, Serialize};

const INITIAL_CAPACITY: usize = 1000;
const OCCUPANCY_MIN: usize = 70;
const OCCUPANCY_MAX: usize = 90;
const OCCUPANCY_MID: usize = 80;

const fn should_increase(len: usize, cap: usize) -> bool {
    let occupancy = (len / cap) * 100;
    occupancy > OCCUPANCY_MAX
}
const fn should_decrease(len: usize, cap: usize) -> bool {
    let occupancy = (len / cap) * 100;
    cap > INITIAL_CAPACITY && occupancy < OCCUPANCY_MIN
}

const fn resize_by(len: usize, cap: usize) -> i64 {
    let len = len as i64;
    let cap = cap as i64;
    let ocup = OCCUPANCY_MID as i64;
    ((100 * len) - (ocup * cap)) / ocup
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
pub struct SegmentSlice {
    pub start: u64,
    pub end: u64,
}

pub struct MappedSegment {
    pub segment: Mmap,
}

impl MappedSegment {
    pub fn get_inserted(&self, segment: SegmentSlice) -> Option<&[u8]> {
        let range = (segment.start as usize)..(segment.end as usize);
        self.segment.get(range)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Eq, Ord, Hash, Serialize, Deserialize)]
pub struct Node {
    pub segment: u64,
    pub vector: SegmentSlice,
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

impl Vector {
    pub fn mapped_consine_similarity(mut x: impl Read, mut y: impl Read) -> f32 {
        let mut buff_x = [0; 8];
        let mut buff_y = [0; 8];
        x.read_exact(&mut buff_x).unwrap();
        y.read_exact(&mut buff_y).unwrap();
        let len_x = Vector::decode_length(buff_x);
        let len_y = Vector::decode_length(buff_y);
        assert_eq!(len_x, len_y);
        let len = len_x;
        let mut buff_x = [0; 4];
        let mut buff_y = [0; 4];
        let mut sum = 0.;
        let mut dem_x = 0.;
        let mut dem_y = 0.;
        for _ in 0..len {
            x.read_exact(&mut buff_x).unwrap();
            y.read_exact(&mut buff_y).unwrap();
            let x_value = Vector::decode_unit(buff_x);
            let y_value = Vector::decode_unit(buff_y);
            sum += x_value * y_value;
            dem_x += x_value * x_value;
            dem_y += y_value * y_value;
        }
        sum / (f32::sqrt(dem_x) * f32::sqrt(dem_y))
    }
    fn encode_unit(u: f32) -> [u8; 4] {
        u.to_le_bytes()
    }
    fn decode_unit(u: [u8; 4]) -> f32 {
        f32::from_le_bytes(u)
    }
    fn encode_length(l: usize) -> [u8; 8] {
        (l as u64).to_le_bytes()
    }
    fn decode_length(l: [u8; 8]) -> usize {
        u64::from_le_bytes(l) as usize
    }
    pub fn write_into(&self, mut w: impl Write) {
        w.write_all(&Vector::encode_length(self.raw.len())).unwrap();
        self.raw
            .iter()
            .cloned()
            .map(Vector::encode_unit)
            .for_each(|b| w.write_all(&b).unwrap());
    }
    pub fn read_from(mut r: impl Read) -> Vector {
        let mut len = [0; 8];
        let mut unit = [0; 4];
        r.read_exact(&mut len).unwrap();
        let len = Vector::decode_length(len);
        let raw: Vec<_> = (0..len)
            .into_iter()
            .map(|_| {
                r.read_exact(&mut unit).unwrap();
                Vector::decode_unit(unit)
            })
            .collect();
        Vector { raw }
    }
}

#[cfg(test)]
mod test_vectors {
    use super::Vector;
    #[test]
    pub fn read_write() {
        let inserted = Vector {
            raw: vec![rand::random(); 13],
        };
        let mut writer = Vec::new();
        inserted.write_into(&mut writer);
        let slice = writer.as_slice();
        let readed0 = Vector::read_from(slice);
        let readed1 = Vector::read_from(slice);
        let readed2 = Vector::read_from(slice);
        assert_eq!(inserted, readed0);
        assert_eq!(inserted, readed1);
        assert_eq!(inserted, readed2);
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct GraphLayer {
    pub cnx: HashMap<Node, BTreeMap<Node, Edge>>,
}

impl Default for GraphLayer {
    fn default() -> Self {
        GraphLayer::new()
    }
}

impl std::ops::Index<Node> for GraphLayer {
    type Output = BTreeMap<Node, Edge>;
    fn index(&self, from: Node) -> &Self::Output {
        &self.cnx[&from]
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
            cnx: HashMap::with_capacity(INITIAL_CAPACITY),
        }
    }
    pub fn has_node(&self, node: Node) -> bool {
        self.cnx.contains_key(&node)
    }
    pub fn add_node(&mut self, node: Node) {
        self.cnx.insert(node, BTreeMap::new());
        self.increase_policy();
    }
    pub fn add_edge(&mut self, node: Node, edge: Edge) {
        let edges = self.cnx.entry(node).or_insert_with(BTreeMap::new);
        edges.insert(edge.to, edge);
    }
    pub fn remove_node(&mut self, node: Node) {
        self.cnx.remove(&node);
        self.decrease_policy();
    }
    pub fn get_edges(&self, from: Node) -> HashMap<Node, Edge> {
        self.cnx[&from].clone().into_iter().collect()
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
    fn increase_policy(&mut self) {
        if let Some(factor) = self.check_policy(should_increase) {
            self.cnx.reserve(factor as usize);
        }
    }
    fn decrease_policy(&mut self) {
        if let Some(factor) = self.check_policy(should_decrease) {
            let cap = ((self.cnx.capacity() as i64) + factor) as usize;
            self.cnx.shrink_to(cap);
        }
    }
    fn check_policy(&self, policy: fn(_: usize, _: usize) -> bool) -> Option<i64> {
        let len = self.cnx.len();
        let cap = self.cnx.capacity();
        if policy(self.cnx.len(), self.cnx.capacity()) {
            Some(resize_by(len, cap))
        } else {
            None
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Log {
    pub fresh_segment: u64,
    pub max_layer: u64,
    pub entry_point: Option<EntryPoint>,
}
