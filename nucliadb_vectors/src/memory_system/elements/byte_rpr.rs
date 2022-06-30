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

pub use std::collections::{BTreeMap, HashMap};

pub use nucliadb_byte_rpr::buff_byte_rpr::*;

use super::definitions::*;
use crate::memory_system::mmap_driver::*;

pub fn semi_mapped_consine_similarity(x: &[f32], y: Node, storage: &Storage) -> f32 {
    let f32_len = f32::segment_len() as u64;
    let mut sum = 0.;
    let mut dem_x = 0.;
    let mut dem_y = 0.;
    let mut y_cursor = y.vector.start + (u64::segment_len() as u64);
    for x_value in x.iter() {
        let y_i = FileSegment {
            start: y_cursor,
            end: y_cursor + f32_len,
        };
        let y_value = storage.read(y_i).map(f32::from_byte_rpr).unwrap();
        sum += x_value * y_value;
        dem_x += x_value * x_value;
        dem_y += y_value * y_value;
        y_cursor = y_i.end;
    }
    sum / (f32::sqrt(dem_x) * f32::sqrt(dem_y))
}

impl ByteRpr for LogField {
    fn as_byte_rpr(&self, buff: &mut dyn std::io::Write) -> usize {
        buff.write_all(&[*self as u8]).unwrap();
        buff.flush().unwrap();
        1
    }
    fn from_byte_rpr(bytes: &[u8]) -> Self {
        use LogField::*;
        match bytes[0] {
            0 => VersionNumber,
            1 => EntryPoint,
            2 => NoLayers,
            _ => panic!("Unknown LogField: {bytes:?}"),
        }
    }
}

impl FixedByteLen for LogField {
    fn segment_len() -> usize {
        1
    }
}
impl ByteRpr for EntryPoint {
    fn as_byte_rpr(&self, buff: &mut dyn std::io::Write) -> usize {
        self.node.as_byte_rpr(buff) + self.layer.as_byte_rpr(buff)
    }
    fn from_byte_rpr(bytes: &[u8]) -> Self {
        let node_start = 0;
        let node_end = node_start + Node::segment_len();
        let layer_start = node_end;
        let layer_end = layer_start + u64::segment_len();
        EntryPoint {
            node: Node::from_byte_rpr(&bytes[node_start..node_end]),
            layer: u64::from_byte_rpr(&bytes[layer_start..layer_end]),
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

impl ByteRpr for FileSegment {
    fn as_byte_rpr(&self, buff: &mut dyn std::io::Write) -> usize {
        self.start.as_byte_rpr(buff) + self.end.as_byte_rpr(buff)
    }
    fn from_byte_rpr(bytes: &[u8]) -> Self {
        let start_s = 0;
        let start_e = start_s + u64::segment_len();
        let end_s = start_e;
        let end_e = end_s + u64::segment_len();
        FileSegment {
            start: u64::from_byte_rpr(&bytes[start_s..start_e]),
            end: u64::from_byte_rpr(&bytes[end_s..end_e]),
        }
    }
}

impl FixedByteLen for FileSegment {
    fn segment_len() -> usize {
        2 * u64::segment_len()
    }
}

impl ByteRpr for Node {
    fn as_byte_rpr(&self, buff: &mut dyn std::io::Write) -> usize {
        self.vector.as_byte_rpr(buff)
    }
    fn from_byte_rpr(bytes: &[u8]) -> Self {
        let vector_start = 0;
        let vector_end = vector_start + FileSegment::segment_len();
        Node {
            vector: FileSegment::from_byte_rpr(&bytes[vector_start..vector_end]),
        }
    }
}
impl FixedByteLen for Node {
    fn segment_len() -> usize {
        FileSegment::segment_len()
    }
}

impl ByteRpr for Edge {
    fn as_byte_rpr(&self, buff: &mut dyn std::io::Write) -> usize {
        self.from.as_byte_rpr(buff) + self.to.as_byte_rpr(buff) + self.dist.as_byte_rpr(buff)
    }
    fn from_byte_rpr(bytes: &[u8]) -> Self {
        let from_start = 0;
        let from_end = from_start + Node::segment_len();
        let to_start = from_end;
        let to_end = to_start + Node::segment_len();
        let dist_start = to_end;
        let dist_end = dist_start + f32::segment_len();
        Edge {
            from: Node::from_byte_rpr(&bytes[from_start..from_end]),
            to: Node::from_byte_rpr(&bytes[to_start..to_end]),
            dist: f32::from_byte_rpr(&bytes[dist_start..dist_end]),
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

impl ByteRpr for Vector {
    fn as_byte_rpr(&self, buff: &mut dyn std::io::Write) -> usize {
        let len = self.raw.len() as u64;
        let body = &self.raw;
        len.as_byte_rpr(buff) + body.as_byte_rpr(buff)
    }
    fn from_byte_rpr(bytes: &[u8]) -> Self {
        let len_start = 0;
        let len_end = len_start + u64::segment_len();
        let len = u64::from_byte_rpr(&bytes[len_start..len_end]) as usize;
        let raw_start = len_end;
        let raw_end = raw_start + (len * f32::segment_len());
        Vector {
            raw: Vec::from_byte_rpr(&bytes[raw_start..raw_end]),
        }
    }
}

impl ByteRpr for GraphLayer {
    fn as_byte_rpr(&self, buff: &mut dyn std::io::Write) -> usize {
        self.cnx.iter().fold(0, |p, (k, v)| {
            let serialized_value = v.alloc_byte_rpr();
            let key_len = k.as_byte_rpr(buff);
            let value_len_len = (serialized_value.len() as u64).as_byte_rpr(buff);
            let value_len = serialized_value.as_byte_rpr(buff);
            p + key_len + value_len_len + value_len
        })
    }
    fn from_byte_rpr(bytes: &[u8]) -> Self {
        let mut cnx = HashMap::new();
        let mut segment_start = 0;
        while segment_start < bytes.len() {
            let key_start = segment_start;
            let key_end = key_start + Node::segment_len();
            let len_start = key_end;
            let len_end = len_start + u64::segment_len();
            let key = Node::from_byte_rpr(&bytes[key_start..key_end]);
            let hash_block = u64::from_byte_rpr(&bytes[len_start..len_end]) as usize;
            let edges_start = len_end;
            let edges_end = edges_start + hash_block;
            let edges = BTreeMap::from_byte_rpr(&bytes[edges_start..edges_end]);
            cnx.insert(key, edges);
            segment_start = edges_end;
        }
        GraphLayer { cnx }
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
        assert_eq!(Node::from_byte_rpr(&id_0.alloc_byte_rpr()), id_0);
        assert_eq!(ep.alloc_byte_rpr().len(), ep.as_byte_rpr(&mut vec![]));
        assert_eq!(ep.alloc_byte_rpr().len(), EntryPoint::segment_len());
        assert_eq!(ep, EntryPoint::from_byte_rpr(&ep.alloc_byte_rpr()));
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
                vector: FileSegment { start: i, end: i },
            };
            nodes.push(node);
        }
        nodes
    }
    #[test]
    fn serialize() {
        let node = test_nodes(1).pop().unwrap();
        assert_eq!(node.alloc_byte_rpr().len(), Node::segment_len());
        assert_eq!(node.alloc_byte_rpr().len(), node.as_byte_rpr(&mut vec![]));
        assert_eq!(Node::from_byte_rpr(&node.alloc_byte_rpr()), node);
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
        assert_eq!(edge.alloc_byte_rpr().len(), Edge::segment_len());
        assert_eq!(edge.alloc_byte_rpr().len(), edge.as_byte_rpr(&mut vec![]));
        assert_eq!(Edge::from_byte_rpr(&edge.alloc_byte_rpr()), edge);
    }
}

#[cfg(test)]
mod vector_test_serialization {
    use super::*;
    #[test]
    fn serialize() {
        let vector = Vector::from(vec![2.0; 3]);
        let serialized = Vector::from_byte_rpr(&vector.alloc_byte_rpr());
        assert_eq!(
            vector.alloc_byte_rpr().len(),
            vector.as_byte_rpr(&mut vec![])
        );
        assert_eq!(serialized.raw.len(), vector.raw.len());
        assert_eq!(serialized, vector);
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
        let tested = GraphLayer::from_byte_rpr(&graph.alloc_byte_rpr());
        assert_eq!(graph.alloc_byte_rpr().len(), graph.as_byte_rpr(&mut vec![]));
        assert_eq!(graph.no_edges(nodes[0]), tested.no_edges(nodes[0]));
        assert_eq!(graph.no_edges(nodes[1]), tested.no_edges(nodes[1]));
        assert_eq!(graph[(nodes[0], nodes[1])], tested[(nodes[0], nodes[1])]);
        assert_eq!(graph.cnx, tested.cnx);
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
        assert_eq!(fs[0].alloc_byte_rpr().len(), FileSegment::segment_len());
        assert_eq!(fs[0].alloc_byte_rpr().len(), fs[0].as_byte_rpr(&mut vec![]));
        assert_eq!(FileSegment::from_byte_rpr(&fs[0].alloc_byte_rpr()), fs[0]);
    }
}
