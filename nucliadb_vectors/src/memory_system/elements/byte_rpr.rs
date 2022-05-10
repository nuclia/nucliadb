pub use std::collections::HashMap;

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

pub trait FixedByteLen: ByteRpr {
    fn segment_len() -> usize;
}

impl ByteRpr for LogField {
    fn as_byte_rpr(&self) -> Vec<u8> {
        vec![*self as u8]
    }
    fn from_byte_rpr(bytes: &[u8]) -> Self {
        use LogField::*;
        match bytes[0] {
            0 => VersionNumber,
            1 => EntryPoint,
            2 => MaxLayer,
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
    fn as_byte_rpr(&self) -> Vec<u8> {
        let mut node = self.node.as_byte_rpr();
        let mut layer = self.layer.as_byte_rpr();
        let mut result = Vec::new();
        result.append(&mut node);
        result.append(&mut layer);
        result
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
    fn as_byte_rpr(&self) -> Vec<u8> {
        let mut result = vec![];
        result.append(&mut self.start.as_byte_rpr());
        result.append(&mut self.end.as_byte_rpr());
        result
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
    fn as_byte_rpr(&self) -> Vec<u8> {
        let mut result = Vec::new();
        result.append(&mut self.key.as_byte_rpr());
        result.append(&mut self.vector.as_byte_rpr());
        result
    }
    fn from_byte_rpr(bytes: &[u8]) -> Self {
        let key_start = 0;
        let key_end = key_start + FileSegment::segment_len();
        let vector_start = key_end;
        let vector_end = vector_start + FileSegment::segment_len();
        Node {
            key: FileSegment::from_byte_rpr(&bytes[key_start..key_end]),
            vector: FileSegment::from_byte_rpr(&bytes[vector_start..vector_end]),
        }
    }
}
impl FixedByteLen for Node {
    fn segment_len() -> usize {
        2 * FileSegment::segment_len()
    }
}

impl ByteRpr for Edge {
    fn as_byte_rpr(&self) -> Vec<u8> {
        let mut result = Vec::new();
        result.append(&mut self.from.as_byte_rpr());
        result.append(&mut self.to.as_byte_rpr());
        result.append(&mut self.dist.to_le_bytes().to_vec());
        result
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
    fn as_byte_rpr(&self) -> Vec<u8> {
        let mut result = vec![];
        let len = self.raw.len() as u64;
        let body = &self.raw;
        result.append(&mut len.as_byte_rpr());
        result.append(&mut body.as_byte_rpr());
        result
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
    fn as_byte_rpr(&self) -> Vec<u8> {
        let mut serialized = vec![];
        for (k, v) in &self.cnx {
            let mut serialized_key = k.as_byte_rpr();
            let mut serialized_value = v.as_byte_rpr();
            let mut len = (serialized_value.len() as u64).as_byte_rpr();
            serialized.append(&mut serialized_key);
            serialized.append(&mut len);
            serialized.append(&mut serialized_value);
        }
        serialized
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
            let edges = HashMap::from_byte_rpr(&bytes[edges_start..edges_end]);
            cnx.insert(key, edges);
            segment_start = edges_end;
        }
        GraphLayer { cnx }
    }
}
impl<T> ByteRpr for Option<T>
where T: ByteRpr + FixedByteLen
{
    fn as_byte_rpr(&self) -> Vec<u8> {
        let mut buff = vec![0];
        match self {
            Some(e) => {
                buff[0] = 1;
                buff.append(&mut e.as_byte_rpr());
            }
            None => {
                buff.append(&mut vec![0; T::segment_len()]);
            }
        }
        buff
    }
    fn from_byte_rpr(bytes: &[u8]) -> Self {
        match bytes[0] {
            1 => Some(T::from_byte_rpr(&bytes[1..])),
            0 => None,
            _ => panic!("Invalid byte pattern"),
        }
    }
}

impl<T> FixedByteLen for Option<T>
where T: ByteRpr + FixedByteLen
{
    fn segment_len() -> usize {
        T::segment_len() + 1
    }
}

impl<T> ByteRpr for Vec<T>
where T: ByteRpr + FixedByteLen
{
    fn as_byte_rpr(&self) -> Vec<u8> {
        let mut result = vec![];
        for elem in self {
            result.append(&mut elem.as_byte_rpr());
        }
        result
    }
    fn from_byte_rpr(bytes: &[u8]) -> Self {
        let segment_len = T::segment_len();
        let mut deserealized = vec![];
        let mut start = 0;
        let mut end = segment_len;
        while start < bytes.len() {
            deserealized.push(T::from_byte_rpr(&bytes[start..end]));
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
    fn as_byte_rpr(&self) -> Vec<u8> {
        let mut result = vec![];
        for (k, v) in self {
            result.append(&mut k.as_byte_rpr());
            result.append(&mut v.as_byte_rpr());
        }
        result
    }
    fn from_byte_rpr(bytes: &[u8]) -> Self {
        let segment_len = K::segment_len() + V::segment_len();
        let mut deserealized = HashMap::new();
        let mut start = 0;
        let mut end = segment_len;
        while start < bytes.len() {
            let key_start = start;
            let key_end = key_start + K::segment_len();
            let value_start = key_end;
            let value_end = value_start + V::segment_len();
            let key = K::from_byte_rpr(&bytes[key_start..key_end]);
            let value = V::from_byte_rpr(&bytes[value_start..value_end]);
            deserealized.insert(key, value);
            start = end;
            end = start + segment_len;
        }
        deserealized
    }
}

impl ByteRpr for u64 {
    fn as_byte_rpr(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
    fn from_byte_rpr(bytes: &[u8]) -> Self {
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
    fn as_byte_rpr(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
    fn from_byte_rpr(bytes: &[u8]) -> Self {
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
    fn as_byte_rpr(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
    fn from_byte_rpr(bytes: &[u8]) -> Self {
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
    fn as_byte_rpr(&self) -> Vec<u8> {
        Vec::with_capacity(0)
    }
    fn from_byte_rpr(_: &[u8]) -> Self {}
}
impl FixedByteLen for () {
    fn segment_len() -> usize {
        0
    }
}

impl ByteRpr for String {
    fn as_byte_rpr(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }
    fn from_byte_rpr(bytes: &[u8]) -> Self {
        String::from_utf8(bytes.to_vec()).unwrap()
    }
}

impl ByteRpr for Vec<u8> {
    fn as_byte_rpr(&self) -> Vec<u8> {
        self.clone()
    }
    fn from_byte_rpr(bytes: &[u8]) -> Self {
        bytes.to_vec()
    }
}

#[cfg(test)]
mod option_test_serialization {
    use super::*;
    #[test]
    fn serialize() {
        let elem = Some(0u64);
        let none_elem: Option<u64> = None;
        assert_eq!(Option::from_byte_rpr(&elem.as_byte_rpr()), Some(0u64));
        assert_eq!(Option::from_byte_rpr(&none_elem.as_byte_rpr()), none_elem);
        assert_eq!(elem.as_byte_rpr().len(), Option::<u64>::segment_len());
        assert_eq!(none_elem.as_byte_rpr().len(), Option::<u64>::segment_len());
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
        assert_eq!(Node::from_byte_rpr(&id_0.as_byte_rpr()), id_0);
        assert_eq!(ep.as_byte_rpr().len(), EntryPoint::segment_len());
        assert_eq!(ep, EntryPoint::from_byte_rpr(&ep.as_byte_rpr()));
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
        assert_eq!(node.as_byte_rpr().len(), Node::segment_len());
        assert_eq!(Node::from_byte_rpr(&node.as_byte_rpr()), node);
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
        assert_eq!(edge.as_byte_rpr().len(), Edge::segment_len());
        assert_eq!(Edge::from_byte_rpr(&edge.as_byte_rpr()), edge);
    }
}

#[cfg(test)]
mod vector_test_serialization {
    use super::*;
    #[test]
    fn serialize() {
        let vector = Vector::from(vec![2.0; 3]);
        let serialized = Vector::from_byte_rpr(&vector.as_byte_rpr());
        assert_eq!(serialized.raw.len(), vector.raw.len());
        assert_eq!(serialized, vector);
    }
}

#[cfg(test)]
mod vec_test_serialization {
    use super::*;
    #[test]
    fn serialize() {
        let vector: Vec<u64> = vec![12; 7];
        let tested: Vec<u64> = Vec::from_byte_rpr(&vector.as_byte_rpr());
        assert_eq!(tested, vector);
    }
}

#[cfg(test)]
mod hashmap_test_serialization {
    use super::*;
    #[test]
    fn serialize() {
        let map: HashMap<u64, u64> = [(0, 0), (1, 1), (2, 2)].into_iter().collect();
        let tested: HashMap<u64, u64> = HashMap::from_byte_rpr(&map.as_byte_rpr());
        assert_eq!(tested, map);
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
        let tested = GraphLayer::from_byte_rpr(&graph.as_byte_rpr());
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
        assert_eq!(fs[0].as_byte_rpr().len(), FileSegment::segment_len());
        assert_eq!(FileSegment::from_byte_rpr(&fs[0].as_byte_rpr()), fs[0]);
    }
}
