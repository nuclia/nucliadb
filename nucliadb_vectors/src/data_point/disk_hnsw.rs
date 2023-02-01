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

// Node:
// -> Layers segment.
// -> Indexing segment.
// \Indexing segment:
// Per layer in the hnsw in reverse order the start of
// its adjacency list.
// \Layer segment:
// -> N: number of connexions.
// -> List per connexion of T tuples (node, edge), where:
// -> node: usize, in little endian.
// -> edge: f32, in little endian.
// Hnsw:
// -> Node segment.
// -> Indexing segment.
// -> Entry point segment.
// \Entry point segment:
// -> Layer, usize in little endian.
// -> Node, usize in little endian.
// \Node segment (serialized as explained above).
// \Indexing segment:
// Per layer in the hnsw:
// -> The byte where it ends.
//
//

use std::collections::HashMap;
use std::io;

use super::ops_hnsw::{Hnsw, Layer};
use super::ram_hnsw::{Edge, EntryPoint, RAMHnsw};
use super::Address;
use crate::data_types::usize_utils::*;

const EDGE_LEN: usize = 4;
const NODE_LEN: usize = USIZE_LEN;
const CNX_LEN: usize = NODE_LEN + EDGE_LEN;

fn f32_from_le_bytes(buf: &[u8]) -> f32 {
    let mut temp = [0; 4];
    temp.copy_from_slice(buf);
    f32::from_le_bytes(temp)
}

pub struct DiskLayer<'a> {
    hnsw: &'a [u8],
    layer: usize,
}

impl<'a> Layer for &'a DiskLayer<'a> {
    type EdgeIt = EdgeIter<'a>;
    fn get_out_edges(&self, Address(node): Address) -> Self::EdgeIt {
        let node = DiskHnsw::get_node(self.hnsw, node);
        DiskHnsw::get_out_edges(node, self.layer)
    }
}

impl<'a> Layer for DiskLayer<'a> {
    type EdgeIt = EdgeIter<'a>;
    fn get_out_edges(&self, Address(node): Address) -> Self::EdgeIt {
        let node = DiskHnsw::get_node(self.hnsw, node);
        DiskHnsw::get_out_edges(node, self.layer)
    }
}

impl<'a> Hnsw for &'a [u8] {
    type L = DiskLayer<'a>;
    fn get_entry_point(&self) -> Option<EntryPoint> {
        DiskHnsw::get_entry_point(self)
    }
    fn get_layer(&self, i: usize) -> Self::L {
        DiskLayer {
            hnsw: self,
            layer: i,
        }
    }
}

pub struct EdgeIter<'a> {
    crnt: usize,
    buf: &'a [u8],
}
impl<'a> Iterator for EdgeIter<'a> {
    type Item = (Address, Edge);
    fn next(&mut self) -> Option<Self::Item> {
        if self.buf.len() == self.crnt {
            None
        } else {
            let buf = self.buf;
            let mut crnt = self.crnt;
            let node = usize_from_slice_le(&buf[crnt..(crnt + NODE_LEN)]);
            crnt += USIZE_LEN;
            let edge = f32_from_le_bytes(&buf[crnt..(crnt + EDGE_LEN)]);
            crnt += EDGE_LEN;
            self.crnt = crnt;
            Some((Address(node), Edge { dist: edge }))
        }
    }
}

pub struct DiskHnsw;
impl DiskHnsw {
    fn serialize_node<W>(
        mut buf: W,
        offset: usize,
        node: usize,
        hnsw: &RAMHnsw,
    ) -> io::Result<usize>
    where
        W: io::Write,
    {
        let node = Address(node);
        let mut length = offset;
        let mut indexing = HashMap::new();
        for layer in 0..hnsw.no_layers() {
            let no_edges = hnsw.get_layer(layer).no_out_edges(node);
            indexing.insert(layer, length);
            buf.write_all(&no_edges.to_le_bytes())?;
            length += USIZE_LEN;
            for (cnx, edge) in hnsw.get_layer(layer).get_out_edges(node) {
                buf.write_all(&cnx.0.to_le_bytes())?;
                buf.write_all(&edge.dist.to_le_bytes())?;
                length += CNX_LEN;
            }
        }
        for layer in (0..hnsw.no_layers()).rev() {
            buf.write_all(&indexing[&layer].to_le_bytes())?;
        }
        length += hnsw.no_layers() * USIZE_LEN;
        buf.flush()?;
        Ok(length)
    }

    // node must be serialized using DiskNode, may have trailing bytes at the start.
    fn get_out_edges(node: &[u8], layer: usize) -> EdgeIter {
        // layer + 1 since the layers are stored in reverse order.
        // [l3, l2, l1, l0, end] Since we have the position of end, the layer i is
        // i + 1 positions to its left.
        let pos = node.len() - ((layer + 1) * USIZE_LEN);
        let cnx_start = usize_from_slice_le(&node[pos..(pos + USIZE_LEN)]);
        let no_cnx = usize_from_slice_le(&node[cnx_start..(cnx_start + USIZE_LEN)]);
        let cnx_start = cnx_start + USIZE_LEN;
        let cnx_end = cnx_start + (no_cnx * CNX_LEN);
        EdgeIter {
            crnt: 0,
            buf: &node[cnx_start..cnx_end],
        }
    }
    pub fn serialize_into<W: io::Write>(
        mut buf: W,
        no_nodes: usize,
        hnsw: RAMHnsw,
    ) -> io::Result<()> {
        if let Some(entry_point) = hnsw.entry_point {
            let mut length = 0;
            let mut nodes_end = vec![];
            for node in 0..no_nodes {
                length = DiskHnsw::serialize_node(&mut buf, length, node, &hnsw)?;
                nodes_end.push(length)
            }
            for ends_at in nodes_end.into_iter().rev() {
                buf.write_all(&ends_at.to_le_bytes())?;
                length += USIZE_LEN;
            }
            let EntryPoint { node, layer } = entry_point;
            buf.write_all(&layer.to_le_bytes())?;
            buf.write_all(&node.0.to_le_bytes())?;
            let _length = length + 2 * USIZE_LEN;
            buf.flush()?;
        }
        Ok(())
    }
    // hnsw must be serialized using DiskHnsw, may have trailing bytes at the start.
    pub fn get_entry_point(hnsw: &[u8]) -> Option<EntryPoint> {
        if !hnsw.is_empty() {
            let node_start = hnsw.len() - USIZE_LEN;
            let layer_start = node_start - USIZE_LEN;
            let node = usize_from_slice_le(&hnsw[node_start..(node_start + USIZE_LEN)]);
            let layer = usize_from_slice_le(&hnsw[layer_start..(layer_start + USIZE_LEN)]);
            Some(EntryPoint {
                node: Address(node),
                layer,
            })
        } else {
            None
        }
    }
    // hnsw must be serialized using MHnsw, may have trailing bytes at the start.
    // The returned node will have trailing bytes at the start.
    pub fn get_node(hnsw: &[u8], node: usize) -> &[u8] {
        let indexing_end = hnsw.len() - (2 * USIZE_LEN);
        // node + 1 since the layers are stored in reverse order.
        // [n3, n2, n1, n0, end] Since we have the position of end, the node i is
        // i + 1 positions to its left.
        let pos = indexing_end - ((node + 1) * USIZE_LEN);
        let node_end = usize_from_slice_le(&hnsw[pos..(pos + USIZE_LEN)]);
        &hnsw[..node_end]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_point::ram_hnsw::RAMLayer;
    fn layer_check<L: Layer>(buf: L, no_nodes: usize, cnx: &[Vec<(Address, Edge)>]) {
        let no_cnx = vec![];
        for i in 0..no_nodes {
            let expected = cnx.get(i).unwrap_or(&no_cnx);
            let got: Vec<_> = buf.get_out_edges(Address(i)).collect();
            assert_eq!(expected, &got);
        }
    }
    #[test]
    fn empty_hnsw() {
        let hnsw = RAMHnsw {
            entry_point: None,
            layers: vec![],
        };
        let mut buf = vec![];
        DiskHnsw::serialize_into(&mut buf, 0, hnsw).unwrap();
        let ep = DiskHnsw::get_entry_point(&buf);
        assert_eq!(ep, None);
    }

    #[test]
    fn hnsw_test() {
        let no_nodes = 3;
        let cnx0 = vec![
            vec![(Address(1), Edge { dist: 1.0 })],
            vec![(Address(2), Edge { dist: 2.0 })],
            vec![(Address(3), Edge { dist: 3.0 })],
        ];
        let layer0 = RAMLayer {
            out: cnx0
                .iter()
                .enumerate()
                .map(|(i, c)| (Address(i), c.clone()))
                .collect(),
        };
        let cnx1 = vec![
            vec![(Address(1), Edge { dist: 4.0 })],
            vec![(Address(2), Edge { dist: 5.0 })],
        ];
        let layer1 = RAMLayer {
            out: cnx1
                .iter()
                .enumerate()
                .map(|(i, c)| (Address(i), c.clone()))
                .collect(),
        };
        let cnx2 = vec![vec![(Address(1), Edge { dist: 6.0 })]];
        let layer2 = RAMLayer {
            out: cnx2
                .iter()
                .enumerate()
                .map(|(i, c)| (Address(i), c.clone()))
                .collect(),
        };
        let entry_point = EntryPoint {
            node: Address(0),
            layer: 2,
        };
        let hnsw = RAMHnsw {
            entry_point: Some(entry_point),
            layers: vec![layer0, layer1, layer2],
        };
        let mut buf = vec![];
        DiskHnsw::serialize_into(&mut buf, no_nodes, hnsw).unwrap();
        let ep = DiskHnsw::get_entry_point(&buf).unwrap();
        assert_eq!(ep, entry_point);
        let layer0 = buf.as_slice().get_layer(0);
        layer_check(&layer0, no_nodes, &cnx0);
        let layer1 = buf.as_slice().get_layer(1);
        layer_check(&layer1, no_nodes, &cnx1);
        let layer2 = buf.as_slice().get_layer(2);
        layer_check(&layer2, no_nodes, &cnx2);
    }
}
