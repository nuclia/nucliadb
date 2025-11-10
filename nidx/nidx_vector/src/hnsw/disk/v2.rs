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
// -> node: u32, in little endian.
// Hnsw:
// -> Node segment.
// -> Indexing segment.
// -> Entry point segment.
// \Entry point segment:
// -> Layer, u32 in little endian.
// -> Node, u32 in little endian.
// \Node segment (serialized as explained above).
// \Indexing segment:
// Per layer in the hnsw:
// -> The byte where it ends.
//
// Edge file (only for deserializing):
// - f32 per edge, in the same order they appear on the main index

use std::collections::HashMap;
use std::io;

use crate::VectorAddr;
use crate::data_types::usize_utils::*;
use crate::hnsw::ram_hnsw::{EntryPoint, RAMHnsw, RAMLayer};
use crate::hnsw::search::{SearchableHnsw, SearchableLayer};

pub struct DiskLayer<'a> {
    hnsw: &'a [u8],
    layer: usize,
}

impl<'a> SearchableLayer for DiskLayer<'a> {
    fn get_out_edges(&self, address: VectorAddr) -> impl Iterator<Item = VectorAddr> {
        let node = DiskHnsw::get_node(self.hnsw, address);
        DiskHnsw::get_out_edges(node, self.layer)
    }
}

impl<'a> SearchableHnsw for DiskHnsw<'a> {
    type L = DiskLayer<'a>;
    fn get_entry_point(&self) -> EntryPoint {
        self.get_entry_point()
    }
    fn get_layer(&self, i: usize) -> Self::L {
        DiskLayer { hnsw: self.0, layer: i }
    }
}

pub struct EdgeIter<'a> {
    pos: usize,
    buf: &'a [u8],
}
impl Iterator for EdgeIter<'_> {
    type Item = VectorAddr;
    fn next(&mut self) -> Option<Self::Item> {
        if self.buf.len() == self.pos {
            None
        } else {
            let node_addr = u32_from_slice_le(&self.buf[self.pos..(self.pos + U32_LEN)]);
            self.pos += 4;
            Some(VectorAddr(node_addr))
        }
    }
}

pub struct DiskHnsw<'a>(pub &'a [u8]);
impl<'a> DiskHnsw<'a> {
    fn serialize_node<W1, W2>(mut graph: W1, mut edge_file: W2, node_addr: u32, hnsw: &RAMHnsw) -> io::Result<usize>
    where
        W1: io::Write,
        W2: io::Write,
    {
        let node = VectorAddr(node_addr);
        let mut indexing = HashMap::new();
        let mut pos = 0;

        for layer in 0..hnsw.num_layers() {
            let num_edges = hnsw.get_layer(layer).num_out_edges(&node);
            indexing.insert(layer, pos);
            graph.write_all(&(num_edges as u32).to_le_bytes())?;

            if num_edges > 0 {
                for (cnx, edge) in hnsw.get_layer(layer).out[&node].read().unwrap().iter() {
                    let to: u32 = cnx.0;
                    graph.write_all(&to.to_le_bytes())?;
                    edge_file.write_all(&edge.to_le_bytes())?;
                }
            }

            pos += (1 + num_edges) * U32_LEN;
        }

        // Positions to the edgelist of each layer, relative to the end of the node
        pos += hnsw.num_layers() * U32_LEN;
        for layer in (0..hnsw.num_layers()).rev() {
            let pos: u32 = (pos - indexing[&layer]) as u32;
            graph.write_all(&pos.to_le_bytes())?;
        }

        Ok(pos)
    }

    // node must be serialized using DiskNode, may have trailing bytes at the start.
    fn get_out_edges(node: &[u8], layer: usize) -> EdgeIter<'_> {
        // layer + 1 since the layers are stored in reverse order.
        // [l3, l2, l1, l0, end] Since we have the position of end, the layer i is
        // i + 1 positions to its left.
        let pos = node.len() - ((layer + 1) * U32_LEN);
        let cnx_offset = u32_from_slice_le(&node[pos..(pos + U32_LEN)]) as usize;
        let cnx_start = node.len() - cnx_offset;
        let num_cnx = u32_from_slice_le(&node[cnx_start..(cnx_start + U32_LEN)]) as usize;

        let cnx_start = cnx_start + U32_LEN;
        let cnx_end = cnx_start + (num_cnx * U32_LEN);
        EdgeIter {
            pos: 0,
            buf: &node[cnx_start..cnx_end],
        }
    }

    pub fn serialize_into<W1: io::Write, W2: io::Write>(
        mut graph: W1,
        mut edge_file: W2,
        num_nodes: u32,
        hnsw: RAMHnsw,
    ) -> io::Result<()> {
        if num_nodes == 0 {
            // Empty graph, nothing to serialize
            return Ok(());
        }

        let mut nodes_end = vec![];
        let mut pos = 0;
        for node in 0..num_nodes {
            pos += DiskHnsw::serialize_node(&mut graph, &mut edge_file, node, &hnsw)?;
            nodes_end.push(pos);
        }

        for ends_at in nodes_end.into_iter().rev() {
            let ends_at: u32 = ends_at as u32;
            graph.write_all(&ends_at.to_le_bytes())?;
        }

        let EntryPoint { node, layer } = hnsw.entry_point;
        let node: u32 = node.0;
        let layer: u32 = layer as u32;

        graph.write_all(&layer.to_le_bytes())?;
        graph.write_all(&node.to_le_bytes())?;

        graph.flush()?;
        edge_file.flush()?;

        Ok(())
    }

    // hnsw must be serialized using DiskHnswV2, may have trailing bytes at the start.
    pub fn get_entry_point(&self) -> EntryPoint {
        assert!(!self.0.is_empty());

        let node_start = self.0.len() - U32_LEN;
        let layer_start = node_start - U32_LEN;
        let node_addr = u32_from_slice_le(&self.0[node_start..(node_start + U32_LEN)]);
        let layer = u32_from_slice_le(&self.0[layer_start..(layer_start + U32_LEN)]) as usize;
        EntryPoint {
            node: VectorAddr(node_addr),
            layer,
        }
    }

    // hnsw must be serialized using MHnsw, may have trailing bytes at the start.
    // The returned node will have trailing bytes at the start.
    pub fn get_node(hnsw: &[u8], address: VectorAddr) -> &[u8] {
        let indexing_end = hnsw.len() - (2 * U32_LEN);
        // node + 1 since the layers are stored in reverse order.
        // [n3, n2, n1, n0, end] Since we have the position of end, the node i is
        // i + 1 positions to its left.
        let pos = indexing_end - ((address.0 as usize + 1) * U32_LEN);
        let node_end = u32_from_slice_le(&hnsw[pos..(pos + U32_LEN)]) as usize;
        &hnsw[..node_end]
    }

    pub fn deserialize(hnsw: &[u8], mut edge_file: impl io::Read) -> std::io::Result<RAMHnsw> {
        let mut ram = RAMHnsw::new();
        if hnsw.is_empty() {
            return Ok(ram);
        }

        let end = hnsw.len();
        ram.entry_point = DiskHnsw(hnsw).get_entry_point();

        let mut node_index: u32 = 0;
        loop {
            let indexing_pos = end - (node_index as usize + 3) * U32_LEN;
            let node_end = u32_from_slice_le(&hnsw[indexing_pos..indexing_pos + U32_LEN]) as usize;
            let mut layer_index = 0;
            loop {
                if ram.layers.len() == layer_index {
                    ram.layers.push(RAMLayer::default());
                }

                let layer_pos = node_end - (layer_index + 1) * U32_LEN;
                let edges_offset = u32_from_slice_le(&hnsw[layer_pos..layer_pos + U32_LEN]) as usize;
                let edges_start = node_end - edges_offset;

                let number_edges = u32_from_slice_le(&hnsw[edges_start..edges_start + U32_LEN]) as usize;

                let cnx_start = edges_start + U32_LEN;
                let cnx_end = cnx_start + number_edges * U32_LEN;

                if number_edges > 0 {
                    let mut ram_edges = ram.layers[layer_index]
                        .out
                        .entry(VectorAddr(node_index))
                        .or_default()
                        .write()
                        .unwrap();
                    let edges = EdgeIter {
                        pos: 0,
                        buf: &hnsw[cnx_start..cnx_end],
                    };
                    for to in edges {
                        let mut buf = [0u8; 4];
                        edge_file.read_exact(&mut buf)?;
                        let edge = f32::from_le_bytes(buf);

                        ram_edges.push((to, edge));
                    }
                }

                if cnx_end == layer_pos {
                    break;
                };
                layer_index += 1;
            }

            if node_end == indexing_pos {
                break;
            }
            node_index += 1;
        }

        Ok(ram)
    }
}

#[cfg(test)]
mod tests {
    use std::{io::BufReader, sync::RwLock};

    use super::*;
    use crate::hnsw::ram_hnsw::RAMLayer;
    fn layer_check<L: SearchableLayer>(buf: L, no_nodes: u32, cnx: &[Vec<(VectorAddr, f32)>]) {
        let no_cnx = vec![];
        for i in 0..no_nodes {
            let expected = cnx.get(i as usize).unwrap_or(&no_cnx);
            let got: Vec<_> = buf.get_out_edges(VectorAddr(i)).collect();
            assert_eq!(got, expected.iter().map(|(x, _)| *x).collect::<Vec<_>>());
        }
    }
    #[test]
    fn empty_hnsw() {
        let hnsw = RAMHnsw::new();
        let mut buf = vec![];
        let mut edges = vec![];
        DiskHnsw::serialize_into(&mut buf, &mut edges, 0, hnsw).unwrap();
        assert!(buf.is_empty());
    }

    #[test]
    fn hnsw_test() {
        let no_nodes = 3;
        let cnx0 = vec![
            vec![(VectorAddr(1), 1.0)],
            vec![(VectorAddr(2), 2.0)],
            vec![(VectorAddr(3), 3.0)],
        ];
        let layer0 = RAMLayer {
            out: cnx0
                .iter()
                .enumerate()
                .map(|(i, c)| (VectorAddr(i as u32), RwLock::new(c.clone())))
                .collect(),
        };
        let cnx1 = vec![vec![(VectorAddr(1), 4.0)], vec![(VectorAddr(2), 5.0)]];
        let layer1 = RAMLayer {
            out: cnx1
                .iter()
                .enumerate()
                .map(|(i, c)| (VectorAddr(i as u32), RwLock::new(c.clone())))
                .collect(),
        };
        let cnx2 = vec![vec![(VectorAddr(1), 6.0)]];
        let layer2 = RAMLayer {
            out: cnx2
                .iter()
                .enumerate()
                .map(|(i, c)| (VectorAddr(i as u32), RwLock::new(c.clone())))
                .collect(),
        };
        let entry_point = EntryPoint {
            node: VectorAddr(0),
            layer: 2,
        };
        let mut hnsw = RAMHnsw::new();
        hnsw.entry_point = entry_point;
        hnsw.layers = vec![layer0, layer1, layer2];
        let mut buf = vec![];
        let mut edges = vec![];
        DiskHnsw::serialize_into(&mut buf, &mut edges, no_nodes, hnsw).unwrap();

        let hnsw = DiskHnsw(&buf);
        let ep = hnsw.get_entry_point();
        assert_eq!(ep, entry_point);
        let layer0 = hnsw.get_layer(0);
        layer_check(layer0, no_nodes, &cnx0);
        let layer1 = hnsw.get_layer(1);
        layer_check(layer1, no_nodes, &cnx1);
        let layer2 = hnsw.get_layer(2);
        layer_check(layer2, no_nodes, &cnx2);
    }

    #[test]
    fn hnsw_deserialize_test() {
        let no_nodes = 3;
        let cnx0 = [
            vec![(VectorAddr(1), 1.0)],
            vec![(VectorAddr(2), 2.0)],
            vec![(VectorAddr(3), 3.0)],
        ];
        let layer0 = RAMLayer {
            out: cnx0
                .iter()
                .enumerate()
                .map(|(i, c)| (VectorAddr(i as u32), RwLock::new(c.clone())))
                .collect(),
        };
        let cnx1 = [vec![(VectorAddr(1), 4.0)], vec![(VectorAddr(2), 5.0)]];
        let layer1 = RAMLayer {
            out: cnx1
                .iter()
                .enumerate()
                .map(|(i, c)| (VectorAddr(i as u32), RwLock::new(c.clone())))
                .collect(),
        };
        let cnx2 = [vec![(VectorAddr(1), 6.0)]];
        let layer2 = RAMLayer {
            out: cnx2
                .iter()
                .enumerate()
                .map(|(i, c)| (VectorAddr(i as u32), RwLock::new(c.clone())))
                .collect(),
        };
        let entry_point = EntryPoint {
            node: VectorAddr(0),
            layer: 2,
        };
        let mut hnsw = RAMHnsw::new();
        hnsw.entry_point = entry_point;
        hnsw.layers = vec![layer0, layer1, layer2];
        let mut buf = vec![];
        let mut edges = vec![];
        DiskHnsw::serialize_into(&mut buf, &mut edges, no_nodes, hnsw).unwrap();
        let ram = DiskHnsw::deserialize(&buf, edges.as_slice()).unwrap();
        let mut buf2 = vec![];
        let mut edges2 = vec![];
        DiskHnsw::serialize_into(&mut buf2, &mut edges2, no_nodes, ram).unwrap();

        assert_eq!(buf, buf2);
        assert_eq!(edges, edges2);
    }
}
