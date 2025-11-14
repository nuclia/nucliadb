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

//! The disk format is as follows:
//!
//! 1st, we serialize each node. For each node, we serialize each layer on the graph like this:
//! [num_edges: u32] [edge_0: u32] [edge_1: u32] ...
//!
//! If the layer is empty (the node is not present in this layer) we just save num_edges = 0.
//! At the end of the layer list, we store offsets that refer to where the layer starts taken
//! from the end of the node:
//!
//! [layer_2_offset: u32] [layer_1_offset: u32] [layer_0_offset: u32]
//!
//! For example, a full node might look like this (each number is a u32)
//!
//! 5 (layer 0 num edges) 1 17 5433 45 667 (VectorAddr of linked nodes)
//! 3 (layer 1 num edges) 45 666 22 (VectorAddr of linked nodes)
//! 0 (layer 2 num edges)
//! 16 (layer 2 offset) 32 (layer 1 offset) 52 (layer 0 offset) |
//!                                                             ^
//!                                           offsets measured from here in bytes
//!
//! After all the nodes, we store the position where the node was saved. The position is the
//! pointer to the end of the node, which is the same as the starting point of the offset for
//! the layers.
//!
//! [node_3_offset: u32] [node_2_offset: u32] [node_1_offset: u32] [node_0_offset: u32]
//!
//! Finally, we serialize the entry point as:
//!
//! [layer: u32] [node: u32]
//!
//! Finally, we store edge weights/distances as f32 in a separate file. This is only used
//! for deserialization during segment merge, so it does not need to be seekable. So we simply
//! dump the edges in the same order that the edges appear in the graph file, one by one:
//! [edge_0: f32] [edge_1: f32] ...

use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read};

use memmap2::{Mmap, MmapOptions};
use std::path::{Path, PathBuf};

use crate::data_types::usize_utils::*;
use crate::hnsw::DiskHnsw;
use crate::hnsw::ram_hnsw::{EntryPoint, RAMHnsw, RAMLayer};
use crate::hnsw::search::{SearchableHnsw, SearchableLayer};
use crate::{VectorAddr, VectorR};

pub const GRAPH_FILENAME: &str = "hnsw.graph";
pub const EDGES_FILENAME: &str = "hnsw.edges";

pub struct DiskLayer<'a> {
    hnsw: &'a [u8],
    layer: usize,
}

impl<'a> SearchableLayer for DiskLayer<'a> {
    fn get_out_edges(&self, address: VectorAddr) -> impl Iterator<Item = VectorAddr> {
        let node = DiskHnswV2::get_node(self.hnsw, address);
        DiskHnswV2::get_out_edges(node, self.layer)
    }
}

impl<'a> SearchableHnsw for &'a DiskHnswV2 {
    type L = DiskLayer<'a>;
    fn get_entry_point(&self) -> EntryPoint {
        self.entrypoint()
    }
    fn get_layer(&self, i: usize) -> Self::L {
        DiskLayer {
            hnsw: &self.0,
            layer: i,
        }
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

pub struct DiskHnswV2(Mmap, PathBuf);
impl DiskHnswV2 {
    pub fn open(path: &Path, prewarm: bool) -> VectorR<Self> {
        let hnsw_file = File::open(path.join(GRAPH_FILENAME))?;
        let mut index_options = MmapOptions::new();
        if prewarm {
            index_options.populate();
        }
        let index = unsafe { index_options.map(&hnsw_file)? };

        Ok(DiskHnswV2(index, path.to_path_buf()))
    }

    fn serialize_node(
        mut graph: impl io::Write,
        mut edge_file: impl io::Write,
        node_addr: u32,
        hnsw: &RAMHnsw,
    ) -> io::Result<usize> {
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

    /// Return the edges for a node (from `get_node()`) and layer
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

    fn serialize_into<W1: io::Write, W2: io::Write>(
        mut graph: W1,
        mut edge_file: W2,
        num_nodes: u32,
        hnsw: &RAMHnsw,
    ) -> io::Result<()> {
        if num_nodes == 0 {
            // Empty graph, nothing to serialize
            return Ok(());
        }

        let mut nodes_end = vec![];
        let mut pos = 0;
        for node in 0..num_nodes {
            pos += DiskHnswV2::serialize_node(&mut graph, &mut edge_file, node, hnsw)?;
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

    /// Serialize to files in the given segment directory
    pub fn serialize_to(path: &Path, num_nodes: u32, hnsw: &RAMHnsw) -> io::Result<()> {
        let mut graph = BufWriter::new(File::create(path.join(GRAPH_FILENAME))?);
        let mut edges = BufWriter::new(File::create(path.join(EDGES_FILENAME))?);

        Self::serialize_into(&mut graph, &mut edges, num_nodes, hnsw)
    }

    /// Get the entry point for the graph
    fn entrypoint(&self) -> EntryPoint {
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

    /// Get a buffer whose last bytes are the one from the requested node
    /// The last byte of the buffer is the last byte of the serialized node
    fn get_node(hnsw: &[u8], address: VectorAddr) -> &[u8] {
        let indexing_end = hnsw.len() - (2 * U32_LEN);
        // node + 1 since the layers are stored in reverse order.
        // [n3, n2, n1, n0, end] Since we have the position of end, the node i is
        // i + 1 positions to its left.
        let pos = indexing_end - ((address.0 as usize + 1) * U32_LEN);
        let node_end = u32_from_slice_le(&hnsw[pos..(pos + U32_LEN)]) as usize;
        &hnsw[..node_end]
    }
}

impl DiskHnsw for DiskHnswV2 {
    fn deserialize(&self) -> VectorR<RAMHnsw> {
        let mut ram = RAMHnsw::new();
        if self.0.is_empty() {
            return Ok(ram);
        }

        let mut edge_file = BufReader::new(File::open(self.1.join(EDGES_FILENAME))?);
        let end = self.0.len();
        ram.entry_point = self.entrypoint();

        let mut node_index: u32 = 0;
        loop {
            let indexing_pos = end - (node_index as usize + 3) * U32_LEN;
            let node_end = u32_from_slice_le(&self.0[indexing_pos..indexing_pos + U32_LEN]) as usize;
            let mut layer_index = 0;
            loop {
                if ram.layers.len() == layer_index {
                    ram.layers.push(RAMLayer::default());
                }

                let layer_pos = node_end - (layer_index + 1) * U32_LEN;
                let edges_offset = u32_from_slice_le(&self.0[layer_pos..layer_pos + U32_LEN]) as usize;
                let edges_start = node_end - edges_offset;

                let number_edges = u32_from_slice_le(&self.0[edges_start..edges_start + U32_LEN]) as usize;

                let cnx_start = edges_start + U32_LEN;
                let cnx_end = cnx_start + number_edges * U32_LEN;

                // All nodes are in layer 0, always deserialize even if not connected
                // This happens when the node only has one node (so no connections are possible)
                if layer_index == 0 || number_edges > 0 {
                    let mut ram_edges = ram.layers[layer_index]
                        .out
                        .entry(VectorAddr(node_index))
                        .or_default()
                        .write()
                        .unwrap();
                    let edges = EdgeIter {
                        pos: 0,
                        buf: &self.0[cnx_start..cnx_end],
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

    fn size(&self) -> usize {
        self.0.len()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use std::sync::RwLock;

    use tempfile::TempDir;

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
        DiskHnswV2::serialize_into(&mut buf, &mut edges, 0, &hnsw).unwrap();
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
        let dir = TempDir::new().unwrap();
        DiskHnswV2::serialize_to(dir.path(), no_nodes, &hnsw).unwrap();

        let hnsw = &DiskHnswV2::open(dir.path(), false).unwrap();
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

        let dir = TempDir::new().unwrap();
        DiskHnswV2::serialize_to(dir.path(), no_nodes, &hnsw).unwrap();
        let disk1 = DiskHnswV2::open(dir.path(), false).unwrap();
        let ram = disk1.deserialize().unwrap();

        let mut buf2 = vec![];
        let mut edges2 = vec![];
        DiskHnswV2::serialize_into(&mut buf2, &mut edges2, no_nodes, &ram).unwrap();

        assert_eq!(disk1.0.as_ref(), buf2.as_slice());
        let mut edges1 = vec![];
        File::open(dir.path().join(EDGES_FILENAME))
            .unwrap()
            .read_to_end(&mut edges1)
            .unwrap();
        assert_eq!(edges1, edges2);
    }

    #[test]
    fn hnsw_deserialize_one_node() {
        let mut hnsw = RAMHnsw::new();
        hnsw.add_node(VectorAddr(0), 0);
        hnsw.update_entry_point();

        let dir = TempDir::new().unwrap();
        DiskHnswV2::serialize_to(dir.path(), 1, &hnsw).unwrap();

        let disk1 = DiskHnswV2::open(dir.path(), false).unwrap();
        let mut ram = disk1.deserialize().unwrap();
        ram.fix_broken_graph();

        assert_eq!(ram.layers.len(), 1);
        assert_eq!(ram.layers[0].out.len(), 1);
        assert_eq!(ram.layers[0].out[&VectorAddr(0)].read().unwrap().len(), 0);
    }
}
