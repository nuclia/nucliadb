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

use std::any::Any;
use std::fs::File;
use std::path::Path;

use memmap2::{Mmap, MmapOptions};

use crate::data_types::usize_utils::*;
use crate::hnsw::DiskHnsw;
use crate::hnsw::ram_hnsw::{EntryPoint, RAMHnsw, RAMLayer};
use crate::hnsw::search::{SearchableHnsw, SearchableLayer};
use crate::{VectorAddr, VectorR};

// These are used in serialization code which is no longer used outside tests
#[cfg(test)]
use std::collections::HashMap;
#[cfg(test)]
use std::io::{self, BufWriter};

pub const FILENAME: &str = "index.hnsw";

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

impl<'a> SearchableLayer for DiskLayer<'a> {
    fn get_out_edges(&self, address: VectorAddr) -> impl Iterator<Item = VectorAddr> {
        let node = DiskHnswV1::get_node(self.hnsw, address);
        DiskHnswV1::get_out_edges(node, self.layer).map(|(a, _s)| a)
    }
}

impl<'a> SearchableHnsw for &'a DiskHnswV1 {
    type L = DiskLayer<'a>;
    fn get_entry_point(&self) -> EntryPoint {
        self.entrypoint()
    }
    fn get_layer(&self, i: usize) -> Self::L {
        DiskLayer {
            hnsw: self.0.as_ref(),
            layer: i,
        }
    }
}

pub struct EdgeIter<'a> {
    crnt: usize,
    buf: &'a [u8],
}
impl Iterator for EdgeIter<'_> {
    type Item = (VectorAddr, f32);
    fn next(&mut self) -> Option<Self::Item> {
        if self.buf.len() == self.crnt {
            None
        } else {
            let buf = self.buf;
            let mut crnt = self.crnt;
            let node_addr = usize_from_slice_le(&buf[crnt..(crnt + NODE_LEN)]);
            crnt += USIZE_LEN;
            let edge = f32_from_le_bytes(&buf[crnt..(crnt + EDGE_LEN)]);
            crnt += EDGE_LEN;
            self.crnt = crnt;
            Some((VectorAddr(node_addr as u32), edge))
        }
    }
}

pub struct DiskHnswV1(Mmap);
impl DiskHnswV1 {
    pub fn open(path: &Path, prewarm: bool) -> VectorR<Self> {
        let hnsw_file = File::open(path.join(FILENAME))?;
        let mut index_options = MmapOptions::new();
        if prewarm {
            index_options.populate();
        }
        let index = unsafe { index_options.map(&hnsw_file)? };

        Ok(DiskHnswV1(index))
    }

    #[cfg(test)] // Not used anymore, used to test migration in tests
    fn serialize_node<W>(mut buf: W, offset: usize, node_addr: u32, hnsw: &RAMHnsw) -> io::Result<usize>
    where
        W: io::Write,
    {
        let node = VectorAddr(node_addr);
        let mut length = offset;
        let mut indexing = HashMap::new();
        for layer in 0..hnsw.num_layers() {
            let num_edges = hnsw.get_layer(layer).num_out_edges(&node);
            indexing.insert(layer, length);
            buf.write_all(&num_edges.to_le_bytes())?;
            length += USIZE_LEN;
            if num_edges > 0 {
                for (cnx, edge) in hnsw.get_layer(layer).out[&node].read().unwrap().iter() {
                    buf.write_all(&(cnx.0 as usize).to_le_bytes())?;
                    buf.write_all(&edge.to_le_bytes())?;
                    length += CNX_LEN;
                }
            }
        }
        for layer in (0..hnsw.num_layers()).rev() {
            buf.write_all(&indexing[&layer].to_le_bytes())?;
        }
        length += hnsw.num_layers() * USIZE_LEN;
        buf.flush()?;
        Ok(length)
    }

    // node must be serialized using DiskNode, may have trailing bytes at the start.
    fn get_out_edges(node: &[u8], layer: usize) -> EdgeIter<'_> {
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

    #[cfg(test)] // Not used anymore, used to test migration in tests
    fn serialize_into(mut buf: impl std::io::Write, num_nodes: u32, hnsw: &RAMHnsw) -> io::Result<()> {
        if num_nodes == 0 {
            // Empty graph, nothing to serialize
            return Ok(());
        }

        let mut length = 0;
        let mut nodes_end = vec![];
        for node in 0..num_nodes {
            length = DiskHnswV1::serialize_node(&mut buf, length, node, hnsw)?;
            nodes_end.push(length)
        }
        for ends_at in nodes_end.into_iter().rev() {
            buf.write_all(&ends_at.to_le_bytes())?;
            length += USIZE_LEN;
        }
        let EntryPoint { node, layer } = hnsw.entry_point;
        buf.write_all(&layer.to_le_bytes())?;
        buf.write_all(&(node.0 as usize).to_le_bytes())?;
        let _length = length + 2 * USIZE_LEN;
        buf.flush()?;

        Ok(())
    }

    #[cfg(test)] // Not used anymore, used to test migration in tests
    pub fn serialize_to(path: &Path, num_nodes: u32, hnsw: &RAMHnsw) -> io::Result<()> {
        let mut buf = BufWriter::new(File::create(path.join(FILENAME))?);

        Self::serialize_into(&mut buf, num_nodes, hnsw)
    }

    // hnsw must be serialized using DiskHnsw, may have trailing bytes at the start.
    fn entrypoint(&self) -> EntryPoint {
        assert!(!self.0.is_empty());

        let node_start = self.0.len() - USIZE_LEN;
        let layer_start = node_start - USIZE_LEN;
        let node_addr = usize_from_slice_le(&self.0[node_start..(node_start + NODE_LEN)]);
        let layer = usize_from_slice_le(&self.0[layer_start..(layer_start + USIZE_LEN)]);
        EntryPoint {
            node: VectorAddr(node_addr as u32),
            layer,
        }
    }
    // hnsw must be serialized using MHnsw, may have trailing bytes at the start.
    // The returned node will have trailing bytes at the start.
    pub fn get_node(hnsw: &[u8], address: VectorAddr) -> &[u8] {
        let indexing_end = hnsw.len() - (2 * USIZE_LEN);
        // node + 1 since the layers are stored in reverse order.
        // [n3, n2, n1, n0, end] Since we have the position of end, the node i is
        // i + 1 positions to its left.
        let pos = indexing_end - ((address.0 as usize + 1) * USIZE_LEN);
        let node_end = usize_from_slice_le(&hnsw[pos..(pos + USIZE_LEN)]);
        &hnsw[..node_end]
    }
}

impl DiskHnsw for DiskHnswV1 {
    fn deserialize(&self) -> VectorR<RAMHnsw> {
        let mut ram = RAMHnsw::new();
        if self.0.is_empty() {
            return Ok(ram);
        }

        let end = self.0.len();
        ram.entry_point = self.entrypoint();

        let mut node_index: u32 = 0;
        loop {
            let indexing_pos = end - (node_index as usize + 3) * USIZE_LEN;
            let node_end = usize_from_slice_le(&self.0[indexing_pos..indexing_pos + USIZE_LEN]);
            let mut layer_index = 0;
            loop {
                if ram.layers.len() == layer_index {
                    ram.layers.push(RAMLayer::default());
                }

                let layer_pos = node_end - (layer_index + 1) * USIZE_LEN;
                let edges_start = usize_from_slice_le(&self.0[layer_pos..layer_pos + USIZE_LEN]);
                let number_edges = usize_from_slice_le(&self.0[edges_start..edges_start + USIZE_LEN]);

                let cnx_start = edges_start + USIZE_LEN;
                let cnx_end = cnx_start + number_edges * CNX_LEN;

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
                        crnt: 0,
                        buf: &self.0[cnx_start..cnx_end],
                    };
                    for (to, edge) in edges {
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

    fn as_any(&self) -> &dyn Any {
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
        DiskHnswV1::serialize_into(&mut buf, 0, &hnsw).unwrap();
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
        DiskHnswV1::serialize_to(dir.path(), no_nodes, &hnsw).unwrap();

        let hnsw = &DiskHnswV1::open(dir.path(), false).unwrap();
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
        DiskHnswV1::serialize_to(dir.path(), no_nodes, &hnsw).unwrap();
        let disk1 = DiskHnswV1::open(dir.path(), false).unwrap();
        let ram = disk1.deserialize().unwrap();

        let mut buf2 = vec![];
        DiskHnswV1::serialize_into(&mut buf2, no_nodes, &ram).unwrap();

        assert_eq!(disk1.0.as_ref(), buf2.as_slice());
    }

    #[test]
    fn hnsw_deserialize_one_node() {
        let mut hnsw = RAMHnsw::new();
        hnsw.add_node(VectorAddr(0), 0);
        hnsw.update_entry_point();

        let dir = TempDir::new().unwrap();
        DiskHnswV1::serialize_to(dir.path(), 1, &hnsw).unwrap();

        let disk1 = DiskHnswV1::open(dir.path(), false).unwrap();
        let mut ram = disk1.deserialize().unwrap();
        ram.fix_broken_graph();

        assert_eq!(ram.layers.len(), 1);
        assert_eq!(ram.layers[0].out.len(), 1);
        assert_eq!(ram.layers[0].out[&VectorAddr(0)].read().unwrap().len(), 0);
    }
}
