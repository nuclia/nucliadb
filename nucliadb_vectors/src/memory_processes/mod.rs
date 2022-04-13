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

use crate::graph_arena::*;
use crate::graph_disk::*;
use crate::graph_elems::*;
use crate::graph_index::*;
use crate::read_index::LockReader;
use crate::write_index::LockWriter;

pub fn load_node_in_writer(
    node_id: NodeId,
    index: &LockWriter,
    arena: &LockArena,
    disk: &LockDisk,
) {
    if !index.is_cached(node_id) {
        let disk_node = disk.get_node(node_id);
        let top_layer = disk_node.neighbours.len() - 1;
        arena.load_node_from_disk(node_id, disk_node.node);
        index.add_node_from_disk(node_id, top_layer);
        for (layer_id, (out_edges, in_edges)) in disk_node.neighbours.into_iter().enumerate() {
            for edge in out_edges {
                arena.load_edge_from_disk(edge.my_id, edge.edge);
                index.add_connexion_from_disk(layer_id, edge.from, edge.goes_to, edge.my_id);
            }
            for edge in in_edges {
                arena.load_edge_from_disk(edge.my_id, edge.edge);
                index.add_connexion_from_disk(layer_id, edge.from, edge.goes_to, edge.my_id);
            }
        }
    }
}

pub fn load_node_in_reader(
    node_id: NodeId,
    index: &LockReader,
    arena: &LockArena,
    disk: &LockDisk,
) {
    if !index.is_cached(node_id) {
        let disk_node = disk.get_node(node_id);
        let top_layer = disk_node.neighbours.len() - 1;
        arena.load_node_from_disk(node_id, disk_node.node);
        index.add_node_from_disk(node_id, top_layer);
        for (layer_id, (out_edges, _)) in disk_node.neighbours.into_iter().enumerate() {
            for edge in out_edges {
                arena.load_edge_from_disk(edge.my_id, edge.edge);
                index.add_connexion_from_disk(layer_id, edge.from, edge.goes_to, edge.my_id);
            }
        }
    }
}

pub fn dump_index_into_disk(index: &LockWriter, arena: &LockArena, disk: &LockDisk) {
    index.invariant_assertion(arena);
    for (node, top_layer) in index.top_layers() {
        if index.is_cached(node) {
            let mut disk_node = DiskNode {
                node: arena.get_node(node),
                neighbours: Vec::new(),
            };
            let mut current = 0;
            while current <= top_layer {
                let out_n = index.out_edges(current, node);
                let in_n = index.in_edges(current, node);
                let mut out_edges = Vec::with_capacity(out_n.len());
                let mut in_edges = Vec::with_capacity(in_n.len());
                for (dest, edge) in out_n {
                    let disk_edge = DiskEdge {
                        my_id: edge,
                        edge: arena.get_edge(edge),
                        goes_to: dest,
                        from: node,
                    };
                    out_edges.push(disk_edge);
                }
                for (origin, edge) in in_n {
                    let disk_edge = DiskEdge {
                        my_id: edge,
                        edge: arena.get_edge(edge),
                        goes_to: node,
                        from: origin,
                    };
                    in_edges.push(disk_edge);
                }
                disk_node.neighbours.push((out_edges, in_edges));
                current += 1;
            }
            disk.add_node(&node, &disk_node);
        }
    }
    disk.log_entry_point(&index.get_entry_point());
    disk.update_version_number();
}
