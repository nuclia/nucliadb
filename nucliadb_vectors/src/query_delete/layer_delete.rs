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

use crate::graph_arena::LockArena;
use crate::graph_disk::LockDisk;
use crate::graph_elems::*;
use crate::heuristics::heuristic_paper::select_neighbours_heuristic;
use crate::memory_processes::load_node_in_writer;
use crate::query::Query;
use crate::query_writer_search::layer_search::{LayerSearchQuery, LayerSearchValue};
use crate::write_index::LockWriter;

pub struct LayerDeleteQuery<'a> {
    pub layer: usize,
    pub delete: NodeId,
    pub m_max: usize,
    pub m: usize,
    pub ef_construction: usize,
    pub index: &'a LockWriter,
    pub arena: &'a LockArena,
    pub disk: &'a LockDisk,
}

impl<'a> Query for LayerDeleteQuery<'a> {
    type Output = ();

    fn run(&mut self) -> Self::Output {
        let in_edges = self.index.in_edges(self.layer, self.delete);
        let out_edges = self.index.out_edges(self.layer, self.delete);
        for (node, edge) in out_edges {
            load_node_in_writer(node, self.index, self.arena, self.disk);
            self.index.disconnect(self.layer, self.delete, node);
            self.arena.delete_edge(edge);
        }
        let mut reaching = Vec::with_capacity(in_edges.len());
        for (node, edge) in in_edges {
            load_node_in_writer(node, self.index, self.arena, self.disk);
            self.index.disconnect(self.layer, node, self.delete);
            self.arena.delete_edge(edge);
            reaching.push(node);
        }

        for source in reaching {
            if self.index.out_edges(self.layer, source).len() < self.m {
                let LayerSearchValue { neighbours } = LayerSearchQuery {
                    layer: self.layer,
                    elem: self.arena.get_node(source).vector,
                    k_neighbours: self.ef_construction,
                    entry_points: vec![source],
                    index: self.index,
                    arena: self.arena,
                    disk: self.disk,
                }
                .run();
                let mut candidates = neighbours;
                for (destination, edge_id) in self.index.out_edges(self.layer, source) {
                    candidates.push((destination, self.arena.get_edge(edge_id).dist));
                    self.arena.delete_edge(edge_id);
                    self.index.disconnect(self.layer, source, destination);
                }
                for (destination, dist) in select_neighbours_heuristic(self.m_max, candidates) {
                    if destination != source && destination != self.delete {
                        let edge_id = self.arena.insert_edge(Edge { dist });
                        self.index.connect(self.layer, source, destination, edge_id);
                    }
                }
            }
        }
    }
}
