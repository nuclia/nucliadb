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

use std::collections::HashSet;

use crate::graph_arena::*;
use crate::graph_disk::*;
use crate::graph_elems::*;
use crate::heuristics::heuristic_paper::select_neighbours_heuristic;
use crate::memory_processes::load_node_in_writer;
use crate::query::Query;
use crate::query_writer_search::layer_search::{LayerSearchQuery, LayerSearchValue};
use crate::write_index::*;
#[derive(Clone, Default)]
pub struct LayerInsertValue {
    pub neighbours: Vec<NodeId>,
}

pub struct LayerInsertQuery<'a> {
    pub layer: usize,
    pub new_element: NodeId,
    pub entry_points: Vec<NodeId>,
    pub m: usize,
    pub m_max: usize,
    pub ef_construction: usize,
    pub index: &'a LockWriter,
    pub arena: &'a LockArena,
    pub disk: &'a LockDisk,
}

impl<'a> Query for LayerInsertQuery<'a> {
    type Output = LayerInsertValue;

    fn run(&mut self) -> Self::Output {
        println!("LAYER {}", self.layer);
        let LayerSearchValue { neighbours } = LayerSearchQuery {
            elem: self.arena.get_node(self.new_element).vector,
            layer: self.layer,
            k_neighbours: self.ef_construction,
            entry_points: self.entry_points.clone(),
            index: self.index,
            arena: self.arena,
            disk: self.disk,
        }
        .run();
        self.index.add_node(self.new_element, self.layer);
        let mut need_repair = HashSet::new();
        let mut query_value = LayerInsertValue::default();
        let neighbours = select_neighbours_heuristic(self.m, neighbours);
        for (node_id, dist) in neighbours {
            load_node_in_writer(node_id, self.index, self.arena, self.disk);
            let edge_id = self.arena.insert_edge(Edge { dist });
            self.index
                .connect(self.layer, self.new_element, node_id, edge_id);
            let edge_id = self.arena.insert_edge(Edge { dist });
            self.index
                .connect(self.layer, node_id, self.new_element, edge_id);
            if self.index.out_edges(self.layer, node_id).len() > self.m_max {
                need_repair.insert(node_id);
            }
            query_value.neighbours.push(node_id);
        }
        for source in need_repair {
            let edges = self.index.out_edges(self.layer, source);
            let mut candidates = Vec::with_capacity(edges.len());
            for (destination, edge_id) in edges {
                candidates.push((destination, self.arena.get_edge(edge_id).dist));
                self.arena.delete_edge(edge_id);
                self.index.disconnect(self.layer, source, destination);
            }
            for (destination, dist) in select_neighbours_heuristic(self.m_max, candidates) {
                if destination != source {
                    let edge_id = self.arena.insert_edge(Edge { dist });
                    self.index.connect(self.layer, source, destination, edge_id);
                }
            }
        }
        query_value
    }
}
