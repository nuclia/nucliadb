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

use std::collections::{BinaryHeap, HashSet};

use crate::graph_disk::*;
use crate::graph_elems::*;
use crate::memory_processes::load_node_in_reader;
use crate::query::*;
use crate::read_index::*;
use crate::utils::*;

#[derive(Clone, Default)]
pub struct LayerSearchValue {
    pub neighbours: Vec<(NodeId, f32)>,
}

pub struct LayerSearchQuery<'a> {
    pub layer: usize,
    pub elem: GraphVector,
    pub k_neighbours: usize,
    pub entry_points: Vec<NodeId>,
    pub index: &'a LockReader,
    pub disk: &'a LockDisk,
}

impl<'a> Query for LayerSearchQuery<'a> {
    type Output = LayerSearchValue;
    fn run(&mut self) -> Self::Output {
        let mut results = BinaryHeap::new();
        let mut candidates = BinaryHeap::new();
        let mut visited = HashSet::new();
        for entry_point in self.entry_points.iter().cloned() {
            if load_node_in_reader(entry_point, self.index, self.disk) {
                let distance = self.index.distance_to(&self.elem, entry_point);
                candidates.push(InverseElem(entry_point, distance));
                results.push(StandardElem(entry_point, distance));
                visited.insert(entry_point);
            }
        }
        loop {
            match (candidates.pop(), results.peek().cloned()) {
                (None, _) => break,
                (Some(InverseElem(_, cd)), Some(StandardElem(_, rd))) if cd > rd => break,
                (Some(InverseElem(candidate, _)), _) => {
                    for edge_index in 0..self.index.no_edges(self.layer, candidate) {
                        let edge = self.index.get_edge(self.layer, candidate, edge_index);
                        let node = edge.goes_to;
                        let loaded = load_node_in_reader(node, self.index, self.disk);
                        if !visited.contains(&node) && loaded {
                            visited.insert(node);
                            let distance = self.index.distance_to(&self.elem, node);
                            candidates.push(InverseElem(node, distance));
                            results.push(StandardElem(node, distance));
                        }
                    }
                }
            }
        }
        let mut neighbours = Vec::with_capacity(self.k_neighbours);
        while results.len() > self.k_neighbours {
            results.pop();
        }
        while !results.is_empty() {
            let StandardElem(node, dist) = results.pop().unwrap();
            neighbours.push((node, dist));
        }
        neighbours.reverse();
        LayerSearchValue { neighbours }
    }
}
