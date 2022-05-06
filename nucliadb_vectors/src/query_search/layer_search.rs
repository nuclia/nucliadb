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

use rayon::prelude::*;

use crate::index::*;
use crate::memory_system::elements::*;
use crate::query::*;
use crate::utils::*;

#[derive(Clone, Default)]
pub struct LayerSearchValue {
    pub neighbours: Vec<(Node, f32)>,
}

pub struct LayerSearchQuery<'a> {
    pub layer: usize,
    pub elem: &'a Vector,
    pub k_neighbours: usize,
    pub entry_points: Vec<Node>,
    pub with_filter: &'a Vec<String>,
    pub index: &'a LockIndex,
}

impl<'a> Query for LayerSearchQuery<'a> {
    type Output = LayerSearchValue;
    fn run(&mut self) -> Self::Output {
        let mut result_max = 0f32;
        let mut results = Vec::with_capacity(self.k_neighbours);
        let mut candidates = BinaryHeap::new();
        let mut visited = HashSet::new();
        for entry_point in self.entry_points.iter().cloned() {
            let distance = self.index.semi_mapped_distance(self.elem, entry_point);
            visited.insert(entry_point);
            candidates.push(StandardElem(entry_point, distance));
            results.push(StandardElem(entry_point, distance));
            result_max = f32::min(result_max, distance);
        }
        loop {
            match candidates.pop() {
                None => break,
                Some(StandardElem(_, cd)) if cd < result_max => break,
                Some(StandardElem(candidate, _)) => {
                    for (node, _) in self.index.out_edges(self.layer, candidate) {
                        if !visited.contains(&node) {
                            visited.insert(node);
                            let distance = self.index.semi_mapped_distance(self.elem, node);
                            candidates.push(StandardElem(node, distance));
                            results.push(StandardElem(node, distance));
                            result_max = f32::max(result_max, distance);
                        }
                    }
                }
            }
        }
        results.sort();
        let mut neighbours: Vec<_> = results
            .into_par_iter()
            .rev()
            .filter(|StandardElem(node, _)| self.index.has_labels(*node, self.with_filter))
            .map(|StandardElem(n, d)| (n, d))
            .collect();
        while neighbours.len() < self.k_neighbours && !candidates.is_empty() {
            let StandardElem(node, dist) = candidates.pop().unwrap();
            if self.index.has_labels(node, self.with_filter) {
                neighbours.push((node, dist));
            }
        }
        LayerSearchValue { neighbours }
    }
}
