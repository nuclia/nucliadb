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

use std::cmp::Reverse;
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
    pub index: &'a Index,
}

impl<'a> Query for LayerSearchQuery<'a> {
    type Output = LayerSearchValue;
    fn run(&mut self) -> Self::Output {
        let mut visited = HashSet::new();
        let mut candidates = BinaryHeap::new();
        let mut ms_neighbours = BinaryHeap::new();
        for ep in &self.entry_points {
            visited.insert(*ep);
            let similarity = self.index.semi_mapped_similarity(self.elem, *ep);
            candidates.push(StandardElem(*ep, similarity));
            ms_neighbours.push(Reverse(StandardElem(*ep, similarity)));
        }
        loop {
            match (candidates.pop(), ms_neighbours.peek().cloned()) {
                (None, _) => break,
                (Some(StandardElem(_, cs)), Some(Reverse(StandardElem(_, ws)))) if cs < ws => break,
                (Some(StandardElem(cn, _)), Some(Reverse(StandardElem(_, ws)))) => {
                    for (node, _) in self.index.out_edges(self.layer, cn) {
                        if !visited.contains(&node) {
                            visited.insert(node);
                            let similarity = self.index.semi_mapped_similarity(self.elem, node);
                            if similarity > ws || ms_neighbours.len() < self.k_neighbours {
                                candidates.push(StandardElem(node, similarity));
                                ms_neighbours.push(Reverse(StandardElem(node, similarity)));
                                if ms_neighbours.len() > self.k_neighbours {
                                    ms_neighbours.pop();
                                }
                            }
                        }
                    }
                }
                _ => (),
            }
        }
        let neighbours = ms_neighbours.into_sorted_vec();
        let neighbours: Vec<_> = neighbours
            .into_par_iter()
            .map(|Reverse(StandardElem(n, d))| (n, d))
            .filter(|(node, _)| self.index.has_labels(*node, self.with_filter))
            .collect();
        LayerSearchValue { neighbours }
    }
}
