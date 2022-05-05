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

use crate::heuristics::heuristic_paper::select_neighbours_heuristic;
use crate::index::LockIndex;
use crate::memory_system::elements::*;
use crate::query::Query;
use crate::query_search::layer_search::{LayerSearchQuery, LayerSearchValue};

#[derive(Clone, Default)]
pub struct LayerInsertValue {
    pub neighbours: Vec<Node>,
}

pub struct LayerInsertQuery<'a> {
    pub layer: usize,
    pub new_element: Node,
    pub entry_points: Vec<Node>,
    pub m: usize,
    pub m_max: usize,
    pub ef_construction: usize,
    pub vector: &'a Vector,
    pub index: &'a LockIndex,
}

impl<'a> Query for LayerInsertQuery<'a> {
    type Output = LayerInsertValue;

    fn run(&mut self) -> Self::Output {
        let LayerSearchValue { neighbours } = LayerSearchQuery {
            elem: self.vector,
            layer: self.layer,
            k_neighbours: self.ef_construction,
            entry_points: self.entry_points.clone(),
            index: self.index,
        }
        .run();
        let mut need_repair = HashSet::new();
        let mut query_value = LayerInsertValue::default();
        let neighbours = select_neighbours_heuristic(self.m, neighbours);
        for (goes_to, dist) in neighbours {
            let edge = Edge {
                from: self.new_element,
                to: goes_to,
                dist,
            };
            self.index.connect(self.layer, edge);
            let edge = Edge {
                from: goes_to,
                to: self.new_element,
                dist,
            };
            self.index.connect(self.layer, edge);
            if self.index.out_edges(self.layer, goes_to).len() > self.m_max {
                need_repair.insert(goes_to);
            }
            query_value.neighbours.push(goes_to);
        }
        for source in need_repair {
            let edges = self.index.out_edges(self.layer, source);
            let mut candidates = Vec::with_capacity(edges.len());
            for (destination, edge) in edges {
                candidates.push((destination, edge.dist));
                self.index.disconnect(self.layer, source, destination);
            }
            for (destination, dist) in select_neighbours_heuristic(self.m_max, candidates) {
                if destination != source {
                    let edge = Edge {
                        from: source,
                        to: destination,
                        dist,
                    };
                    self.index.connect(self.layer, edge);
                }
            }
        }
        query_value
    }
}
