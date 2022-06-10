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

use crate::heuristics::heuristic_paper::select_neighbours_heuristic;
use crate::index::Index;
use crate::memory_system::elements::*;
use crate::query::Query;
use crate::query_search::layer_search::{LayerSearchQuery, LayerSearchValue};

pub struct LayerDeleteQuery<'a> {
    pub layer: usize,
    pub delete: Node,
    pub m_max: usize,
    pub m: usize,
    pub ef_construction: usize,
    pub vector: &'a Vector,
    pub index: &'a mut Index,
}

impl<'a> Query for LayerDeleteQuery<'a> {
    type Output = ();

    fn run(&mut self) -> Self::Output {
        if !self.index.is_node_at(self.layer, self.delete) {
            return;
        }
        let in_edges = self.index.in_edges(self.layer, self.delete);
        let out_edges = self.index.out_edges(self.layer, self.delete);
        for (node, _) in out_edges {
            self.index.disconnect(self.layer, self.delete, node);
        }
        let mut reaching = Vec::with_capacity(in_edges.len());
        for (node, edge) in in_edges {
            assert_eq!(node, edge.to);
            assert_eq!(self.delete, edge.from);
            self.index.disconnect(self.layer, node, self.delete);
            reaching.push(node);
        }
        assert!(self.index.in_edges(self.layer, self.delete).is_empty());
        assert!(self.index.out_edges(self.layer, self.delete).is_empty());
        for source in reaching {
            if self.index.out_edges(self.layer, source).len() < (self.m / 2) {
                let LayerSearchValue { neighbours } = LayerSearchQuery {
                    layer: self.layer,
                    elem: self.vector,
                    k_neighbours: self.ef_construction,
                    entry_points: vec![source],
                    index: self.index,
                    with_filter: &vec![],
                }
                .run();
                let mut candidates = neighbours;
                for (destination, edge) in self.index.out_edges(self.layer, source) {
                    candidates.push((destination, edge.dist));
                    self.index.disconnect(self.layer, source, destination);
                }
                for (destination, dist) in select_neighbours_heuristic(self.m_max, candidates) {
                    if destination != source && destination != self.delete {
                        let edge = Edge {
                            from: source,
                            to: destination,
                            dist,
                        };
                        self.index.connect(self.layer, edge);
                    }
                }
            }
        }
    }
}
