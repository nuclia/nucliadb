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

pub(crate) mod layer_search;

use layer_search::*;

use crate::graph_disk::*;
use crate::graph_elems::*;
use crate::memory_processes::load_node_in_reader;
use crate::query::Query;
use crate::read_index::*;

#[derive(Default, Clone)]
pub struct SearchValue {
    pub neighbours: Vec<(NodeId, f32)>,
}

pub struct SearchQuery<'a> {
    pub elem: GraphVector,
    pub k_neighbours: usize,
    pub index: &'a LockReader,
    pub disk: &'a LockDisk,
}

impl<'a> Query for SearchQuery<'a> {
    type Output = SearchValue;

    fn run(&mut self) -> Self::Output {
        if let Some((ep, ep_layer)) = self.index.get_entry_point() {
            if load_node_in_reader(ep, self.index, self.disk) {
                let mut down_step = LayerSearchQuery {
                    layer: ep_layer,
                    k_neighbours: 1,
                    elem: self.elem.clone(),
                    entry_points: vec![ep],
                    index: self.index,
                    disk: self.disk,
                };
                while down_step.layer != 0 {
                    let result = down_step.run();
                    if result.neighbours.is_empty() {
                        down_step.entry_points[0] = result.neighbours[0].0;
                    }
                    down_step.layer -= 1;
                }
                let mut final_search = down_step;
                final_search.k_neighbours = self.k_neighbours;
                let layer_result = final_search.run();
                SearchValue {
                    neighbours: layer_result.neighbours,
                }
            } else {
                SearchValue::default()
            }
        } else {
            SearchValue::default()
        }
    }
}
