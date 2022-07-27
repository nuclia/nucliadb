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

use crate::index::*;
use crate::memory_system::elements::*;
use crate::query::Query;

#[derive(Default, Clone)]
pub struct SearchValue {
    pub neighbours: Vec<(Node, f32)>,
}

pub struct SearchQuery<'a> {
    pub elem: Vector,
    pub k_neighbours: usize,
    pub index: &'a Index,
    pub with_filter: &'a Vec<String>,
}

impl<'a> Query for SearchQuery<'a> {
    type Output = SearchValue;

    fn run(&mut self) -> Self::Output {
        if let Some(entry_point) = self.index.get_entry_point() {
            let mut down_step = LayerSearchQuery {
                layer: entry_point.layer as usize,
                k_neighbours: 1,
                elem: &self.elem,
                entry_points: vec![entry_point.node],
                index: self.index,
                with_filter: self.with_filter,
            };
            while down_step.layer != 0 {
                let result = down_step.run();
                down_step.entry_points = result.neighbours.into_iter().map(|(n, _)| n).collect();
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
    }
}
