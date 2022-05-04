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

pub use layer_insert::*;

use crate::index::*;
use crate::memory_system::elements::*;
use crate::query::Query;
use crate::query_search::layer_search::*;
pub(crate) mod layer_insert;

pub struct InsertQuery<'a> {
    pub key: String,
    pub element: Vec<f32>,
    pub labels: Vec<String>,
    pub m: usize,
    pub m_max: usize,
    pub ef_construction: usize,
    pub index: &'a LockIndex,
}

impl<'a> Query for InsertQuery<'a> {
    type Output = ();

    fn run(&mut self) -> Self::Output {
        if self.index.has_node(&self.key) {
            return;
        }
        let label_adder = {
            let labels = std::mem::take(&mut self.labels);
            let label_adder = self.index.clone();
            let key_adder = self.key.clone();
            std::thread::spawn(move || {
                for label_value in labels {
                    label_adder.add_label(key_adder.clone(), label_value);
                }
            })
        };

        let key = self.key.clone();
        let vector = Vector::from(self.element.clone());
        match self.index.get_entry_point() {
            None => {
                let top_level = rand::random::<usize>() % hnsw_params::no_layers();
                let node = self.index.add_node(self.key.clone(), vector, top_level);
                self.index.set_entry_point((node, top_level).into())
            }
            Some(entry_point) => {
                let mut ep = entry_point.node;
                let ep_level = entry_point.layer as usize;
                let LayerSearchValue { mut neighbours } = LayerSearchQuery {
                    elem: &vector,
                    layer: ep_level,
                    k_neighbours: 1,
                    entry_points: vec![ep],
                    index: self.index,
                }
                .run();
                ep = neighbours.pop().unwrap().0;
                let node_level = rand::random::<usize>() % hnsw_params::no_layers();
                let node = self.index.add_node(key, vector.clone(), node_level);
                let mut current_layer = std::cmp::min(ep_level, node_level);
                let mut entry_points = vec![ep];
                loop {
                    let LayerInsertValue { neighbours } = LayerInsertQuery {
                        vector: &vector,
                        index: self.index,
                        entry_points,
                        new_element: node,
                        layer: current_layer,
                        m: self.m,
                        m_max: self.m_max,
                        ef_construction: self.ef_construction,
                    }
                    .run();
                    if current_layer == 0 {
                        break;
                    } else {
                        current_layer -= 1;
                        entry_points = neighbours
                    }
                }
                self.index.set_entry_point((node, node_level).into());
            }
        }
        label_adder.join().unwrap();
    }
}
