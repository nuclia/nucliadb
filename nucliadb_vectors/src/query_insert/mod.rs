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

pub use layer_insert::*;

use crate::graph_arena::*;
use crate::graph_disk::*;
use crate::graph_elems::*;
use crate::query::Query;
use crate::query_writer_search::*;
use crate::write_index::*;
pub(crate) mod layer_insert;

pub struct InsertQuery<'a> {
    pub key: String,
    pub element: Vec<f32>,
    pub labels: Vec<String>,
    pub m: usize,
    pub m_max: usize,
    pub ef_construction: usize,
    pub index: &'a LockWriter,
    pub arena: &'a LockArena,
    pub disk: &'a LockDisk,
}

impl<'a> Query for InsertQuery<'a> {
    type Output = ();

    fn run(&mut self) -> Self::Output {
        let mut labels = HashSet::new();
        for label_value in std::mem::take(&mut self.labels) {
            match self.disk.get_label_id(&label_value) {
                Some(label_id) => {
                    self.disk.grow_label(label_id);
                    labels.insert(label_id);
                }
                None => {
                    let label = Label::new(self.arena.free_label(), label_value);
                    self.disk.add_label(&label);
                    labels.insert(label.my_id);
                }
            }
        }
        let node = Node {
            labels,
            key: self.key.clone(),
            vector: GraphVector::from(self.element.clone()),
        };
        let new_element = self.arena.insert_node(node);
        self.disk.log_node_id(&self.key, new_element);
        match self.index.get_entry_point() {
            None => {
                let top_level = rand::random::<usize>() % self.index.max_layers();
                self.index.set_top_layer(new_element, top_level);
                for i in 0..=top_level {
                    self.index
                        .replace_layer(i, WriteIndexLayer::with_ep(new_element));
                }
                self.index.set_entry_point(new_element, top_level);
            }
            Some((mut ep, ep_level)) => {
                let new_element_level = rand::random::<usize>() % self.index.max_layers();
                self.index.set_top_layer(new_element, new_element_level);
                for i in (ep_level + 1)..=new_element_level {
                    let layer = WriteIndexLayer::with_ep(new_element);
                    self.index.replace_layer(i, layer);
                }
                let LayerSearchValue { mut neighbours } = LayerSearchQuery {
                    elem: self.arena.get_node(new_element).vector,
                    layer: ep_level,
                    k_neighbours: 1,
                    entry_points: vec![ep],
                    index: self.index,
                    arena: self.arena,
                    disk: self.disk,
                }
                .run();
                ep = neighbours.pop().unwrap().0;

                let mut current_layer = std::cmp::min(ep_level, new_element_level);
                let mut entry_points = vec![ep];
                loop {
                    let LayerInsertValue { neighbours } = LayerInsertQuery {
                        index: self.index,
                        arena: self.arena,
                        disk: self.disk,
                        entry_points,
                        new_element,
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
                if new_element_level > ep_level {
                    self.index.set_entry_point(new_element, new_element_level);
                }
            }
        }
    }
}
