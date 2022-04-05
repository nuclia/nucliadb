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

pub(crate) mod layer_delete;

use layer_delete::LayerDeleteQuery;

use crate::graph_arena::LockArena;
use crate::graph_disk::LockDisk;
use crate::memory_processes::load_node_in_writer;
use crate::query::Query;
use crate::write_index::LockWriter;
pub struct DeleteQuery<'a> {
    pub delete: String,
    pub m_max: usize,
    pub m: usize,
    pub ef_construction: usize,
    pub index: &'a LockWriter,
    pub arena: &'a LockArena,
    pub disk: &'a LockDisk,
}

impl<'a> Query for DeleteQuery<'a> {
    type Output = ();

    fn run(&mut self) -> Self::Output {
        if let Some(delete_id) = self.disk.get_node_id(&self.delete) {
            load_node_in_writer(delete_id, self.index, self.arena, self.disk);
            let mut current_layer = self.index.get_top_layer(delete_id);
            loop {
                let mut query = LayerDeleteQuery {
                    delete: delete_id,
                    layer: current_layer,
                    m_max: self.m_max,
                    m: self.m,
                    ef_construction: self.ef_construction,
                    index: self.index,
                    arena: self.arena,
                    disk: self.disk,
                };

                query.run();
                if current_layer == 0 {
                    break;
                } else {
                    current_layer -= 1;
                }
            }
            let labels = self.arena.get_node(delete_id).labels;
            for label in labels {
                let remain = self.disk.remove_label(label);
                if remain == 0 {
                    self.arena.delete_label(label);
                }
            }
            self.index.erase(delete_id);
            self.arena.delete_node(delete_id);
            self.disk.remove_vector(&self.delete);
        }
    }
}
