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

use crate::index::Index;
use crate::query::Query;
pub struct DeleteQuery<'a> {
    pub delete: String,
    pub m_max: usize,
    pub m: usize,
    pub ef_construction: usize,
    pub index: &'a mut Index,
}

impl<'a> Query for DeleteQuery<'a> {
    type Output = ();

    fn run(&mut self) -> Self::Output {
        if let Some(delete) = self.index.get_node(&self.delete) {
            let vector = self.index.get_node_vector(delete);
            for current_layer in (0..self.index.no_layers()).rev() {
                LayerDeleteQuery {
                    delete,
                    layer: current_layer,
                    m_max: self.m_max,
                    m: self.m,
                    ef_construction: self.ef_construction,
                    index: self.index,
                    vector: &vector,
                }
                .run();
            }
            self.index.erase(delete);
        }
    }
}
