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

#[cfg(test)]
pub mod test_utils;
use std::fmt::Debug;
use std::path::Path;

use crate::index::*;
use crate::memory_system::elements::*;
use crate::query::Query;
use crate::query_delete::DeleteQuery;
use crate::query_insert::InsertQuery;
pub struct Writer {
    index: Index,
}

impl Debug for Writer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VectorWriter")
            .field("index", &self.index)
            .finish()
    }
}

impl Writer {
    pub fn new(path: &str) -> Writer {
        Writer {
            index: Index::writer(Path::new(path)),
        }
    }
    pub fn insert(&mut self, key: String, element: Vec<f32>, labels: Vec<String>) {
        InsertQuery {
            key,
            element,
            labels,
            m: hnsw_params::m(),
            m_max: hnsw_params::m_max(),
            ef_construction: hnsw_params::ef_construction(),
            index: &mut self.index,
        }
        .run();
    }
    pub fn delete_document(&mut self, doc: String) {
        for key in self.index.get_prefixed(&doc) {
            self.delete_vector(key)
        }
    }
    pub fn delete_vector(&mut self, key: String) {
        DeleteQuery {
            delete: key,
            m: hnsw_params::m(),
            m_max: hnsw_params::m_max(),
            ef_construction: hnsw_params::ef_construction(),
            index: &mut self.index,
        }
        .run();
    }
    pub fn commit(&mut self) {
        self.index.commit()
    }
    pub fn run_garbage_collection(&mut self) {
        self.index.run_garbage_collection()
    }
    pub fn no_vectors(&self) -> usize {
        self.index.no_nodes() as usize
    }
    pub fn stats(&self) -> Stats {
        self.index.stats()
    }
}
