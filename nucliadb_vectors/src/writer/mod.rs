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

use crate::graph_arena::*;
use crate::graph_disk::*;
use crate::graph_elems::HNSWParams;
pub use crate::graph_elems::NodeId;
use crate::query::Query;
use crate::query_delete::DeleteQuery;
use crate::query_insert::InsertQuery;
use crate::utils::SystemLock;
use crate::write_index::*;
pub struct Writer {
    lock: SystemLock,
    index: LockWriter,
    arena: LockArena,
    disk: LockDisk,
    params: HNSWParams,
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
        let params = HNSWParams::default();
        let disk = Disk::start(Path::new(path));
        let arena = Arena::from_disk(&disk);
        let index = WriteIndex::with_params(&disk, params.no_layers);
        Writer {
            params,
            lock: SystemLock::new(path),
            disk: disk.into(),
            arena: arena.into(),
            index: index.into(),
        }
    }
    pub fn insert(&mut self, key: String, element: Vec<f32>, labels: Vec<String>) {
        InsertQuery {
            key,
            element,
            labels,
            m: self.params.m,
            m_max: self.params.m_max,
            ef_construction: self.params.ef_construction,
            index: &self.index,
            arena: &self.arena,
            disk: &self.disk,
        }
        .run();
    }
    pub fn delete_document(&mut self, doc: String) {
        let keys = self.disk.all_nodes_in(&doc);
        for key in keys {
            self.delete_vector(key)
        }
    }
    pub fn delete_vector(&mut self, key: String) {
        DeleteQuery {
            delete: key,
            m: self.params.m,
            m_max: self.params.m_max,
            ef_construction: self.params.ef_construction,
            index: &self.index,
            arena: &self.arena,
            disk: &self.disk,
        }
        .run();
    }
    pub fn flush(&mut self) {
        use crate::memory_processes::dump_index_into_disk;
        self.lock.lock_exclusive();
        dump_index_into_disk(&self.index, &self.arena, &self.disk);
        self.arena.dump_into_disk(&self.disk);
        self.arena.reload(&self.disk);
        self.index.reload(&self.disk);
        self.lock.lock_shared();
    }
    pub fn no_vectors(&self) -> usize {
        self.disk.no_nodes()
    }
    pub fn no_labels(&self) -> usize {
        self.disk.no_labels()
    }
}
