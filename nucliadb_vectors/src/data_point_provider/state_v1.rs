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

use std::collections::{HashMap, LinkedList};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use super::fs_state;
use serde::{Deserialize, Serialize};

use super::VectorR;
use crate::data_point::{DpId, Journal};
use crate::data_types::dtrie_ram::DTrie;

#[derive(Serialize, Deserialize)]
struct WorkUnit {
    pub age: SystemTime,
    pub load: Vec<Journal>,
}

#[cfg(test)]
impl WorkUnit {
    fn new() -> WorkUnit {
        WorkUnit {
            age: SystemTime::now(),
            load: vec![],
        }
    }
    pub fn add_unit(&mut self, dp: Journal) {
        self.load.push(dp);
    }

    pub fn size(&self) -> usize {
        self.load.len()
    }
}

#[derive(Serialize, Deserialize)]
pub struct State {
    // Deprecated, location must be passed as an argument.
    // WARNING: Can not use serde::skip nor move this field due to a bug in serde.
    #[allow(unused)]
    #[deprecated]
    location: PathBuf,

    // Total number of nodes stored. Some
    // may be marked as deleted but are waiting
    // for a merge to be fully removed.
    no_nodes: usize,

    // Current work unit
    current: WorkUnit,

    // Trie containing the deleted keys and the
    // time when they were deleted
    pub delete_log: DTrie<SystemTime>,

    // Already closed WorkUnits waiting to be merged
    work_stack: LinkedList<WorkUnit>,

    // This field is deprecated and is only
    // used for old states. Always use
    // the data_point journal for time references
    data_points: HashMap<DpId, SystemTime>,
}

impl State {
    pub fn data_point_iterator(&self) -> impl Iterator<Item = &Journal> {
        self.work_stack.iter().flat_map(|u| u.load.iter()).chain(self.current.load.iter())
    }

    pub fn open(path: &Path) -> VectorR<Self> {
        let r = fs_state::load_state::<Self>(path)?;
        Ok(r)
    }

    pub fn no_nodes(&self) -> usize {
        self.no_nodes
    }

    #[cfg(test)]
    pub fn new() -> Self {
        State {
            location: Default::default(),
            no_nodes: 0,
            current: WorkUnit::new(),
            delete_log: DTrie::new(),
            work_stack: Default::default(),
            data_points: Default::default(),
        }
    }

    #[cfg(test)]
    pub fn remove(&mut self, id: &str, deleted_since: SystemTime) {
        self.delete_log.insert(id.as_bytes(), deleted_since);
    }

    #[cfg(test)]
    pub fn add(&mut self, journal: Journal) {
        self.no_nodes += journal.no_nodes();
        self.current.add_unit(journal);
        if self.current.size() == 5 {
            self.close_work_unit();
        }
    }

    #[cfg(test)]
    fn close_work_unit(&mut self) {
        use std::mem;
        let prev = mem::replace(&mut self.current, WorkUnit::new());
        self.work_stack.push_front(prev);
    }
}
