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

use crate::data_point::{DpId, Journal};
use crate::data_types::dtrie_ram::DTrie;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, LinkedList};
use std::path::PathBuf;
use std::time::SystemTime;

#[derive(Serialize, Deserialize)]
struct WorkUnit {
    // This field is deprecated.
    pub age: SystemTime,
    pub load: Vec<Journal>,
}
impl Default for WorkUnit {
    fn default() -> Self {
        WorkUnit::new()
    }
}
impl WorkUnit {
    pub fn new() -> WorkUnit {
        WorkUnit {
            age: SystemTime::now(),
            load: vec![],
        }
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
    pub delete_log: DTrie,

    // Already closed WorkUnits waiting to be merged
    // Consider this field deprecated in favor of data_points.
    // WorkUnit is a bad abstraction that adds to much complexity.
    // The goal is to remove it in a future refactor.
    work_stack: LinkedList<WorkUnit>,

    // Available data points
    #[serde(default)]
    pub available_data_points: Vec<DpId>,

    // This field is deprecated and is only
    // used for old states. Always use
    // the data_point journal for time references
    data_points: HashMap<DpId, SystemTime>,

    // Deprecated field, not all vector clusters are
    // identified by a resource.
    #[serde(skip)]
    #[allow(unused)]
    #[deprecated]
    resources: HashMap<String, usize>,
}

impl Default for State {
    fn default() -> Self {
        Self::new()
    }
}

impl State {
    fn work_stack_iterator(&self) -> impl Iterator<Item = &Journal> {
        self.work_stack.iter().flat_map(|u| u.load.iter()).chain(self.current.load.iter())
    }

    #[allow(deprecated)]
    pub fn new() -> State {
        State {
            available_data_points: Vec::default(),
            location: PathBuf::default(),
            no_nodes: usize::default(),
            current: WorkUnit::default(),
            delete_log: DTrie::default(),
            work_stack: LinkedList::default(),
            data_points: HashMap::default(),
            resources: HashMap::default(),
        }
    }

    pub fn data_point_iter(&self) -> impl Iterator<Item = DpId> + '_ {
        let available = self.available_data_points.iter().copied();
        // Older versions will have data point journals in the work stack.
        let work_stack = self.work_stack_iterator().map(|journal| journal.id());
        available.chain(work_stack)
    }
}
