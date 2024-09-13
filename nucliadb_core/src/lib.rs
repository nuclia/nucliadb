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

pub mod fs_state;
pub mod merge;
pub mod metrics;
pub mod paragraphs;
pub mod query_language;
pub mod query_planner;
pub mod relations;
pub mod tantivy_replica;
pub mod texts;
pub mod vectors;

pub mod protos {
    pub use nucliadb_protos::prelude::*;
    pub use {prost, prost_types};
}

pub mod tracing {
    pub use tracing::*;
}

pub mod thread {
    pub use rayon::prelude::*;
    pub use rayon::*;
}

pub mod prelude {
    pub use crate::{node_error, read_rw_lock, write_rw_lock, Context, NodeResult};
}

use std::collections::HashMap;
use std::fs::File;
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

pub use anyhow::{anyhow as node_error, Context, Error};

use crate::tantivy_replica::TantivyReplicaState;
pub type NodeResult<O> = anyhow::Result<O>;

#[derive(Debug, Default)]
pub struct RawReplicaState {
    pub metadata_files: HashMap<String, Vec<u8>>,
    pub files: Vec<(String, File)>,
}

impl RawReplicaState {
    pub fn extend(&mut self, other: RawReplicaState) {
        self.metadata_files.extend(other.metadata_files);
        self.files.extend(other.files);
    }
}

pub enum IndexFiles {
    Tantivy(TantivyReplicaState),
    Other(RawReplicaState),
}

pub fn read_rw_lock<T: ?Sized>(rw_lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    rw_lock.read().unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub fn write_rw_lock<T: ?Sized>(rw_lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    rw_lock.write().unwrap_or_else(|poisoned| poisoned.into_inner())
}
