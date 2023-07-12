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

use std::path::PathBuf;
use std::sync::{Arc, RwLock, RwLockWriteGuard};
use std::time::SystemTime;

use nucliadb_core::tracing::*;

use super::merger::MergeQuery;
use crate::data_point::{DataPoint, DpId, Journal, Similarity};
use crate::data_types::dtrie_ram::{DTrie, TimeSensitiveDTrie};
use crate::{VectorErr, VectorR};

#[derive(Debug, Clone, Copy, Default)]
pub enum MergingState {
    #[default]
    Empty,
    Waiting,
    Done(Journal),
}

#[derive(Clone, Default)]
pub struct Merged {
    inner: Arc<RwLock<MergingState>>,
}
impl Merged {
    fn write(&self) -> RwLockWriteGuard<'_, MergingState> {
        self.inner.write().unwrap_or_else(|e| e.into_inner())
    }
    fn add_merge(&self, datapoint: Journal) -> VectorR<()> {
        let mut writer = self.write();
        if let MergingState::Done(_) = *writer {
            Err(VectorErr::MergeLostError(datapoint.id()))
        } else {
            *writer = MergingState::Done(datapoint);
            Ok(())
        }
    }
    pub fn new() -> Merged {
        Self::default()
    }
    pub fn abort_work(&self) {
        let mut writer = self.write();
        if let MergingState::Waiting = *writer {
            *writer = MergingState::Empty;
        }
    }
    pub fn take(&self) -> Option<Journal> {
        let mut writer = self.write();
        let MergingState::Done(journal) = *writer else {
            return None;
        };
        *writer = MergingState::Empty;
        Some(journal)
    }
    pub fn reserve_work(&self) -> bool {
        let mut writer = self.write();
        if let MergingState::Empty = *writer {
            *writer = MergingState::Waiting;
            true
        } else {
            false
        }
    }
}

pub(super) struct Worker {
    pub(super) location: PathBuf,
    pub(super) delete_log: DTrie,
    pub(super) work: Vec<(DpId, SystemTime)>,
    pub(super) similarity: Similarity,
    pub(super) result: Merged,
}
impl MergeQuery for Worker {
    fn do_work(&self) -> VectorR<()> {
        self.work()
    }
}
impl Worker {
    fn merge_report<It>(&self, old: It, new: DpId) -> String
    where It: Iterator<Item = DpId> {
        use std::fmt::Write;
        let mut msg = String::new();
        for (id, dp_id) in old.enumerate() {
            writeln!(msg, "  ({id}) {dp_id}").unwrap();
        }
        write!(msg, "==> {new}").unwrap();
        msg
    }
    fn work(&self) -> VectorR<()> {
        let mut work_stack = vec![];
        let subscriber = self.location.as_path();
        for (id, ctime) in self.work.iter().copied() {
            work_stack.push((TimeSensitiveDTrie::new(&self.delete_log, ctime), id))
        }
        info!("{subscriber:?} is ready to perform a merge");
        match DataPoint::merge(&self.location, &work_stack, self.similarity) {
            Err(err) => {
                self.result.abort_work();
                Err(err)
            }
            Ok(merged) => {
                let report = self.merge_report(self.work.iter().map(|i| i.0), merged.get_id());
                info!("Merge on {subscriber:?}:\n{report}");
                info!("Merge request completed");
                self.result.abort_work();
                self.result.add_merge(merged.meta())
            }
        }
    }
}
