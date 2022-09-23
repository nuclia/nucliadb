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

use super::{State, VectorR};
use crate::disk::directory;
use crate::utils::merger::{MergeQuery, MergeRequest};
use crate::vectors::data_point::{DataPoint, DpId};
use std::path::PathBuf;
use tracing::*;
pub struct Worker(PathBuf);
impl MergeQuery for Worker {
    fn do_work(&self) -> Result<(), String> {
        self.work()
            .map_err(|e| format!("Error in vectors worker {e}"))
    }
}
impl Worker {
    pub fn request(at: PathBuf) -> MergeRequest {
        Box::new(Worker(at))
    }
    fn merge_report(&self, old: &[DpId], new: DpId) -> String {
        use std::fmt::Write;
        let mut msg = String::new();
        for (id, dp_id) in old.iter().copied().enumerate() {
            writeln!(msg, "  ({id}) {dp_id}").unwrap();
        }
        write!(msg, "==> {new}").unwrap();
        msg
    }
    fn work(&self) -> VectorR<()> {
        let subscriber = self.0.as_path();
        let lock = directory::shared_lock(subscriber)?;
        let state: State = directory::load_state(&lock)?;
        std::mem::drop(lock);
        if let Some(work) = state.get_work() {
            let ids: Vec<_> = work.iter().map(|journal| journal.id()).collect();
            let new_dp = DataPoint::merge(subscriber, &ids, state.get_delete_log())?;
            std::mem::drop(state);

            let new_id = new_dp.meta().id();
            let lock = directory::exclusive_lock(subscriber)?;
            let mut state: State = directory::load_state(&lock)?;
            state.replace_work_unit(new_dp);
            directory::persist_state(&lock, &state)?;
            std::mem::drop(lock);
            info!(
                "Merge on {subscriber:?}:\n{}",
                self.merge_report(&ids, new_id)
            );
            ids.into_iter()
                .map(|dp| (subscriber, dp, DataPoint::delete(subscriber, dp)))
                .filter(|(.., r)| r.is_err())
                .for_each(|(s, id, ..)| tracing::info!("Error while deleting {s:?}/{id}"));
        }
        Ok(())
    }
}
