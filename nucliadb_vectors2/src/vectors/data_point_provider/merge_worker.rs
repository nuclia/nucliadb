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
use std::time::SystemTime;

use tracing::*;

use super::{State, VectorR};
use crate::disk::directory;
use crate::utils::merger::{MergeQuery, MergeRequest};
use crate::vectors::data_point::{DataPoint, DpId};

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
        let subscriber = self.0.as_path();
        let lock = directory::shared_lock(subscriber)?;
        let state: State = directory::load_state(&lock)?;
        let time = SystemTime::now();
        std::mem::drop(lock);
        if let Some(work) = state.get_work() {
            let work: Vec<_> = work
                .iter()
                .rev()
                .map(|j| (state.create_dlog(*j), j.id()))
                .collect();
            let new_dp = DataPoint::merge(subscriber, &work)?;
            let ids: Vec<_> = work.into_iter().map(|(_, v)| v).collect();
            std::mem::drop(state);

            let report = self.merge_report(ids.iter().copied(), new_dp.meta().id());
            let lock = directory::exclusive_lock(subscriber)?;
            let mut state: State = directory::load_state(&lock)?;
            state.replace_work_unit(new_dp, time);
            directory::persist_state(&lock, &state)?;
            std::mem::drop(lock);
            info!("Merge on {subscriber:?}:\n{report}");
            ids.into_iter()
                .map(|dp| (subscriber, dp, DataPoint::delete(subscriber, dp)))
                .filter(|(.., r)| r.is_err())
                .for_each(|(s, id, ..)| tracing::info!("Error while deleting {s:?}/{id}"));
        }
        Ok(())
    }
}
