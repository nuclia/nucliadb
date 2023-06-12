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
use std::sync::MutexGuard;
use std::time::Duration;

use nucliadb_core::fs_state;
use nucliadb_core::tracing::*;

use super::merger::{MergeQuery, MergeRequest};
use super::work_flag::MergerWriterSync;
use super::State;
use crate::data_point::{DataPoint, DpId, Similarity};
use crate::data_point_provider::merger;
use crate::VectorR;

const SLEEP_TIME: Duration = Duration::from_millis(100);
pub(crate) struct Worker {
    location: PathBuf,
    work_flag: MergerWriterSync,
    similarity: Similarity,
}
impl MergeQuery for Worker {
    fn do_work(&self) -> VectorR<()> {
        self.work()
    }
}
impl Worker {
    pub(crate) fn request(
        location: PathBuf,
        work_flag: MergerWriterSync,
        similarity: Similarity,
    ) -> MergeRequest {
        Box::new(Worker {
            similarity,
            location,
            work_flag,
        })
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
    fn notify_merger(&self) {
        let worker = Worker::request(
            self.location.clone(),
            self.work_flag.clone(),
            self.similarity,
        );
        merger::send_merge_request(worker);
    }
    fn try_to_work_or_delay(&self) -> MutexGuard<'_, ()> {
        loop {
            match self.work_flag.try_to_start_working() {
                Ok(lock) => break lock,
                Err(_) => {
                    info!("Merge delayed at: {:?}", self.location);
                    std::thread::sleep(SLEEP_TIME);
                }
            }
        }
    }
    fn work(&self) -> VectorR<()> {
        let work_flag = self.try_to_work_or_delay();

        let subscriber = self.location.as_path();
        info!("{subscriber:?} is ready to perform a merge");
        let lock = fs_state::shared_lock(subscriber)?;
        let state: State = fs_state::load_state(&lock)?;
        std::mem::drop(lock);

        let Some(work) = state.current_work_unit().map(|work|
            work
            .iter()
            .rev()
            .map(|journal| (state.delete_log(*journal), journal.id()))
            .collect::<Vec<_>>()
        ) else { return Ok(());};
        let new_dp = DataPoint::merge(subscriber, &work, self.similarity)?;
        let ids: Vec<_> = work.into_iter().map(|(_, v)| v).collect();
        std::mem::drop(state);

        let report = self.merge_report(ids.iter().copied(), new_dp.meta().id());

        let lock = fs_state::exclusive_lock(subscriber)?;
        let mut state: State = fs_state::load_state(&lock)?;
        let creates_work = state.replace_work_unit(new_dp);
        fs_state::persist_state(&lock, &state)?;
        std::mem::drop(lock);
        info!("Merge on {subscriber:?}:\n{report}");
        if creates_work {
            self.notify_merger();
        }

        info!("Removing deprecated datapoints");
        ids.into_iter()
            .map(|dp| (subscriber, dp, DataPoint::delete(subscriber, dp)))
            .filter(|(.., r)| r.is_err())
            .for_each(|(s, id, ..)| info!("Error while deleting {s:?}/{id}"));
        std::mem::drop(work_flag);

        info!("Merge request completed");
        Ok(())
    }
}
