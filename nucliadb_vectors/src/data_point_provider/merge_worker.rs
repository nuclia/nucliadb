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
use std::time::Duration;

use nucliadb_core::fs_state;
use tracing::*;

use super::merger::{MergeQuery, MergeRequest};
use super::work_flag::MergerWriterSync;
use super::State;
use crate::data_point::{DataPoint, DpId};
use crate::VectorR;

const SLEEP_TIME: Duration = Duration::from_millis(100);
pub struct Worker {
    location: PathBuf,
    work_flag: MergerWriterSync,
}
impl MergeQuery for Worker {
    fn do_work(&self) -> VectorR<()> {
        self.work()
    }
}
impl Worker {
    pub fn request(location: PathBuf, work_flag: MergerWriterSync) -> MergeRequest {
        Box::new(Worker {
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
        use crate::data_point_provider::merger;
        let notifier = merger::get_notifier();
        let worker = Worker::request(self.location.clone(), self.work_flag.clone());
        if let Err(e) = notifier.send(worker) {
            tracing::info!("Could not request merge: {}", e);
        }
    }
    fn work(&self) -> VectorR<()> {
        while self.work_flag.try_to_start_working().is_err() {}
        let work_flag = loop {
            if let Ok(lock) = self.work_flag.try_to_start_working() {
                break lock;
            }
            tracing::info!("Merge delayed at: {:?}", self.location);
            std::thread::sleep(SLEEP_TIME);
        };
        let subscriber = self.location.as_path();
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
        let new_dp = DataPoint::merge(subscriber, &work)?;
        let ids: Vec<_> = work.into_iter().map(|(_, v)| v).collect();
        std::mem::drop(state);

        let report = self.merge_report(ids.iter().copied(), new_dp.meta().id());

        let lock = fs_state::exclusive_lock(subscriber)?;
        let mut state: State = fs_state::load_state(&lock)?;
        let creates_work = state.replace_work_unit(new_dp);
        fs_state::persist_state(&lock, &state)?;
        std::mem::drop(lock);
        std::mem::drop(work_flag);
        info!("Merge on {subscriber:?}:\n{report}");
        if creates_work {
            self.notify_merger();
        }
        Ok(())
    }
}
