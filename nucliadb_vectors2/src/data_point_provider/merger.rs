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

use std::path::{Path, PathBuf};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Mutex;
use std::thread;

use super::{disk_handler, VectorR};
use crate::data_point::{DataPoint, DpId};

pub type MergeTxn = Sender<PathBuf>;

struct Updater {
    rtxn: Receiver<PathBuf>,
}

impl Updater {
    fn merge_report(&self, old: &[DpId], new: DpId) -> String {
        use std::fmt::Write;
        let mut msg = String::new();
        for (id, dp_id) in old.iter().copied().enumerate() {
            writeln!(msg, "  ({id}) {dp_id}").unwrap();
        }
        write!(msg, "==> {new}").unwrap();
        msg
    }
    fn do_work(&self, subscriber: &Path) -> VectorR<()> {
        let lock = disk_handler::shared_lock(subscriber)?;
        let state = disk_handler::load_state(&lock)?;
        std::mem::drop(lock);
        if let Some(work) = state.get_work() {
            let ids: Vec<_> = work.iter().map(|journal| journal.id()).collect();
            let new_dp = DataPoint::merge(subscriber, &ids, state.get_delete_log())?;
            std::mem::drop(state);

            let new_id = new_dp.meta().id();
            let lock = disk_handler::exclusive_lock(subscriber)?;
            let mut state = disk_handler::load_state(&lock)?;
            state.replace_work_unit(new_dp);
            disk_handler::persist_state(&lock, &state)?;
            std::mem::drop(lock);
            tracing::info!(
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
    pub fn new(rtxn: Receiver<PathBuf>) -> Updater {
        Updater { rtxn }
    }
    pub fn run(self) {
        loop {
            match self.rtxn.recv() {
                Err(err) => tracing::info!("channel error {}", err),
                Ok(path) => match self.do_work(&path) {
                    Ok(()) => (),
                    Err(err) => tracing::info!("merging error {}", err),
                },
            }
        }
    }
}

lazy_static::lazy_static! {
    static ref MERGER: Mutex<MergeTxn> = {
        let (stxn, rtxn) = channel();
        let updater = Updater::new(rtxn);
        thread::spawn(move || updater.run());
        Mutex::new(stxn)
    };
}

pub fn get_notifier() -> MergeTxn {
    MERGER.lock().unwrap().clone()
}
