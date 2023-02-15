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

mod merge_worker;
mod merger;
mod state;
mod work_flag;
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::SystemTime;

use nucliadb_core::fs_state::{self, ELock, Lock, SLock, Version};
use nucliadb_core::tracing::*;
use state::*;
use work_flag::MergerWriterSync;

use crate::data_point::{DataPoint, DpId};
use crate::VectorR;
pub type TemporalMark = SystemTime;

pub trait SearchRequest {
    fn get_query(&self) -> &[f32];
    fn get_labels(&self) -> &[String];
    fn no_results(&self) -> usize;
    fn with_duplicates(&self) -> bool;
}

#[derive(Clone, Copy, Debug)]
pub enum IndexCheck {
    None,
    Sanity,
}

pub struct Index {
    work_flag: MergerWriterSync,
    state: RwLock<State>,
    date: RwLock<Version>,
    location: PathBuf,
}
impl Index {
    fn read_state(&self) -> RwLockReadGuard<'_, State> {
        self.state.read().unwrap_or_else(|e| e.into_inner())
    }
    fn write_state(&self) -> RwLockWriteGuard<'_, State> {
        self.state.write().unwrap_or_else(|e| e.into_inner())
    }
    fn read_date(&self) -> RwLockReadGuard<'_, Version> {
        self.date.read().unwrap_or_else(|e| e.into_inner())
    }
    fn write_date(&self) -> RwLockWriteGuard<'_, Version> {
        self.date.write().unwrap_or_else(|e| e.into_inner())
    }
    fn update(&self, lock: &Lock) -> VectorR<()> {
        let disk_v = fs_state::crnt_version(lock)?;
        let date = self.read_date();
        if disk_v > *date {
            mem::drop(date);
            let new_state = fs_state::load_state(lock)?;
            let mut state = self.write_state();
            let mut date = self.write_date();
            *state = new_state;
            *date = disk_v;
            mem::drop(date);
            mem::drop(state);
        }
        Ok(())
    }
    fn notify_merger(&self) {
        use merge_worker::Worker;
        let notifier = merger::get_notifier();
        let worker = Worker::request(self.location.clone(), self.work_flag.clone());
        if let Err(e) = notifier.send(worker) {
            tracing::info!("Could not request merge: {}", e);
        }
    }
    pub fn new(path: &Path, with_check: IndexCheck) -> VectorR<Index> {
        if !path.exists() {
            std::fs::create_dir_all(path)?;
        }
        fs_state::initialize_disk(path, State::new)?;
        let lock = fs_state::shared_lock(path)?;
        let state = fs_state::load_state::<State>(&lock)?;
        let date = fs_state::crnt_version(&lock)?;
        let index = Index {
            work_flag: MergerWriterSync::new(),
            state: RwLock::new(state),
            date: RwLock::new(date),
            location: path.to_path_buf(),
        };
        if let IndexCheck::Sanity = with_check {
            let mut state = index.write_state();
            let merge_work = state.work_stack_len();
            (0..merge_work).for_each(|_| index.notify_merger());
        }
        Ok(index)
    }
    pub fn delete(&self, prefix: impl AsRef<str>, temporal_mark: SystemTime, _: &ELock) {
        let mut state = self.write_state();
        state.remove(prefix.as_ref(), temporal_mark);
    }
    pub fn get_keys(&self, _: &Lock) -> VectorR<Vec<String>> {
        self.read_state().keys(&self.location)
    }
    pub fn search(&self, request: &dyn SearchRequest, _: &Lock) -> VectorR<Vec<(String, f32)>> {
        self.read_state().search(&self.location, request)
    }
    pub fn no_nodes(&self, _: &Lock) -> usize {
        self.read_state().no_nodes()
    }
    pub fn collect_garbage(&self, _: &Lock) -> VectorR<()> {
        use std::collections::HashSet;
        let work_flag = self.work_flag.try_to_start_working()?;
        let state = self.read_state();
        let in_use_dp: HashSet<_> = state.dpid_iter().collect();
        for dir_entry in std::fs::read_dir(&self.location)? {
            let entry = dir_entry?;
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().to_string();
            if path.is_file() {
                continue;
            }
            let Ok(dpid) = DpId::parse_str(&name) else {
                info!("Unknown item {path:?} found");
                continue;
            };
            if !in_use_dp.contains(&dpid) {
                info!("found garbage {name}");
                let Err(err)  = DataPoint::delete(&self.location, dpid) else { continue };
                warn!("{name} is garbage and could not be deleted because of {err}");
            }
        }
        std::mem::drop(work_flag);
        Ok(())
    }
    pub fn add(&self, dp: DataPoint, _: &ELock) {
        let mut state = self.write_state();
        if state.add(dp) {
            self.notify_merger()
        }
    }
    pub fn commit(&self, lock: ELock) -> VectorR<()> {
        let state = self.read_state();
        let mut date = self.write_date();
        fs_state::persist_state::<State>(&lock, &state)?;
        *date = fs_state::crnt_version(&lock)?;
        Ok(())
    }
    pub fn get_elock(&self) -> VectorR<ELock> {
        let lock = fs_state::exclusive_lock(&self.location)?;
        self.update(&lock)?;
        Ok(lock)
    }
    pub fn get_slock(&self) -> VectorR<SLock> {
        let lock = fs_state::shared_lock(&self.location)?;
        self.update(&lock)?;
        Ok(lock)
    }
    pub fn location(&self) -> &Path {
        &self.location
    }
}

#[cfg(test)]
mod test {
    use nucliadb_core::NodeResult;

    use super::*;
    #[test]
    fn garbage_collection_test() -> NodeResult<()> {
        let dir = tempfile::tempdir()?;
        let index = Index::new(dir.path(), IndexCheck::None)?;
        let empty_no_entries = std::fs::read_dir(dir.path())?.count();
        for _ in 0..10 {
            DataPoint::new(dir.path(), vec![], None).unwrap();
        }
        let lock = index.get_slock()?;
        index.collect_garbage(&lock)?;
        let no_entries = std::fs::read_dir(dir.path())?.count();
        assert_eq!(no_entries, empty_no_entries);
        Ok(())
    }
}
