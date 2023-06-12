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
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::SystemTime;

pub use merger::Merger;
use nucliadb_core::fs_state::{self, ELock, Lock, SLock, Version};
use nucliadb_core::tracing::*;
use serde::{Deserialize, Serialize};
use state::*;
use work_flag::MergerWriterSync;

pub use crate::data_point::Neighbour;
use crate::data_point::{DataPoint, DpId, Similarity};
use crate::data_point_provider::merge_worker::Worker;
use crate::formula::Formula;
use crate::VectorR;
pub type TemporalMark = SystemTime;

const METADATA: &str = "metadata.json";

pub trait SearchRequest {
    fn get_query(&self) -> &[f32];
    fn get_filter(&self) -> &Formula;
    fn no_results(&self) -> usize;
    fn with_duplicates(&self) -> bool;
}

#[derive(Clone, Copy, Debug)]
pub enum IndexCheck {
    None,
    Sanity,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct IndexMetadata {
    #[serde(default)]
    pub similarity: Similarity,
}
impl IndexMetadata {
    pub fn write(&self, path: &Path) -> VectorR<()> {
        let mut writer = BufWriter::new(File::create(path.join(METADATA))?);
        serde_json::to_writer(&mut writer, self)?;
        Ok(writer.flush()?)
    }
    pub fn open(path: &Path) -> VectorR<Option<IndexMetadata>> {
        let path = &path.join(METADATA);
        if !path.is_file() {
            return Ok(None);
        }
        let mut reader = BufReader::new(File::open(path)?);
        Ok(Some(serde_json::from_reader(&mut reader)?))
    }
}

pub struct Index {
    metadata: IndexMetadata,
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
        let worker = Worker::request(
            self.location.clone(),
            self.work_flag.clone(),
            self.metadata.similarity,
        );
        merger::send_merge_request(worker);
    }
    pub fn open(path: &Path, with_check: IndexCheck) -> VectorR<Index> {
        let lock = fs_state::shared_lock(path)?;
        let state = fs_state::load_state::<State>(&lock)?;
        let date = fs_state::crnt_version(&lock)?;
        let metadata = IndexMetadata::open(path)?.map(Ok).unwrap_or_else(|| {
            // Old indexes may not have this file so in that case the
            // metadata file they should have is created.
            let metadata = IndexMetadata::default();
            metadata.write(path).map(|_| metadata)
        })?;
        let index = Index {
            metadata,
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
    pub fn new(path: &Path, metadata: IndexMetadata) -> VectorR<Index> {
        std::fs::create_dir_all(path)?;
        fs_state::initialize_disk(path, State::new)?;
        metadata.write(path)?;
        let lock = fs_state::shared_lock(path)?;
        let state = fs_state::load_state::<State>(&lock)?;
        let date = fs_state::crnt_version(&lock)?;
        let index = Index {
            metadata,
            work_flag: MergerWriterSync::new(),
            state: RwLock::new(state),
            date: RwLock::new(date),
            location: path.to_path_buf(),
        };
        Ok(index)
    }
    pub fn delete(&self, prefix: impl AsRef<str>, temporal_mark: SystemTime, _: &ELock) {
        let mut state = self.write_state();
        state.remove(prefix.as_ref(), temporal_mark);
    }
    pub fn get_keys(&self, _: &Lock) -> VectorR<Vec<String>> {
        self.read_state().keys(&self.location)
    }
    pub fn search(&self, request: &dyn SearchRequest, _: &Lock) -> VectorR<Vec<Neighbour>> {
        self.read_state()
            .search(&self.location, request, self.metadata.similarity)
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
    pub fn metadata(&self) -> &IndexMetadata {
        &self.metadata
    }
}

#[cfg(test)]
mod test {
    use nucliadb_core::NodeResult;

    use super::*;
    use crate::data_point::Similarity;
    #[test]
    fn garbage_collection_test() -> NodeResult<()> {
        let dir = tempfile::tempdir()?;
        let index = Index::new(dir.path(), IndexMetadata::default())?;
        let empty_no_entries = std::fs::read_dir(dir.path())?.count();
        for _ in 0..10 {
            DataPoint::new(dir.path(), vec![], None, Similarity::Cosine).unwrap();
        }
        let lock = index.get_slock()?;
        index.collect_garbage(&lock)?;
        let no_entries = std::fs::read_dir(dir.path())?.count();
        assert_eq!(no_entries, empty_no_entries);
        Ok(())
    }
}
