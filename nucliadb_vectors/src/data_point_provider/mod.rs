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
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::SystemTime;

use crossbeam::channel::{self, Receiver};
pub use merger::Merger;
use nucliadb_core::fs_state::{self, ELock, Lock, SLock, Version};
use nucliadb_core::tracing::*;
use nucliadb_core::Channel;
use serde::{Deserialize, Serialize};
use state::*;

pub use crate::data_point::Neighbour;
use crate::data_point::{DataPoint, DpId, Journal, Similarity};
use crate::data_point_provider::merge_worker::Worker;
use crate::formula::Formula;
use crate::{VectorErr, VectorR};
pub type TemporalMark = SystemTime;

const METADATA: &str = "metadata.json";
const ALLOWED_BEFORE_MERGE: usize = 5;

pub trait SearchRequest {
    fn get_query(&self) -> &[f32];
    fn get_filter(&self) -> &Formula;
    fn no_results(&self) -> usize;
    fn with_duplicates(&self) -> bool;
    fn min_score(&self) -> f32;
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct IndexMetadata {
    #[serde(default)]
    pub similarity: Similarity,
    #[serde(default)]
    pub channel: Channel,
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

#[derive(Default)]
enum MergerStatus {
    #[default]
    Free,
    WorkScheduled(Receiver<Journal>),
}

pub struct Index {
    metadata: IndexMetadata,
    merger_status: MergerStatus,
    state: RwLock<State>,
    date: RwLock<Version>,
    pub location: PathBuf,
    dimension: RwLock<Option<u64>>,
}
impl Index {
    pub fn get_dimension(&self) -> Option<u64> {
        *self.dimension.read().unwrap_or_else(|e| e.into_inner())
    }
    fn set_dimension(&self, dimension: Option<u64>) {
        *self.dimension.write().unwrap_or_else(|e| e.into_inner()) = dimension;
    }
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
    fn update(&self, _lock: &Lock) -> VectorR<()> {
        let location = self.location();
        let disk_v = fs_state::crnt_version(location)?;
        let date = self.read_date();
        if disk_v > *date {
            mem::drop(date);
            let new_state: State = fs_state::load_state(location)?;
            let new_dimension = new_state.stored_len(location)?;
            let mut state = self.write_state();
            let mut date = self.write_date();
            *state = new_state;
            *date = disk_v;
            mem::drop(date);
            mem::drop(state);
            self.set_dimension(new_dimension);
        }
        Ok(())
    }
    pub fn open(path: &Path) -> VectorR<Index> {
        let state = fs_state::load_state::<State>(path)?;
        let date = fs_state::crnt_version(path)?;
        let dimension_used = state.stored_len(path)?;
        let metadata = IndexMetadata::open(path)?.map(Ok).unwrap_or_else(|| {
            // Old indexes may not have this file so in that case the
            // metadata file they should have is created.
            let metadata = IndexMetadata::default();
            metadata.write(path).map(|_| metadata)
        })?;
        let index = Index {
            metadata,
            merger_status: MergerStatus::Free,
            dimension: RwLock::new(dimension_used),
            state: RwLock::new(state),
            date: RwLock::new(date),
            location: path.to_path_buf(),
        };
        Ok(index)
    }
    pub fn new(path: &Path, metadata: IndexMetadata) -> VectorR<Index> {
        std::fs::create_dir(path)?;
        fs_state::initialize_disk(path, State::new)?;
        metadata.write(path)?;
        let state = fs_state::load_state::<State>(path)?;
        let date = fs_state::crnt_version(path)?;
        let index = Index {
            metadata,
            merger_status: MergerStatus::Free,
            dimension: RwLock::new(None),
            state: RwLock::new(state),
            date: RwLock::new(date),
            location: path.to_path_buf(),
        };
        Ok(index)
    }
    pub fn delete(&self, prefix: impl AsRef<str>, temporal_mark: SystemTime, _: &Lock) {
        let mut state = self.write_state();
        state.remove(prefix.as_ref(), temporal_mark);
    }
    pub fn get_keys(&self, _: &Lock) -> VectorR<Vec<String>> {
        self.read_state().keys(&self.location)
    }
    pub fn search(&self, request: &dyn SearchRequest, _: &Lock) -> VectorR<Vec<Neighbour>> {
        let state = self.read_state();
        let given_len = request.get_query().len() as u64;
        match self.get_dimension() {
            Some(expected) if expected != given_len => Err(VectorErr::InconsistentDimensions),
            None => Ok(Vec::with_capacity(0)),
            Some(_) => state.search(&self.location, request, self.metadata.similarity),
        }
    }
    pub fn no_nodes(&self, _: &Lock) -> usize {
        self.read_state().no_nodes()
    }
    pub fn collect_garbage(&mut self, _lock: &ELock) -> VectorR<()> {
        // A merge may be waiting to be recorded.
        let possible_merge = self.take_available_merge();
        let mut state = self.write_state();
        let mut date = self.write_date();
        if let Some(journal) = possible_merge {
            state.replace_work_unit(journal)
        }
        fs_state::persist_state::<State>(&self.location, &state)?;
        *date = fs_state::crnt_version(&self.location)?;
        std::mem::drop(state);
        std::mem::drop(date);

        // At this point there are no merges available, so we can
        // start collecting garbage.
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
                let Err(err) = DataPoint::delete(&self.location, dpid) else {
                    continue;
                };
                warn!("{name} is garbage and could not be deleted because of {err}");
            }
        }
        Ok(())
    }
    pub fn add(&mut self, dp: DataPoint, _lock: &Lock) -> VectorR<()> {
        let mut state = self.write_state();
        let Some(new_dp_vector_len) = dp.stored_len() else {
            return Ok(());
        };
        let Some(state_vector_len) = self.get_dimension() else {
            self.set_dimension(dp.stored_len());
            state.add(dp.journal());
            std::mem::drop(state);
            return Ok(());
        };
        if state_vector_len != new_dp_vector_len {
            return Err(VectorErr::InconsistentDimensions);
        }
        state.add(dp.journal());
        Ok(())
    }

    fn take_available_merge(&mut self) -> Option<Journal> {
        let MergerStatus::WorkScheduled(rcv) = std::mem::take(&mut self.merger_status) else {
            return None;
        };
        match rcv.try_recv() {
            Ok(journal) => Some(journal),
            Err(channel::TryRecvError::Disconnected) => None,
            Err(channel::TryRecvError::Empty) => {
                self.merger_status = MergerStatus::WorkScheduled(rcv);
                None
            }
        }
    }
    pub fn commit(&mut self, _lock: &Lock) -> VectorR<()> {
        let possible_merge = self.take_available_merge();
        let mut state = self.write_state();
        let mut date = self.write_date();
        if let Some(journal) = possible_merge {
            state.replace_work_unit(journal)
        }

        fs_state::persist_state::<State>(&self.location, &state)?;
        *date = fs_state::crnt_version(&self.location)?;
        let work_stack_len = state.work_stack_len();
        std::mem::drop(state);
        std::mem::drop(date);

        if matches!(self.merger_status, MergerStatus::Free) && work_stack_len > ALLOWED_BEFORE_MERGE
        {
            let location = self.location.clone();
            let similarity = self.metadata.similarity;
            let (sender, receiver) = channel::unbounded();
            let worker = Worker::request(location, sender, similarity, self.metadata.channel);
            self.merger_status = MergerStatus::WorkScheduled(receiver);
            merger::send_merge_request(worker);
        }
        Ok(())
    }
    pub fn try_elock(&self) -> VectorR<ELock> {
        let lock = fs_state::try_exclusive_lock(&self.location)?;
        self.update(&lock)?;
        Ok(lock)
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
        let vectors_path = dir.path().join("vectors");
        let mut index = Index::new(&vectors_path, IndexMetadata::default())?;
        let lock = index.get_elock()?;

        let empty_no_entries = std::fs::read_dir(&vectors_path)?.count();
        for _ in 0..10 {
            DataPoint::new(
                &vectors_path,
                vec![],
                None,
                Similarity::Cosine,
                Channel::EXPERIMENTAL,
            )
            .unwrap();
        }

        index.collect_garbage(&lock)?;
        let no_entries = std::fs::read_dir(&vectors_path)?.count();
        assert_eq!(no_entries, empty_no_entries);
        Ok(())
    }
}
