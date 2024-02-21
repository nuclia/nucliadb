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
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::SystemTime;

pub use merger::Merger;
use nucliadb_core::fs_state::{self, ELock, Lock, SLock, Version};
use nucliadb_core::tracing::*;
use nucliadb_core::Channel;
use serde::{Deserialize, Serialize};
use state::*;

pub use crate::data_point::Neighbour;
use crate::data_point::{DataPoint, DpId, Similarity};
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

pub struct IndexInner {
    metadata: IndexMetadata,
    state: RwLock<State>,
    date: RwLock<Version>,
    location: PathBuf,
    dimension: RwLock<Option<u64>>,
    merge_lock: Mutex<()>,
    alive: AtomicBool,
}
pub struct Index(Arc<IndexInner>);
impl Drop for Index {
    fn drop(&mut self) {
        self.0.alive.store(false, std::sync::atomic::Ordering::Release);
    }
}
impl IndexInner {
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
        let location = &self.location;
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
    fn do_merge(&self) -> bool {
        if let Ok(_mutex) = self.merge_lock.try_lock() {
            let _lock = self.get_slock().unwrap();

            let state_guard = self.read_state();
            let state = state_guard.clone();
            drop(state_guard);

            let Some(work) = state.current_work_unit().map(|work| {
                work.iter().rev().map(|journal| (state.delete_log(*journal), journal.id())).collect::<Vec<_>>()
            }) else {
                return false;
            };

            let new_dp =
                DataPoint::merge(&self.location, &work, self.metadata.similarity, self.metadata.channel).unwrap();

            if !self.alive.load(std::sync::atomic::Ordering::Acquire) {
                warn!("Shard was closed while merge was running, discarding it");
                return false;
            }

            let mut state = self.write_state();
            state.replace_work_unit(new_dp.journal());
            let mut date = self.write_date();
            fs_state::persist_state::<State>(&self.location, &state).unwrap();
            *date = fs_state::crnt_version(&self.location).unwrap();

            info!("Merge request completed for {:?}", &self.location);

            state.work_stack_len() > 5
        } else {
            warn!("Cannot merge, a merge was already running");
            false
        }
    }
    pub fn get_slock(&self) -> VectorR<SLock> {
        let lock = fs_state::shared_lock(&self.location)?;
        self.update(&lock)?;
        Ok(lock)
    }
}
impl Index {
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
        let index = Arc::new(IndexInner {
            metadata,
            dimension: RwLock::new(dimension_used),
            state: RwLock::new(state),
            date: RwLock::new(date),
            location: path.to_path_buf(),
            merge_lock: Mutex::default(),
            alive: AtomicBool::new(true),
        });
        if index.read_state().work_stack_len() > ALLOWED_BEFORE_MERGE {
            merger::send_merge_request(path.to_string_lossy().into(), Worker::request(index.clone()))
        }
        Ok(Index(index))
    }
    pub fn new(path: &Path, metadata: IndexMetadata) -> VectorR<Index> {
        std::fs::create_dir(path)?;
        fs_state::initialize_disk(path, State::new)?;
        metadata.write(path)?;
        let state = fs_state::load_state::<State>(path)?;
        let date = fs_state::crnt_version(path)?;
        let index = IndexInner {
            metadata,
            dimension: RwLock::new(None),
            state: RwLock::new(state),
            date: RwLock::new(date),
            location: path.to_path_buf(),
            merge_lock: Mutex::default(),
            alive: AtomicBool::new(true),
        };
        Ok(Index(Arc::new(index)))
    }
    pub fn delete(&self, prefix: impl AsRef<str>, temporal_mark: SystemTime, _: &Lock) {
        let mut state = self.0.write_state();
        state.remove(prefix.as_ref(), temporal_mark);
    }
    pub fn commit(&self, _lock: &Lock) -> VectorR<()> {
        let state = self.0.write_state();
        let mut date = self.0.write_date();

        fs_state::persist_state::<State>(self.location(), &state)?;
        *date = fs_state::crnt_version(self.location())?;
        let work_stack_len = state.work_stack_len();

        if work_stack_len > ALLOWED_BEFORE_MERGE {
            merger::send_merge_request(self.location().to_string_lossy().into(), Worker::request(self.0.clone()))
        }
        std::mem::drop(state);
        std::mem::drop(date);

        Ok(())
    }
    pub fn search(&self, request: &dyn SearchRequest, _: &Lock) -> VectorR<Vec<Neighbour>> {
        let state = self.0.read_state();
        let given_len = request.get_query().len() as u64;
        match self.0.get_dimension() {
            Some(expected) if expected != given_len => Err(VectorErr::InconsistentDimensions),
            None => Ok(Vec::with_capacity(0)),
            Some(_) => state.search(&self.0.location, request, self.0.metadata.similarity),
        }
    }
    pub fn get_keys(&self, _: &Lock) -> VectorR<Vec<String>> {
        self.0.read_state().keys(&self.0.location)
    }
    pub fn no_nodes(&self, _: &Lock) -> usize {
        self.0.read_state().no_nodes()
    }
    pub fn collect_garbage(&self, _lock: &ELock) -> VectorR<()> {
        // At this point there are no merges available, so we can
        // start collecting garbage.
        let state = self.0.read_state();
        let in_use_dp: HashSet<_> = state.dpid_iter().collect();
        for dir_entry in std::fs::read_dir(&self.0.location)? {
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
                let Err(err) = DataPoint::delete(&self.0.location, dpid) else {
                    continue;
                };
                warn!("{name} is garbage and could not be deleted because of {err}");
            }
        }
        Ok(())
    }
    pub fn add(&self, dp: DataPoint, _lock: &Lock) -> VectorR<()> {
        let mut state = self.0.write_state();
        let Some(new_dp_vector_len) = dp.stored_len() else {
            return Ok(());
        };
        let Some(state_vector_len) = self.0.get_dimension() else {
            self.0.set_dimension(dp.stored_len());
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
    pub fn try_elock(&self) -> VectorR<ELock> {
        let lock = fs_state::try_exclusive_lock(&self.0.location)?;
        self.0.update(&lock)?;
        Ok(lock)
    }
    pub fn get_elock(&self) -> VectorR<ELock> {
        let lock = fs_state::exclusive_lock(&self.0.location)?;
        self.0.update(&lock)?;
        Ok(lock)
    }
    pub fn get_slock(&self) -> VectorR<SLock> {
        self.0.get_slock()
    }
    pub fn location(&self) -> &Path {
        &self.0.location
    }
    pub fn metadata(&self) -> &IndexMetadata {
        &self.0.metadata
    }
    pub fn get_dimension(&self) -> Option<u64> {
        self.0.get_dimension()
    }
}

#[cfg(test)]
mod test {
    use nucliadb_core::NodeResult;

    use super::*;
    use crate::data_point::{Elem, LabelDictionary, Similarity};
    #[test]
    fn garbage_collection_test() -> NodeResult<()> {
        let dir = tempfile::tempdir()?;
        let vectors_path = dir.path().join("vectors");
        let index = Index::new(&vectors_path, IndexMetadata::default())?;
        let lock = index.get_elock()?;

        let empty_no_entries = std::fs::read_dir(&vectors_path)?.count();
        for _ in 0..10 {
            DataPoint::new(&vectors_path, vec![], None, Similarity::Cosine, Channel::EXPERIMENTAL).unwrap();
        }

        index.collect_garbage(&lock)?;
        let no_entries = std::fs::read_dir(&vectors_path)?.count();
        assert_eq!(no_entries, empty_no_entries);
        Ok(())
    }

    #[test]
    fn test_delete_get_keys() -> NodeResult<()> {
        let dir = tempfile::tempdir()?;
        let vectors_path = dir.path().join("vectors");
        let index = Index::new(&vectors_path, IndexMetadata::default())?;
        let lock = index.get_slock().unwrap();

        let data_point = DataPoint::new(
            &vectors_path,
            vec![
                Elem::new("key_0".to_string(), vec![1.0], LabelDictionary::default(), None),
                Elem::new("key_1".to_string(), vec![1.0], LabelDictionary::default(), None),
            ],
            None,
            Similarity::Cosine,
            Channel::EXPERIMENTAL,
        )
        .unwrap();
        index.add(data_point, &lock).unwrap();
        index.commit(&lock).unwrap();

        assert_eq!(index.get_keys(&lock).unwrap(), vec!["key_0".to_string(), "key_1".to_string()]);

        index.delete("key_0", SystemTime::now(), &lock);
        assert_eq!(index.get_keys(&lock).unwrap(), vec!["key_1".to_string()]);

        index.delete("key", SystemTime::now(), &lock);
        assert!(index.get_keys(&lock).unwrap().is_empty());

        Ok(())
    }
}
