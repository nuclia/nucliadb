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

pub(crate) mod fs_state;
mod merge_worker;
mod merger;
mod segment_manager;
mod state_v1;

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::SystemTime;

use crossbeam::channel::{self, Receiver};
pub use merger::Merger;
use nucliadb_core::tracing::*;
use nucliadb_core::Channel;
use serde::{Deserialize, Serialize};

use self::merge_worker::Worker;
use self::segment_manager::{SegmentManager, Transaction};
pub use crate::data_point::Neighbour;
use crate::data_point::{DataPoint, DpId, Similarity};
use crate::formula::Formula;
use crate::{VectorErr, VectorR};

pub type TemporalMark = SystemTime;

const METADATA: &str = "metadata.json";
const ALLOWED_BEFORE_MERGE: usize = 10; // segments before merging

// Fixed-sized sorted collection
struct Fssc {
    size: usize,
    with_duplicates: bool,
    seen: HashSet<Vec<u8>>,
    buff: HashMap<Neighbour, f32>,
}
impl From<Fssc> for Vec<Neighbour> {
    fn from(fssv: Fssc) -> Self {
        let mut result: Vec<_> = fssv.buff.into_keys().collect();
        result.sort_by(|a, b| b.score().partial_cmp(&a.score()).unwrap_or(Ordering::Less));
        result
    }
}
impl Fssc {
    fn is_full(&self) -> bool {
        self.buff.len() == self.size
    }
    fn new(size: usize, with_duplicates: bool) -> Fssc {
        Fssc {
            size,
            with_duplicates,
            seen: HashSet::new(),
            buff: HashMap::with_capacity(size),
        }
    }
    fn add(&mut self, candidate: Neighbour) {
        if !self.with_duplicates && self.seen.contains(candidate.vector()) {
            return;
        } else if !self.with_duplicates {
            let vector = candidate.vector().to_vec();
            self.seen.insert(vector);
        }

        let score = candidate.score();
        if self.is_full() {
            let smallest_bigger = self
                .buff
                .iter()
                .map(|(key, score)| (key.clone(), *score))
                .filter(|(_, v)| score > *v)
                .min_by(|(_, v0), (_, v1)| v0.partial_cmp(v1).unwrap())
                .map(|(key, _)| key);
            if let Some(key) = smallest_bigger {
                self.buff.remove_entry(&key);
                self.buff.insert(candidate, score);
            }
        } else {
            self.buff.insert(candidate, score);
        }
    }
}

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
    WorkScheduled(Receiver<Transaction>),
}
pub struct Index {
    metadata: IndexMetadata,
    merger_status: MergerStatus,
    state: RwLock<SegmentManager>,
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

    fn read_segments(&self) -> RwLockReadGuard<'_, SegmentManager> {
        self.state.read().unwrap_or_else(|e| e.into_inner())
    }

    fn write_segments(&self) -> RwLockWriteGuard<'_, SegmentManager> {
        self.state.write().unwrap_or_else(|e| e.into_inner())
    }

    fn update(&self) -> VectorR<()> {
        if self.read_segments().needs_refresh()? {
            self.write_segments().refresh()?;
            self.set_dimension(self.stored_dimension()?);
        }

        Ok(())
    }

    fn load_v1_state(path: &Path) -> VectorR<SegmentManager> {
        let v1 = state_v1::State::open(path)?;
        let state_version = fs_state::crnt_version(path)?;
        SegmentManager::from_v1_state(path, v1, state_version)
    }

    pub fn open(path: &Path, write: bool) -> VectorR<Index> {
        let metadata = IndexMetadata::open(path)?.map(Ok).unwrap_or_else(|| {
            // Old indexes may not have this file so in that case the
            // metadata file they should have is created.
            let metadata = IndexMetadata::default();
            metadata.write(path).map(|_| metadata)
        })?;

        let state = if let Ok(v2) = SegmentManager::open(path.to_path_buf()) {
            v2
        } else {
            // Failed to open V2 metadata, try V1
            let migrated_v1 = Self::load_v1_state(path)?;
            if write {
                // Make a backup and persist the changes
                fs_state::backup_state(path)?;
                migrated_v1.save()?;
            }
            migrated_v1
        };

        let index = Index {
            metadata,
            merger_status: MergerStatus::Free,
            dimension: RwLock::new(None),
            state: RwLock::new(state),
            location: path.to_path_buf(),
        };
        // Read dimension from segments
        index.set_dimension(index.stored_dimension()?);
        Ok(index)
    }

    pub fn new(path: &Path, metadata: IndexMetadata) -> VectorR<Index> {
        std::fs::create_dir(path)?;
        let state = SegmentManager::create(path.to_path_buf())?;

        metadata.write(path)?;
        let index = Index {
            metadata,
            merger_status: MergerStatus::Free,
            dimension: RwLock::new(None),
            state: RwLock::new(state),
            location: path.to_path_buf(),
        };
        Ok(index)
    }

    pub fn transaction(&self) -> Transaction {
        Transaction::default()
    }

    pub fn get_keys(&self) -> VectorR<Vec<String>> {
        self.update()?;
        let mut keys = vec![];
        for (delete_log, dpid) in self.read_segments().segment_iterator() {
            let dp = DataPoint::open(&self.location, dpid)?;
            keys.append(&mut dp.get_keys(&delete_log));
        }

        Ok(keys)
    }

    pub fn search(&self, request: &dyn SearchRequest) -> VectorR<Vec<Neighbour>> {
        self.update()?;
        let given_len = request.get_query().len() as u64;
        match self.get_dimension() {
            Some(expected) if expected != given_len => Err(VectorErr::InconsistentDimensions),
            None => Ok(Vec::with_capacity(0)),
            Some(_) => self.inner_search(request),
        }
    }

    fn inner_search(&self, request: &dyn SearchRequest) -> VectorR<Vec<Neighbour>> {
        let query = request.get_query();
        let filter = request.get_filter();
        let with_duplicates = request.with_duplicates();
        let no_results = request.no_results();
        let min_score = request.min_score();
        let mut ffsv = Fssc::new(request.no_results(), with_duplicates);

        for (delete_log, dpid) in self.read_segments().segment_iterator() {
            let data_point = DataPoint::open(&self.location, dpid)?;
            let partial_solution = data_point.search(
                &delete_log,
                query,
                filter,
                with_duplicates,
                no_results,
                self.metadata.similarity,
                min_score,
            );
            for candidate in partial_solution {
                ffsv.add(candidate);
            }
        }

        Ok(ffsv.into())
    }

    pub fn no_nodes(&self) -> VectorR<usize> {
        self.update()?;
        Ok(self.read_segments().no_nodes())
    }

    pub fn collect_garbage(&mut self) -> VectorR<()> {
        // A merge may be waiting to be recorded.
        self.apply_pending_merge()?;

        // First compact the segment log, to remove all references
        // to segments that are no longer in use by any reader
        let mut state = self.write_segments();
        state.compact()?;

        // We iterate all segments in the log including those who have been
        // deleted, but might still be in use in a long-running reader.
        // This is often the same as live segments, except when a merge
        // operation has just finished and some readers are still working
        // with the old segments
        let in_use_dp: HashSet<_> = state.all_segments_iterator().copied().collect();
        drop(state);
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

    fn stored_dimension(&self) -> VectorR<Option<u64>> {
        let Some((_, dpid)) = self.read_segments().segment_iterator().next() else {
            return Ok(None);
        };

        let data_point = DataPoint::open(&self.location, dpid)?;
        Ok(data_point.stored_len())
    }

    fn take_available_merge(&mut self) -> Option<Transaction> {
        let MergerStatus::WorkScheduled(rcv) = std::mem::take(&mut self.merger_status) else {
            return None;
        };
        match rcv.try_recv() {
            Ok(transaction) => Some(transaction),
            Err(channel::TryRecvError::Disconnected) => None,
            Err(channel::TryRecvError::Empty) => {
                self.merger_status = MergerStatus::WorkScheduled(rcv);
                None
            }
        }
    }

    pub fn commit(&mut self, transaction: Transaction) -> VectorR<()> {
        // A merge may be waiting to be recorded.
        self.apply_pending_merge()?;

        if transaction.is_empty() {
            return Ok(());
        }

        self.write_segments().commit(transaction)?;

        if self.get_dimension().is_none() {
            self.set_dimension(self.stored_dimension()?);
        }

        let segment_count = self.read_segments().segment_iterator().count();
        if matches!(self.merger_status, MergerStatus::Free) && segment_count > ALLOWED_BEFORE_MERGE {
            self.start_merge();
        }
        Ok(())
    }

    fn start_merge(&mut self) {
        let location = self.location.clone();
        let similarity = self.metadata.similarity;
        let (sender, receiver) = channel::unbounded();
        let worker = Worker::request(location, sender, similarity, self.metadata.channel);
        self.merger_status = MergerStatus::WorkScheduled(receiver);
        merger::send_merge_request(worker);
    }

    fn apply_pending_merge(&mut self) -> VectorR<bool> {
        let possible_merge = self.take_available_merge();
        if let Some(merge_tx) = possible_merge {
            self.write_segments().commit(merge_tx)?;
            Ok(true)
        } else {
            Ok(false)
        }
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
    use std::thread::{self, sleep};
    use std::time::Duration;

    use nucliadb_core::NodeResult;

    use super::fs_state;

    use super::*;
    use crate::data_point::{Elem, LabelDictionary, Similarity};
    #[test]
    fn garbage_collection_test() -> NodeResult<()> {
        let dir = tempfile::tempdir()?;
        let vectors_path = dir.path().join("vectors");
        let mut index = Index::new(&vectors_path, IndexMetadata::default())?;

        let empty_no_entries = std::fs::read_dir(&vectors_path)?.count();
        for _ in 0..10 {
            DataPoint::new(&vectors_path, vec![], None, Similarity::Cosine, Channel::EXPERIMENTAL).unwrap();
        }

        index.collect_garbage()?;
        let no_entries = std::fs::read_dir(&vectors_path)?.count();
        assert_eq!(no_entries, empty_no_entries);
        Ok(())
    }

    fn insert_resource(index: &mut Index, i: u32) -> VectorR<()> {
        let e = Elem::new(format!("key_{i}"), vec![2.0 + (i as f32 * 0.1)], LabelDictionary::new(vec![]), None);
        let dp = DataPoint::new(index.location(), vec![e], None, Similarity::Cosine, Channel::EXPERIMENTAL).unwrap();
        let mut tx = index.transaction();
        tx.add_segment(dp.journal());
        index.commit(tx)
    }

    #[test]
    fn test_merge_and_gc_with_deletions() -> NodeResult<()> {
        let dir = tempfile::tempdir()?;
        let vectors_path = dir.path().join("vectors");
        let mut index = Index::new(&vectors_path, IndexMetadata::default())?;

        // Insert(0, 1, 2)
        for i in 0..3 {
            insert_resource(&mut index, i)?;
        }
        let result = index.get_keys()?;
        assert_eq!(result.len(), 3);

        // Delete(0)
        let mut tx = index.transaction();
        tx.delete_entry("key_0".into());
        index.commit(tx)?;

        let result = index.get_keys()?;
        assert_eq!(result.len(), 2);

        // Insert(0)
        insert_resource(&mut index, 0)?;

        let result = index.get_keys()?;
        assert_eq!(result.len(), 3);
        assert_eq!(index.read_segments().all_segments_iterator().count(), 4);
        assert_eq!(index.read_segments().segment_iterator().count(), 4);

        // Garbage collection should not do anything since all segments are active
        index.collect_garbage()?;
        let result = index.get_keys()?;
        assert_eq!(result.len(), 3);
        assert_eq!(index.read_segments().all_segments_iterator().count(), 4);
        assert_eq!(index.read_segments().segment_iterator().count(), 4);

        // Merge segments now, should leave a single live segments and prune the delete log
        Merger::install_global().map(std::thread::spawn)?;
        index.start_merge();
        while !index.apply_pending_merge()? {
            sleep(Duration::from_millis(50));
        }

        // Will have a new segment, but old ones are not deleted yet
        let result = index.get_keys()?;
        assert_eq!(result.len(), 3);
        assert_eq!(index.read_segments().all_segments_iterator().count(), 5);
        assert_eq!(index.read_segments().segment_iterator().count(), 1);

        // After garbage collection, we delete the old stuff
        index.collect_garbage()?;

        let result = index.get_keys()?;
        assert_eq!(result.len(), 3);
        assert_eq!(index.read_segments().all_segments_iterator().count(), 1);
        assert_eq!(index.read_segments().segment_iterator().count(), 1);

        Ok(())
    }

    #[test]
    fn test_index_metadata_upgrade() -> NodeResult<()> {
        // Manually create an Index V1 since we do not allow this anymore
        let dir = tempfile::tempdir()?;
        let vectors_path = dir.path();
        let mut v1_state = state_v1::State::new();
        fs_state::persist_state(vectors_path, &v1_state)?;
        let meta = IndexMetadata::default();
        meta.write(vectors_path)?;

        // Fill in some data manually, since we don't allow writes in V1 anymore
        {
            for i in 0..13 {
                let e = Elem::new(format!("key_{i}"), vec![2.0 + (i as f32 * 0.1)], LabelDictionary::new(vec![]), None);
                let dp =
                    DataPoint::new(vectors_path, vec![e], None, Similarity::Cosine, Channel::EXPERIMENTAL).unwrap();
                v1_state.add(dp.journal());
            }
            v1_state.remove("key_12", SystemTime::now());
            v1_state.remove("key_6", SystemTime::now().checked_sub(Duration::from_secs(10)).unwrap());
            fs_state::persist_state(vectors_path, &v1_state)?;
        }

        // A reader can open a V1 index
        let index_reader = Index::open(dir.path(), false)?;
        assert_eq!(index_reader.get_keys()?.len(), 12);
        // assert!(matches!(*index_reader.read_segments(), State::V1(_)));

        // Will upgrade to V2 when a writer opens this
        thread::sleep(Duration::from_millis(500));
        let mut index_writer = Index::open(dir.path(), true)?;
        // assert!(matches!(*index_writer.read_segments(), State::V2(_)));

        // The reader also gets updated on next read
        thread::sleep(Duration::from_millis(500));
        assert_eq!(index_reader.get_keys()?.len(), 12);
        // assert!(matches!(*index_reader.read_segments(), State::V2(_)));

        // Writer can write to V2, and reader sees it
        insert_resource(&mut index_writer, 15)?;
        thread::sleep(Duration::from_millis(500));
        assert_eq!(index_reader.get_keys()?.len(), 13);

        Ok(())
    }
}
