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
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Write};
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock, RwLockReadGuard};
use std::time::SystemTime;

use fs2::FileExt;
pub use merger::Merger;
use nucliadb_core::tracing::*;
use serde::{Deserialize, Serialize};
use state::*;
use tempfile::NamedTempFile as TemporalFile;

pub use crate::data_point::Neighbour;
use crate::data_point::{DataPoint, DpId, Journal, Similarity};
use crate::data_point_provider::merge_worker::Merged;
use crate::formula::Formula;
use crate::{VectorErr, VectorR};
pub type TemporalMark = SystemTime;

// Remain
const METADATA: &str = "metadata.json";
const STATE: &str = "state.bincode";
const READERS: &str = "readers";
const WRITER_FLAG: &str = "writer.flag";

// 'f' will have access to a BufWriter whose contents at the end of
// f's execution will be atomically persisted in 'path'.
// After this function is executed 'path' will either only contain the data wrote by
// 'f' or be on its previous state.
fn compute_with_atomic_write<F, R>(path: &Path, f: F) -> VectorR<R>
where F: FnOnce(&mut BufWriter<&mut TemporalFile>) -> VectorR<R> {
    let mut file = TemporalFile::new()?;
    let mut buffer = BufWriter::new(&mut file);
    let user_result = f(&mut buffer)?;
    buffer.flush()?;
    mem::drop(buffer);
    file.persist(path)?;
    Ok(user_result)
}

pub trait SearchRequest {
    fn get_query(&self) -> &[f32];
    fn get_filter(&self) -> &Formula;
    fn no_results(&self) -> usize;
    fn with_duplicates(&self) -> bool;
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct IndexMetadata {
    #[serde(default)]
    pub similarity: Similarity,
}
impl IndexMetadata {
    pub fn write(&self, path: &Path) -> VectorR<()> {
        compute_with_atomic_write(&path.join(METADATA), |buffer| {
            Ok(serde_json::to_writer(buffer, self)?)
        })
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

#[derive(Clone)]
pub struct Index {
    metadata: IndexMetadata,
    location: PathBuf,
}
impl Index {
    pub fn open(path: &Path) -> VectorR<Index> {
        match IndexMetadata::open(path)? {
            Some(metadata) => Ok(Index {
                metadata,
                location: path.to_path_buf(),
            }),
            None => {
                // Old indexes may not have this file so in that case the
                // metadata file they should have is created.
                let location = path.to_path_buf();
                let metadata = IndexMetadata::default();
                metadata.write(&location)?;
                Ok(Index { metadata, location })
            }
        }
    }
    pub fn new(path: &Path, metadata: IndexMetadata) -> VectorR<Index> {
        metadata.write(path)?;
        compute_with_atomic_write(&path.join(STATE), |buffer| {
            let location = path.to_path_buf();
            bincode::serialize_into(buffer, &State::new())?;
            Ok(Index { metadata, location })
        })
    }
    pub fn writer(&self) -> VectorR<Writer> {
        Writer::new(self.clone())
    }
    pub fn reader(&self) -> VectorR<Reader> {
        Reader::new(self.clone())
    }
    pub fn location(&self) -> &Path {
        &self.location
    }
    pub fn metadata(&self) -> &IndexMetadata {
        &self.metadata
    }
}

#[derive(Clone)]
struct RwState {
    inner: Arc<RwLock<State>>,
}
impl RwState {
    pub fn read(&self) -> RwLockReadGuard<'_, State> {
        self.inner.read().unwrap_or_else(|e| e.into_inner())
    }
    pub fn new(path: &Path) -> VectorR<RwState> {
        let mut state_buffer = BufReader::new(File::open(path)?);
        let state: State = bincode::deserialize_from(&mut state_buffer)?;
        let inner = Arc::new(RwLock::new(state));
        Ok(RwState { inner })
    }
    pub fn apply<F, R>(&self, transform: F) -> VectorR<R>
    where F: FnOnce(&mut State) -> VectorR<R> {
        let mut writer = self.inner.write().unwrap_or_else(|e| e.into_inner());
        transform(&mut writer)
    }
    pub fn persist(&self, path: &Path) -> VectorR<()> {
        compute_with_atomic_write(path, |buffer| {
            Ok(bincode::serialize_into(buffer, &*self.read())?)
        })
    }
}

pub struct Reader {
    status_path: PathBuf,
    state_path: PathBuf,
    inner: Index,
    state: RwState,
    update_scheduled: Arc<AtomicBool>,
}
impl Drop for Reader {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.status_path);
    }
}
impl Reader {
    fn new(inner: Index) -> VectorR<Reader> {
        let id = uuid::Uuid::new_v4().to_string();
        let update_scheduled = Arc::new(AtomicBool::new(false));
        let status_dir = inner.location().join(READERS);
        let status_path = status_dir.join(id).with_extension("bincode");
        let state_path = inner.location().join(STATE);
        let state = RwState::new(&state_path)?;
        let watching: Vec<_> = state.read().dpid_iter().collect();
        if !status_dir.exists() {
            std::fs::create_dir_all(&status_dir)?;
        }
        compute_with_atomic_write(&status_path, |buffer| {
            Ok(bincode::serialize_into(buffer, &watching)?)
        })?;
        Ok(Reader {
            state_path,
            status_path,
            inner,
            state,
            update_scheduled,
        })
    }
    pub fn schedule_update(&self) {
        if self.update_scheduled.swap(true, Ordering::SeqCst) {
            return;
        }

        let update_scheduled = self.update_scheduled.clone();
        let status_path = self.status_path.clone();
        let state_path = self.state_path.clone();
        let state = self.state.clone();
        std::thread::spawn(move || {
            try_catch::catch! {
                    try {
                        let state_file = File::open(&state_path)?;
                        let mut state_file_buffer = BufReader::new(state_file);
                        let new_state: State = bincode::deserialize_from(&mut state_file_buffer)?;
                        let watching = new_state.dpid_iter().collect::<Vec<_>>();
                        let mut temp_status = TemporalFile::new()?;
                        let mut temp_status_buffer = BufWriter::new(&mut temp_status);
                        bincode::serialize_into(&mut temp_status_buffer, &watching)?;
                        temp_status_buffer.flush()?;
                        mem::drop(temp_status_buffer);
                        state.apply(|inner| {
                            // Is important that reader state updates
                            // and status updates are done atomically.
                            // Otherwise data points that are in use may be delete by the GC.
                            *inner = new_state;
                            temp_status.persist(status_path)?;
                            update_scheduled.swap(false, Ordering::SeqCst);
                            Ok(())
                        })?;
                    } catch error {
                        // Is crucial that upon failure the flag is restored.
                        // Otherwise updates will not happen again.
                        error!("Could not update reader due to {error:?}");
                        update_scheduled.swap(false, Ordering::SeqCst);
                    }
            }
        });
    }
    pub fn keys(&self) -> VectorR<Vec<String>> {
        self.state.read().keys(self.location())
    }
    pub fn search(&self, request: &dyn SearchRequest) -> VectorR<Vec<Neighbour>> {
        let state = self.state.read();
        let location = self.location();
        let similarity = self.metadata().similarity;
        state.search(location, request, similarity)
    }
    pub fn number_of_nodes(&self) -> usize {
        self.state.read().no_nodes()
    }
    pub fn location(&self) -> &Path {
        self.inner.location()
    }
    pub fn metadata(&self) -> &IndexMetadata {
        self.inner.metadata()
    }
    pub fn index(&self) -> &Index {
        &self.inner
    }
}

pub struct Writer {
    #[allow(unused)]
    writer_flag: File,
    merged_queue: Merged,
    datapoint_buffer: Vec<Journal>,
    delete_buffer: Vec<(String, SystemTime)>,
    inner: Index,
    state: RwState,
    state_path: PathBuf,
    readers_path: PathBuf,
}
impl Writer {
    fn new(inner: Index) -> VectorR<Writer> {
        let writer_flag = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(inner.location.join(WRITER_FLAG))?;
        writer_flag
            .try_lock_exclusive()
            .map_err(|_| VectorErr::WriterExistsError)?;
        let readers_path = inner.location().join(READERS);
        let state_path = inner.location().join(STATE);
        let state = RwState::new(&state_path)?;
        let merged_queue = Merged::new();
        Ok(Writer {
            inner,
            state,
            state_path,
            writer_flag,
            readers_path,
            merged_queue,
            datapoint_buffer: vec![],
            delete_buffer: vec![],
        })
    }
    pub fn add(&mut self, datapoint: DataPoint) {
        self.datapoint_buffer.push(datapoint.meta());
    }
    pub fn delete(&mut self, prefix: String, from: SystemTime) {
        self.delete_buffer.push((prefix, from));
    }
    pub fn collect_garbage(&self) -> VectorR<()> {
        if self.has_work() {
            return Err(VectorErr::WouldBlockError);
        }
        let location = self.location();
        let state = self.state.read();
        let mut in_use_dp: HashSet<_> = state.dpid_iter().collect();
        // Loading the readers status
        for reader_status in std::fs::read_dir(&self.readers_path)? {
            let entry = reader_status?;
            let status_file = OpenOptions::new().read(true).open(entry.path())?;
            let mut status_buf = BufReader::new(&status_file);
            let status: Vec<DpId> = bincode::deserialize_from(&mut status_buf)?;
            in_use_dp.extend(status.into_iter());
        }

        // Garbage is whatever is not in use
        for dir_entry in std::fs::read_dir(location)? {
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
                let Err(err) = DataPoint::delete(location, dpid) else {
                    continue;
                };
                warn!("{name} is garbage and could not be deleted because of {err}");
            }
        }
        Ok(())
    }
    pub fn commit(&mut self) -> VectorR<()> {
        if !self.has_work() {
            return Ok(());
        }

        let adds = mem::take(&mut self.datapoint_buffer).into_iter();
        let deletes = mem::take(&mut self.delete_buffer).into_iter();
        let merge_work = self.merged_queue.take().into_iter();

        // Modifying the state with the current buffers
        self.state.apply(move |state: &mut State| {
            adds.for_each(|i| state.add(i));
            deletes.for_each(|i| state.remove(&i.0, i.1));
            merge_work.for_each(|i| state.add_merge(i));
            Ok(())
        })?;

        // Persisting the new state
        self.state.persist(&self.state_path)?;

        // Looking for possible merges
        let state = self.state.read();
        let merged_queue = self.merged_queue.clone();
        if let Some(work_unit) = state.datapoints_to_merge() {
            if merged_queue.reserve_work() {
                merger::send_merge_request(merge_worker::Worker {
                    location: self.location().to_path_buf(),
                    similarity: self.metadata().similarity,
                    delete_log: state.delete_log().clone(),
                    result: self.merged_queue.clone(),
                    work: work_unit
                        .load
                        .iter()
                        .copied()
                        .map(|i| (i.id(), state.creation_time(i)))
                        .collect(),
                });
            }
        }

        Ok(())
    }
    pub fn number_of_nodes(&self) -> usize {
        self.state.read().no_nodes()
    }
    pub fn abort(&mut self) {
        self.datapoint_buffer.clear();
        self.delete_buffer.clear();
    }
    pub fn has_work(&self) -> bool {
        self.datapoint_buffer.len() + self.delete_buffer.len() > 0
    }
    pub fn location(&self) -> &Path {
        self.inner.location()
    }
    pub fn metadata(&self) -> &IndexMetadata {
        self.inner.metadata()
    }
    pub fn index(&self) -> &Index {
        &self.inner
    }
}

#[cfg(test)]
mod test {
    use nucliadb_core::NodeResult;

    use super::*;
    use crate::data_point::{Elem, LabelDictionary, Similarity};

    #[test]
    fn test_reader_update() -> NodeResult<()> {
        let number_of_nodes = 100;
        let dir = tempfile::tempdir()?;
        let path = dir.path();
        let metadata = IndexMetadata::default();
        let index = Index::new(path, metadata)?;
        let elems: Vec<_> = (0..number_of_nodes)
            .map(|i| format!("key_{i}"))
            .map(|i| Elem::new(i, vec![0.0; 12], LabelDictionary::default(), None))
            .collect();

        let mut writer = index.writer()?;
        let reader = index.reader()?;
        let empty_state_reader = index.reader()?;
        let timestamp = SystemTime::now();
        let datapoint = DataPoint::new(path, elems, Some(timestamp), Similarity::Dot)?;
        writer.add(datapoint);
        writer.commit()?;
        // Not updated yet
        assert_eq!(empty_state_reader.number_of_nodes(), 0);
        assert_eq!(reader.number_of_nodes(), 0);
        reader.schedule_update();
        while reader.update_scheduled.load(Ordering::SeqCst) {}
        // Now the reader has been updated
        assert_eq!(reader.number_of_nodes(), number_of_nodes);
        assert_eq!(empty_state_reader.number_of_nodes(), 0);
        Ok(())
    }

    #[test]
    fn many_readers() -> VectorR<()> {
        let dir = tempfile::tempdir()?;
        let metadata = IndexMetadata::default();
        let index = Index::new(dir.path(), metadata)?;
        let readers_status = dir.path().join(READERS);
        {
            let _reader1 = index.reader()?;
            let _reader2 = index.reader()?;
            let _reader3 = index.reader()?;
            let _reader4 = index.reader()?;
            let reader_count = std::fs::read_dir(&readers_status)?.count();
            assert_eq!(reader_count, 4);
        }
        let reader_count = std::fs::read_dir(&readers_status)?.count();
        assert_eq!(reader_count, 0);
        Ok(())
    }
    #[test]
    fn only_one_writer() -> VectorR<()> {
        let dir = tempfile::tempdir()?;
        let metadata = IndexMetadata::default();
        let index = Index::new(dir.path(), metadata)?;

        // There is no other writer, so opening a
        // writer does not fail.
        let writer = index.writer()?;

        // There is no another writer for this index
        let Err(VectorErr::WriterExistsError) = index.writer() else {
            panic!("This should have failed");
        };
        mem::drop(writer);

        // Is safe to open a new writer again
        let _writer = index.writer()?;

        Ok(())
    }

    #[test]
    fn garbage_collection_test() -> NodeResult<()> {
        let dir = tempfile::tempdir()?;
        let index = Index::new(dir.path(), IndexMetadata::default())?;
        let writer = index.writer()?;
        let _reader = index.reader()?;
        let empty_no_entries = std::fs::read_dir(dir.path())?.count();

        for _ in 0..10 {
            DataPoint::new(dir.path(), vec![], None, Similarity::Cosine).unwrap();
        }

        writer.collect_garbage()?;
        let no_entries = std::fs::read_dir(dir.path())?.count();
        assert_eq!(no_entries, empty_no_entries);
        Ok(())
    }
}
