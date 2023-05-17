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
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Write};
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock, RwLockReadGuard};
use std::time::SystemTime;

use fs2::FileExt;
pub use merger::Merger;
use nucliadb_core::fs_state::{self, Version};
use nucliadb_core::tracing::*;
use serde::{Deserialize, Serialize};
use state::*;
use work_flag::MergerWriterSync;

pub use crate::data_point::Neighbour;
use crate::data_point::{DataPoint, DpId, Journal, Similarity};
use crate::data_point_provider::merge_worker::Worker;
use crate::formula::Formula;
use crate::{VectorErr, VectorR};
pub type TemporalMark = SystemTime;

const METADATA: &str = "metadata.json";
const WRITER_FLAG: &str = "writer.flag";
const READERS_STATUS: &str = "readers";

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

#[derive(Clone)]
pub struct Index {
    metadata: IndexMetadata,
    location: PathBuf,
}
impl Index {
    pub fn open(path: &Path) -> VectorR<Index> {
        let metadata = IndexMetadata::open(path)?.map(Ok).unwrap_or_else(|| {
            // Old indexes may not have this file so in that case the
            // metadata file they should have is created.
            let metadata = IndexMetadata::default();
            metadata.write(path).map(|_| metadata)
        })?;
        Ok(Index {
            metadata,
            location: path.to_path_buf(),
        })
    }
    pub fn new(path: &Path, metadata: IndexMetadata) -> VectorR<Index> {
        std::fs::create_dir_all(path)?;
        std::fs::create_dir_all(path.join(READERS_STATUS))?;
        fs_state::initialize_disk(path, State::new)?;
        metadata.write(path)?;
        Ok(Index {
            metadata,
            location: path.to_path_buf(),
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

struct InnerContext {
    state: State,
    version: Version,
}
impl InnerContext {
    pub fn new(path: &Path) -> VectorR<InnerContext> {
        let (version, state) = fs_state::load_state::<State>(path)?;
        Ok(InnerContext { state, version })
    }
}

#[derive(Clone)]
struct Context {
    inner: Arc<RwLock<InnerContext>>,
}
impl Context {
    pub fn read(&self) -> RwLockReadGuard<'_, InnerContext> {
        self.inner.read().unwrap_or_else(|e| e.into_inner())
    }
    pub fn new(path: &Path) -> VectorR<Context> {
        let inner = Arc::new(RwLock::new(InnerContext::new(path)?));
        Ok(Context { inner })
    }
    pub fn apply<F, R>(&self, transform: F) -> VectorR<R>
    where F: FnOnce(&mut InnerContext) -> VectorR<R> {
        let mut writer = self.inner.write().unwrap_or_else(|e| e.into_inner());
        transform(&mut writer)
    }
    pub fn persist(&self, path: &Path) -> VectorR<()> {
        let state = self.read();
        Ok(fs_state::atomic_write(path, &state.state)?)
    }
}

pub struct Reader {
    status: PathBuf,
    inner: Index,
    context: Context,
}
impl Drop for Reader {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.status);
    }
}
impl Reader {
    fn new(inner: Index) -> VectorR<Reader> {
        let id = uuid::Uuid::new_v4().to_string();
        let status_dir = inner.location().join(READERS_STATUS);
        let status = status_dir.join(id).with_extension("json");
        let context = Context::new(inner.location())?;
        {
            // Creating the reader status
            let mut status_file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&status)?;
            status_file.lock_exclusive()?;

            let watching = context.read().state.dpid_iter().collect::<Vec<_>>();
            let mut status_buf = BufWriter::new(&mut status_file);
            serde_json::to_writer(&mut status_buf, &watching)?;
            status_buf.flush()?;
            mem::drop(status_buf);

            status_file.unlock()?;
        }
        Ok(Reader {
            status,
            inner,
            context,
        })
    }
    pub fn schedule_update(&self) -> VectorR<()> {
        // Is important that reader state updates
        // and status updates are done atomically.
        // Otherwise data points that are in use may be delete by the GC.
        let location = self.inner.location().to_path_buf();
        let status = self.status.clone();
        let transform = move |state: &mut InnerContext| {
            let disk_version = fs_state::crnt_version(&location)?;
            if disk_version > state.version {
                let mut status_file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(&status)?;
                status_file.lock_exclusive()?;

                let (new_version, new_state) = fs_state::load_state(&location)?;
                state.state = new_state;
                state.version = new_version;

                let watching = state.state.dpid_iter().collect::<Vec<_>>();
                let mut status_buf = BufWriter::new(&mut status_file);
                serde_json::to_writer(&mut status_buf, &watching)?;
                status_buf.flush()?;
                mem::drop(status_buf);

                status_file.unlock()?;
            }
            Ok(())
        };
        // Updating the state in the background
        let state = self.context.clone();
        std::thread::spawn(move || state.apply(transform));
        Ok(())
    }
    pub fn keys(&self) -> VectorR<Vec<String>> {
        let state = self.context.read();
        state.state.keys(self.location())
    }
    pub fn search(&self, request: &dyn SearchRequest) -> VectorR<Vec<Neighbour>> {
        let context = self.context.read();
        let location = self.location();
        let similarity = self.metadata().similarity;
        context.state.search(location, request, similarity)
    }
    pub fn number_of_nodes(&self) -> usize {
        let context = self.context.read();
        context.state.no_nodes()
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
    work_flag: MergerWriterSync,
    datapoint_buffer: Vec<Journal>,
    delete_buffer: Vec<(String, SystemTime)>,
    inner: Index,
    context: Context,
}
impl Writer {
    fn update(&self) -> VectorR<()> {
        let location = self.inner.location().to_path_buf();
        let transform = move |context: &mut InnerContext| {
            let disk_version = fs_state::crnt_version(&location)?;
            if disk_version > context.version {
                let (new_version, new_state) = fs_state::load_state(&location)?;
                context.state = new_state;
                context.version = new_version;
            }
            Ok(())
        };
        self.context.apply(transform)
    }
    fn notify_merger(&self) {
        let worker = Worker::request(
            self.location().to_path_buf(),
            self.work_flag.clone(),
            self.metadata().similarity,
        );
        merger::send_merge_request(worker);
    }
    fn new(inner: Index) -> VectorR<Writer> {
        let work_flag = MergerWriterSync::new();
        let writer_flag = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(inner.location.join(WRITER_FLAG))?;
        writer_flag
            .try_lock_exclusive()
            .map_err(|_| VectorErr::WriterExists)?;
        let context = Context::new(&inner.location)?;
        let work_len = context.read().state.work_stack_len();
        let writer = Writer {
            inner,
            writer_flag,
            work_flag,
            context,
            datapoint_buffer: vec![],
            delete_buffer: vec![],
        };
        (0..work_len).for_each(|_| writer.notify_merger());
        Ok(writer)
    }
    pub fn add(&mut self, datapoint: DataPoint) {
        self.datapoint_buffer.push(datapoint.meta());
    }
    pub fn delete(&mut self, prefix: String, from: SystemTime) {
        self.delete_buffer.push((prefix, from));
    }
    pub fn collect_garbage(&self) -> VectorR<()> {
        if self.has_work() {
            return Err(VectorErr::WorkDelayed);
        }
        let location = self.location();
        // Synchronizing with the merger.
        let work_flag = self.work_flag.start_working();
        self.update()?;
        let context = self.context.read();
        let mut in_use_dp: HashSet<_> = context.state.dpid_iter().collect();
        // Loading the readers status
        for reader_status in std::fs::read_dir(location.join(READERS_STATUS))? {
            let entry = reader_status?;
            let status_file = OpenOptions::new().read(true).open(entry.path())?;
            status_file.lock_shared()?;
            let mut status_buf = BufReader::new(&status_file);
            let status: Vec<DpId> = serde_json::from_reader(&mut status_buf)?;
            status_file.unlock()?;
            status.into_iter().for_each(|i| {
                in_use_dp.insert(i);
            });
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
                let Err(err)  = DataPoint::delete(location, dpid) else { continue };
                warn!("{name} is garbage and could not be deleted because of {err}");
            }
        }
        mem::drop(work_flag);
        Ok(())
    }
    pub fn commit(&mut self) -> VectorR<()> {
        if !self.has_work() {
            return Ok(());
        }
        let adds = mem::take(&mut self.datapoint_buffer);
        let deletes = mem::take(&mut self.delete_buffer);
        let location = self.location();
        let work_flag = self.work_flag.start_working();
        // Get the last version of the state, merges may have happen.
        // Is important to ensure that we are the only ones working on the
        // state.
        self.update()?;

        // Modifying the state with the current buffers
        let merge_work = self.context.apply(move |state: &mut InnerContext| {
            let merge_work = adds
                .iter()
                .copied()
                .fold(0, |acc, i| acc + (state.state.add(i) as usize));
            deletes
                .iter()
                .for_each(|(prefix, time)| state.state.remove(prefix, *time));
            Ok(merge_work)
        })?;
        // Persisting the new state
        {
            // Moving the work flag to this scope
            let _work_flag = work_flag;
            self.context.persist(location)?;
        }
        // Once the commit is done is safe to notify the merger
        (0..merge_work).for_each(|_| self.notify_merger());
        Ok(())
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
    use crate::data_point::Similarity;

    #[test]
    fn many_readers() -> VectorR<()> {
        let dir = tempfile::tempdir()?;
        let metadata = IndexMetadata::default();
        let index = Index::new(dir.path(), metadata)?;
        let readers_status = dir.path().join(READERS_STATUS);
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
        let Err(VectorErr::WriterExists) = index.writer() else {
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
