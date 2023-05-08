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
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Write};
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::SystemTime;

use fs2::FileExt;
pub use merger::Merger;
use nucliadb_core::fs_state::{self, ELock, Lock, SLock, Version};
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
    work_flag: MergerWriterSync,
    state: Arc<RwLock<State>>,
    date: Arc<RwLock<Version>>,
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
    fn get_elock(&self) -> VectorR<ELock> {
        let lock = fs_state::exclusive_lock(&self.location)?;
        self.update(&lock)?;
        Ok(lock)
    }
    fn get_slock(&self) -> VectorR<SLock> {
        let lock = fs_state::shared_lock(&self.location)?;
        self.update(&lock)?;
        Ok(lock)
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
    fn get_keys(&self, _: &Lock) -> VectorR<Vec<String>> {
        self.read_state().keys(&self.location)
    }
    fn search(&self, request: &dyn SearchRequest, _: &Lock) -> VectorR<Vec<Neighbour>> {
        self.read_state()
            .search(&self.location, request, self.metadata.similarity)
    }
    fn no_nodes(&self, _: &Lock) -> usize {
        self.read_state().no_nodes()
    }

    fn delete(&self, prefix: impl AsRef<str>, temporal_mark: SystemTime, _: &ELock) {
        let mut state = self.write_state();
        state.remove(prefix.as_ref(), temporal_mark);
    }
    fn collect_garbage(&self) -> VectorR<()> {
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
    #[must_use]
    fn add(&self, journal: Journal, _: &ELock) -> bool {
        let mut state = self.write_state();
        state.add(journal)
    }
    fn commit(&self, lock: ELock) -> VectorR<()> {
        let state = self.read_state();
        let mut date = self.write_date();
        fs_state::persist_state::<State>(&lock, &state)?;
        *date = fs_state::crnt_version(&lock)?;
        Ok(())
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
            state: Arc::new(RwLock::new(state)),
            date: Arc::new(RwLock::new(date)),
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
            state: Arc::new(RwLock::new(state)),
            date: Arc::new(RwLock::new(date)),
            location: path.to_path_buf(),
        };
        Ok(index)
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

pub struct Reader {
    inner: Index,
    lock: SLock,
}
impl Reader {
    fn new(inner: Index) -> VectorR<Reader> {
        inner.get_slock().map(|lock| Reader { inner, lock })
    }
    pub fn search(&self, request: &dyn SearchRequest) -> VectorR<Vec<Neighbour>> {
        self.inner.search(request, &self.lock)
    }
    pub fn keys(&self) -> VectorR<Vec<String>> {
        self.inner.get_keys(&self.lock)
    }
    pub fn no_nodes(&self) -> usize {
        self.inner.no_nodes(&self.lock)
    }
    pub fn index(&self) -> &Index {
        &self.inner
    }
}

pub struct Writer {
    #[allow(unused)]
    flag: File,
    datapoint_buffer: Vec<Journal>,
    delete_buffer: Vec<(String, SystemTime)>,
    inner: Index,
}
impl Writer {
    fn new(inner: Index) -> VectorR<Writer> {
        let flag = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(inner.location.join(WRITER_FLAG))?;
        flag.try_lock_exclusive()
            .map_err(|_| VectorErr::WriterExists)?;
        Ok(Writer {
            flag,
            inner,
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
            return Err(VectorErr::WorkDelayed);
        }
        self.inner.collect_garbage()
    }
    pub fn commit(&mut self) -> VectorR<()> {
        if !self.has_work() {
            return Ok(());
        }

        let lock = self.inner.get_elock()?;
        self.inner.update(&lock)?;
        let merge_work = self
            .datapoint_buffer
            .iter()
            .copied()
            .fold(0, |acc, i| acc + (self.inner.add(i, &lock) as usize));
        self.delete_buffer
            .iter()
            .for_each(|(prefix, time)| self.inner.delete(prefix, *time, &lock));
        self.inner.commit(lock)?;

        // The work was commited so is not needed anymore
        self.datapoint_buffer.clear();
        self.delete_buffer.clear();
        // Once the commit is done is safe to notify the merger
        (0..merge_work).for_each(|_| self.inner.notify_merger());
        Ok(())
    }
    pub fn abort(&mut self) {
        self.datapoint_buffer.clear();
        self.delete_buffer.clear();
    }
    pub fn has_work(&self) -> bool {
        self.datapoint_buffer.len() + self.delete_buffer.len() > 0
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
        let _reader1 = index.reader()?;
        let _reader2 = index.reader()?;
        let _reader3 = index.reader()?;
        let _reader4 = index.reader()?;
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
        std::mem::drop(writer);

        // Is safe to open a new writer again
        let _writer = index.writer()?;

        Ok(())
    }

    #[test]
    fn garbage_collection_test() -> NodeResult<()> {
        let dir = tempfile::tempdir()?;
        let index = Index::new(dir.path(), IndexMetadata::default())?;
        let writer = index.writer()?;
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
