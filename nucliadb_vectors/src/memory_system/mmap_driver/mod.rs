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
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use fs2::FileExt;
use memmap2::{Mmap, MmapMut};
use rayon::prelude::*;

use crate::memory_system::elements::{
    ByteRpr, FileSegment, FixedByteLen, GraphLayer, HNSWParams, Vector,
};

const STORAGE: &str = "STORAGE.nuclia";
const LOCK: &str = "LOCK.nuclia";
const STACK: &str = "STACK.nuclia";
const STORAGE_BACKUP: &str = "STORAGE_BCK.nuclia";

pub trait SegmentReader {
    fn read_all(&self) -> &[u8];
    fn read(&self, segment: FileSegment) -> Option<&[u8]>;
}

pub trait SegmentWriter {
    fn delete_segment(&mut self, segment: FileSegment);
    fn insert(&mut self, bytes: &[u8]) -> FileSegment;
    fn truncate(&mut self, bytes: &[u8]);
}

struct CapturedItem {
    path_storage: PathBuf,
    path_picture: PathBuf,
    picture: Mmap,
}
impl CapturedItem {
    pub fn new(path: &Path) -> CapturedItem {
        use std::time::Duration;
        while !path.exists() {
            let sleep_time = std::time::Duration::from_millis(10);
            std::thread::sleep(sleep_time);
        }
        let path_storage = path.join(STORAGE);
        let path_picture = path.join(STORAGE_BACKUP);
        File::create(&path_picture).unwrap();
        std::fs::copy(&path_storage, &path_picture).unwrap();
        let picture = File::open(&path_picture).unwrap();
        let picture = unsafe { Mmap::map(&picture).unwrap() };
        CapturedItem {
            path_storage,
            path_picture,
            picture,
        }
    }
    pub fn reload(&mut self) {
        std::fs::copy(&self.path_storage, &self.path_picture).unwrap();
        let backup = File::open(&self.path_picture).unwrap();
        self.picture = unsafe { Mmap::map(&backup).unwrap() };
    }
}

impl SegmentReader for CapturedItem {
    fn read_all(&self) -> &[u8] {
        &self.picture[..]
    }
    fn read(&self, segment: FileSegment) -> Option<&[u8]> {
        let range = (segment.start as usize)..(segment.end as usize);
        self.picture.get(range)
    }
}

struct DeletedStack {
    stack: PathBuf,
}
impl DeletedStack {
    pub fn new(path: &Path) -> DeletedStack {
        std::fs::create_dir_all(&path).unwrap();
        let path = path.to_path_buf();
        let path_lock = path.join(LOCK);
        let lock = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&path_lock)
            .unwrap();
        DeletedStack {
            stack: path.join(STACK),
        }
    }
    pub fn push(&self, segment: FileSegment) {
        let mut stack = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&self.stack)
            .unwrap();
        stack.write_all(&segment.serialize()).unwrap();
        stack.flush().unwrap();
    }
    pub fn pop(&self) -> Option<FileSegment> {
        let mut stack = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&self.stack)
            .unwrap();
        match stack.seek(SeekFrom::End(-(FileSegment::segment_len() as i64))) {
            Ok(new_len) => {
                let mut buffer = vec![0u8; FileSegment::segment_len()];
                stack.read_exact(&mut buffer).unwrap();
                stack.set_len(new_len).unwrap();
                stack.rewind().unwrap();
                Some(FileSegment::deserialize(&buffer))
            }
            Err(_) => {
                stack.rewind().unwrap();
                None
            }
        }
    }
    pub fn clear(&self) {
        let mut stack = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&self.stack)
            .unwrap();
        stack.set_len(0).unwrap();
    }
}

struct StorageItem {
    path_lock: PathBuf,
    path_storage: PathBuf,
    lock: File,
    deleted: DeletedStack,
    storage: Mmap,
}

impl SegmentReader for StorageItem {
    fn read_all(&self) -> &[u8] {
        &self.storage[..]
    }
    fn read(&self, segment: FileSegment) -> Option<&[u8]> {
        let range = (segment.start as usize)..(segment.end as usize);
        self.storage.get(range)
    }
}

impl SegmentWriter for StorageItem {
    fn delete_segment(&mut self, segment: FileSegment) {
        self.deleted.push(segment);
    }
    fn insert(&mut self, bytes: &[u8]) -> FileSegment {
        match self.deleted.pop() {
            Some(segment) => self.update_segment(segment, bytes),
            None => self.append(bytes),
        }
    }
    fn truncate(&mut self, bytes: &[u8]) {
        self.lock.lock_exclusive().unwrap();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.path_storage)
            .unwrap();
        file.write_all(bytes).unwrap();
        file.flush().unwrap();
        file.set_len(bytes.len() as u64).unwrap();
        self.storage = unsafe { Mmap::map(&file).unwrap() };
        self.deleted.clear();
        self.lock.unlock();
    }
}

impl StorageItem {
    pub fn new(path: &Path) -> StorageItem {
        std::fs::create_dir_all(&path).unwrap();
        let path_storage = path.join(STORAGE);
        let path_lock = path.join(LOCK);
        let path_stack = path.join(STACK);
        let storage = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&path_storage)
            .unwrap();
        let lock = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&path_lock)
            .unwrap();
        let storage = unsafe { Mmap::map(&storage).unwrap() };
        let deleted = DeletedStack::new(path_stack.as_path());
        StorageItem {
            path_lock,
            path_storage,
            lock,
            deleted,
            storage,
        }
    }
    fn update_segment(&mut self, segment: FileSegment, bytes: &[u8]) -> FileSegment {
        self.lock.lock_exclusive().unwrap();
        let range = (segment.start as usize)..(segment.end as usize);
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&self.path_storage)
            .unwrap();
        let mut mmap_mut = unsafe { MmapMut::map_mut(&file).unwrap() };
        if let Some(slice) = mmap_mut.get_mut(range.clone()) {
            slice.clone_from_slice(bytes);
            mmap_mut.flush_range(range.start, range.len()).unwrap();
            self.storage = unsafe { Mmap::map(&file).unwrap() };
        }
        self.lock.unlock();
        segment
    }
    fn append(&mut self, bytes: &[u8]) -> FileSegment {
        self.lock.lock_exclusive().unwrap();
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&self.path_storage)
            .unwrap();
        let metadata = file.metadata().unwrap();
        let mut segment = FileSegment {
            start: metadata.len(),
            end: metadata.len() + (bytes.len() as u64),
        };
        file.write_all(bytes).unwrap();
        file.flush().unwrap();
        self.storage = unsafe { Mmap::map(&file).unwrap() };
        self.lock.unlock();
        segment
    }
}

#[cfg(test)]
mod deleted_stack_tests {
    use super::*;
    #[test]
    pub fn deleted_buffers_usage() {
        let dir = tempfile::tempdir().unwrap();
        let mut deleted_buffer = DeletedStack::new(dir.path());
        let fs_0 = FileSegment { start: 0, end: 0 };
        let fs_1 = FileSegment { start: 1, end: 1 };
        let fs_2 = FileSegment { start: 2, end: 2 };
        assert_eq!(deleted_buffer.pop(), None);
        deleted_buffer.push(fs_0);
        deleted_buffer.push(fs_1);
        deleted_buffer.push(fs_2);
        deleted_buffer.clear();
        assert_eq!(deleted_buffer.pop(), None);
        deleted_buffer.push(fs_0);
        deleted_buffer.push(fs_1);
        deleted_buffer.push(fs_2);
        assert_eq!(deleted_buffer.pop(), Some(fs_2));
        assert_eq!(deleted_buffer.pop(), Some(fs_1));
        assert_eq!(deleted_buffer.pop(), Some(fs_0));
    }
}

#[cfg(test)]
mod captured_item_tests {
    use super::*;
    #[test]
    pub fn captured_item_usage() {
        let mut dir = tempfile::tempdir().unwrap();
        let mut file = File::create(&dir.path().join(STORAGE)).unwrap();
        let mut captured = CapturedItem::new(dir.path());
        assert!(captured.read_all().is_empty());
        file.write_all(b"Some bytes").unwrap();
        file.flush().unwrap();
        assert!(captured.read_all().is_empty());
        captured.reload();
        assert_eq!(captured.read_all(), b"Some bytes");
    }
}

#[cfg(test)]
mod storage_item_tests {
    use super::*;
    #[test]
    pub fn storage_item_usage() {
        let msg_0 = b"message 0";
        let msg_1 = b"message 1";
        let msg_2 = b"message 2";
        let msg_3 = b"message 3";
        let msg_empty = b"this sentence is false";
        let mut dir = tempfile::tempdir().unwrap();
        let mut segment = StorageItem::new(dir.path());
        assert!(segment.read_all().is_empty());
        let fs_0 = segment.insert(msg_0);
        let fs_1 = segment.insert(msg_1);
        let fs_2 = segment.insert(msg_2);
        assert_eq!(segment.read(fs_0), Some(msg_0.as_ref()));
        assert_eq!(segment.read(fs_1), Some(msg_1.as_ref()));
        assert_eq!(segment.read(fs_2), Some(msg_2.as_ref()));
        segment.delete_segment(fs_2);
        let fs_3 = segment.insert(msg_3);
        assert_eq!(fs_3, fs_2);
        assert_eq!(segment.read(fs_3), Some(msg_3.as_ref()));
        segment.truncate(msg_empty);
        assert_eq!(segment.read_all(), msg_empty);
    }
}
