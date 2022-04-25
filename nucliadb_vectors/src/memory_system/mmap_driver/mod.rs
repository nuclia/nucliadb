use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};

use memmap2::Mmap;

use crate::memory_system::elements::{ByteRpr, FileSegment, Vector};

const KEY_STORAGE: &str = "KEYS.stg";
const VECTOR_STORAGE: &str = "KEYS.stg";
pub struct MmapStorage {
    dir_path: PathBuf,
    key_path: PathBuf,
    vector_path: PathBuf,
    key_storage: Mmap,
    vector_storage: Mmap,
}

impl MmapStorage {
    pub fn new(path: &Path) -> MmapStorage {
        std::fs::create_dir_all(&path).unwrap();
        let mut key_path = path.to_path_buf();
        key_path.push(KEY_STORAGE);
        let key_file = OpenOptions::new()
            .append(true)
            .read(true)
            .create(true)
            .open(&key_path)
            .unwrap();
        let mut vector_path = path.to_path_buf();
        vector_path.push(VECTOR_STORAGE);
        let vector_file = OpenOptions::new()
            .append(true)
            .read(true)
            .create(true)
            .open(&vector_path)
            .unwrap();
        MmapStorage {
            dir_path: path.canonicalize().unwrap(),
            key_path: key_path.canonicalize().unwrap(),
            vector_path: vector_path.canonicalize().unwrap(),
            key_storage: unsafe { Mmap::map(&key_file).unwrap() },
            vector_storage: unsafe { Mmap::map(&vector_file).unwrap() },
        }
    }
    pub fn write_key(&self, key: String) -> FileSegment {
        let bytes = key.serialize();
        let mut file = OpenOptions::new()
            .append(true)
            .open(&self.key_path)
            .unwrap();
        let metadata = file.metadata().unwrap();
        let mut segment = FileSegment {
            start: metadata.len(),
            end: metadata.len(),
        };
        file.write_all(&bytes).unwrap();
        file.flush().unwrap();
        segment.end += bytes.len() as u64;
        segment
    }
    pub fn get_key(&self, segment: FileSegment) -> Option<String> {
        if MmapStorage::is_valid(&self.key_storage, segment) {
            let start = segment.start as usize;
            let end = segment.end as usize;
            Some(String::deserialize(&self.key_storage[start..end]))
        } else {
            None
        }
    }
    pub fn write_vector(&self, vector: Vector) -> FileSegment {
        let bytes = vector.serialize();
        let mut file = OpenOptions::new()
            .append(true)
            .open(&self.vector_path)
            .unwrap();
        let metadata = file.metadata().unwrap();
        let mut segment = FileSegment {
            start: metadata.len(),
            end: metadata.len(),
        };
        file.write_all(&bytes).unwrap();
        file.flush().unwrap();
        segment.end += bytes.len() as u64;
        segment
    }
    pub fn get_vector(&self, segment: FileSegment) -> Option<Vector> {
        if MmapStorage::is_valid(&self.vector_storage, segment) {
            let start = segment.start as usize;
            let end = segment.end as usize;
            Some(Vector::deserialize(&self.vector_storage[start..end]))
        } else {
            None
        }
    }
    pub fn is_valid(mmap: &Mmap, segment: FileSegment) -> bool {
        mmap.len() > (segment.start as usize) && mmap.len() >= (segment.end as usize)
    }
    pub fn remap(&mut self) {
        let path = self.dir_path.clone();
        let _prev = std::mem::replace(self, MmapStorage::new(path.as_path()));
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    pub fn open_new() {
        let dir = tempfile::tempdir().unwrap();
        MmapStorage::new(dir.path());
    }
    #[test]
    pub fn write_read() {
        let dir = tempfile::tempdir().unwrap();
        let mut mmap = MmapStorage::new(dir.path());
        let key_segment = mmap.write_key("KEY_1".to_string());
        let vector_segment = mmap.write_vector(vec![1., 2., 3.].into());
        assert_eq!(mmap.get_key(key_segment), None);
        assert_eq!(mmap.get_vector(vector_segment), None);
        mmap.remap();
        assert_eq!(mmap.get_key(key_segment), Some("KEY_1".to_string()));
        assert_eq!(
            mmap.get_vector(vector_segment),
            Some(vec![1., 2., 3.].into())
        );
    }
}
