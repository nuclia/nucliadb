// TODO:
// -> workflow tests

use std::fs::{DirBuilder, File, OpenOptions};
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};

use fs2::FileExt;
use thiserror::Error;
use tracing::*;

use crate::database::{DBErr, VectorDB};
use crate::index::*;

const LOCK_FILE: &str = "dir.lock";
const STATE: &str = "state.bincode";
const TEMP_STATE: &str = "temp.bincode";
const STAMP: &str = "stamp.nuclia";
const TRANSACTIONS: &str = "transactions";
const DATABASE: &str = "database";
const SEGMENT: &str = "segment.vectors";
const DELETE_LOG: &str = "delete_log.bincode";

pub type DiskResult<T> = Result<T, DiskError>;
#[derive(Error, Debug)]
pub enum DiskError {
    #[error("IOErr: {0}")]
    IOErr(#[from] std::io::Error),
    #[error("BincodeErr: {0}")]
    BincodeErr(#[from] Box<bincode::ErrorKind>),
    #[error("DBErr: {0}")]
    DBErr(#[from] DBErr),
}

pub struct Lock {
    file: File,
}

impl Lock {
    pub fn new<P: AsRef<Path>>(path: P) -> DiskResult<Lock> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        file.lock_exclusive()?;
        Ok(Lock { file })
    }
}

pub struct WSegment {
    len: usize,
    handle: BufWriter<File>,
}
impl WSegment {
    fn new(handle: BufWriter<File>) -> WSegment {
        WSegment { len: 0, handle }
    }
    pub fn write(&mut self, buf: &[u8]) -> DiskResult<(usize, usize)> {
        use std::io::Write;
        let result = (self.len, self.len + buf.len());
        self.handle.write_all(buf)?;
        self.handle.flush()?;
        self.len += buf.len();
        Ok(result)
    }
}

pub struct WDeletelog(BufWriter<File>);
impl WDeletelog {
    pub fn write(mut self, data: &DeleteLog) -> DiskResult<()> {
        use std::io::Write;
        bincode::serialize_into(&mut self.0, data)?;
        self.0.flush()?;
        Ok(())
    }
}

pub struct WState<'a> {
    disk: DiskStructure<'a>,
}

impl<'a> WState<'a> {
    fn borrow_abort(&self) -> DiskResult<()> {
        let temp = self.disk.base_path.join(TEMP_STATE);
        if temp.exists() {
            std::fs::remove_file(&temp)?;
        }
        Ok(())
    }
    fn borrow_flush(&self) -> DiskResult<()> {
        let new = self.disk.base_path.join(TEMP_STATE);
        let old = self.disk.base_path.join(STATE);
        if new.exists() {
            std::fs::remove_file(&old)?;
            std::fs::rename(&new, &old)?;
        }
        Ok(())
    }
    pub fn write_state(&self, data: &State) -> DiskResult<()> {
        let writer = BufWriter::new(File::create(self.disk.base_path.join(TEMP_STATE))?);
        bincode::serialize_into(writer, &data)?;
        Ok(())
    }
    pub fn abort(self) -> DiskResult<DiskStructure<'a>> {
        self.borrow_abort()?;
        Ok(self.disk)
    }
    pub fn flush(self) -> DiskResult<DiskStructure<'a>> {
        self.borrow_flush()?;
        Ok(self.disk)
    }
}

pub struct DiskStructure<'a> {
    lock: Lock,
    base_path: &'a Path,
}

impl<'a> DiskStructure<'a> {
    fn transaction_path(&self, id: usize) -> PathBuf {
        self.base_path.join(TRANSACTIONS).join(&format!("txn_{id}"))
    }

    pub fn new(path: &'a Path) -> DiskResult<DiskStructure<'a>> {
        use std::io::{Error, ErrorKind};
        let base_path = path;
        if path.join(TEMP_STATE).exists() {
            Err(Error::new(ErrorKind::InvalidData, "temporal file exits").into())
        } else if path.join(STAMP).exists() {
            let lock = Lock::new(base_path.join(LOCK_FILE).as_path())?;
            Ok(DiskStructure { lock, base_path })
        } else {
            DirBuilder::new().create(base_path.join(TRANSACTIONS))?;
            DirBuilder::new().create(base_path.join(DATABASE))?;
            let lock = Lock::new(base_path.join(LOCK_FILE))?;
            let _db = VectorDB::new(base_path.join(DATABASE))?;
            let _stamp = File::create(base_path.join(STAMP))?;
            let _state = File::create(base_path.join(STATE))?;
            let disk = DiskStructure { lock, base_path };
            let wtoken = disk.into_wstate();
            wtoken.write_state(&State::default())?;
            Ok(wtoken.flush()?)
        }
    }
    pub fn delete_txn(&self, txn_id: usize) -> DiskResult<()> {
        let base_path = self.transaction_path(txn_id);
        Ok(std::fs::remove_dir_all(base_path)?)
    }
    pub fn create_txn(&self, txn_id: usize) -> DiskResult<(WSegment, WDeletelog)> {
        let base_path = self.transaction_path(txn_id);
        DirBuilder::new().create(&base_path)?;
        let dlog_handle = BufWriter::new(File::create(base_path.join(DELETE_LOG))?);
        let segment_handle = BufWriter::new(File::create(base_path.join(SEGMENT))?);
        let wdlog = WDeletelog(dlog_handle);
        let wsegment = WSegment::new(segment_handle);
        Ok((wsegment, wdlog))
    }

    pub fn txn_exists(&self, txn_id: usize) -> bool {
        self.transaction_path(txn_id).is_dir()
    }

    pub fn get_segment(&self, txn_id: usize) -> DiskResult<Segment> {
        let path = self.transaction_path(txn_id).join(SEGMENT);
        Ok(Segment::new(path)?)
    }
    pub fn get_delete_log(&self, txn_id: usize) -> DiskResult<DeleteLog> {
        let path = self.transaction_path(txn_id).join(DELETE_LOG);
        let reader = BufReader::new(File::open(path)?);
        Ok(bincode::deserialize_from(reader)?)
    }
    pub fn get_db(&self) -> DiskResult<VectorDB> {
        Ok(VectorDB::new(self.base_path.join(DATABASE))?)
    }
    pub fn get_state(&self) -> DiskResult<State> {
        let reader = BufReader::new(File::open(self.base_path.join(STATE))?);
        Ok(bincode::deserialize_from(reader)?)
    }
    pub fn into_wstate(self) -> WState<'a> {
        WState { disk: self }
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::path::Path;

    use tempfile::tempdir;

    use super::*;
    use crate::hnsw::Hnsw;
    use crate::index::DeleteLog;

    #[test]
    fn create() {
        let dir = tempdir().unwrap();
        let dir_path = dir.path();
        let disk = DiskStructure::new(dir_path).unwrap();
        assert!(dir_path.join(DATABASE).is_dir());
        assert!(dir_path.join(TRANSACTIONS).is_dir());
        assert!(dir_path.join(STATE).is_file());
        assert!(dir_path.join(STATE).is_file());
        assert!(dir_path.join(LOCK_FILE).is_file());
        assert!(dir_path.join(STAMP).is_file());
        disk.get_state().unwrap();
        disk.get_db().unwrap();
        assert!(disk.get_segment(0).is_err());
        assert!(disk.get_delete_log(0).is_err());
    }
    #[test]
    fn open_new() {
        let dir = tempdir().unwrap();
        let dir_path = dir.path();
        DiskStructure::new(dir_path).unwrap();
        DiskStructure::new(dir_path).unwrap();
    }
    #[test]
    fn create_txn() {
        let dir = tempdir().unwrap();
        let dir_path = dir.path();
        let disk = DiskStructure::new(dir_path).unwrap();
        let (wsegment, wdlog) = disk.create_txn(0).unwrap();
        wdlog.write(&DeleteLog::default()).unwrap();
        let metadata = std::fs::metadata(&disk.transaction_path(0).join(SEGMENT)).unwrap();
        assert_eq!(metadata.len(), 0);
        drop(wsegment);

        let (mut wsegment, wdlog) = disk.create_txn(1).unwrap();
        wdlog.write(&DeleteLog::default()).unwrap();
        let (s0, e0) = wsegment.write(&[1; 12]).unwrap();
        assert_eq!(s0, 0);
        assert_eq!(e0, 12);
        let metadata = std::fs::metadata(&disk.transaction_path(1).join(SEGMENT)).unwrap();
        assert_eq!(metadata.len(), 12);
        let (s1, e1) = wsegment.write(&[2; 12]).unwrap();
        assert_eq!(s1, e0);
        assert_eq!(e1, 24);
        let metadata = std::fs::metadata(&disk.transaction_path(1).join(SEGMENT)).unwrap();
        assert_eq!(metadata.len(), 24);
        drop(wsegment);

        assert!(disk.txn_exists(0));
        disk.get_segment(0).unwrap();
        disk.get_delete_log(0).unwrap();

        assert!(disk.txn_exists(1));
        disk.get_segment(1).unwrap();
        disk.get_delete_log(1).unwrap();
        let segment = disk.get_segment(1).unwrap();
        let slice0 = segment.get_vector(SegmentSlice { start: s0, end: e0 });
        let slice1 = segment.get_vector(SegmentSlice { start: s1, end: e1 });
        assert_eq!(slice0, Some([1; 12].as_slice()));
        assert_eq!(slice1, Some([2; 12].as_slice()));
    }

    #[test]
    fn delete_txn() {
        let dir = tempdir().unwrap();
        let dir_path = dir.path();
        let disk = DiskStructure::new(dir_path).unwrap();
        assert!(disk.delete_txn(0).is_err());

        let (wsegment, wdlog) = disk.create_txn(0).unwrap();
        wdlog.write(&DeleteLog::default()).unwrap();
        drop(wsegment);
        disk.delete_txn(0).unwrap();
        assert!(!disk.txn_exists(0));
        assert!(disk.get_segment(0).is_err());
        assert!(disk.get_delete_log(0).is_err());
        assert!(!disk.transaction_path(0).exists());
    }
    #[test]
    fn abort_write_state() {
        let dir = tempdir().unwrap();
        let dir_path = dir.path();
        let disk = DiskStructure::new(dir_path).unwrap();
        let state = std::fs::metadata(disk.base_path.join(STATE)).unwrap();
        let start = state.modified().unwrap();
        let token = disk.into_wstate();

        token.write_state(&State::default()).unwrap();
        assert!(dir_path.join(TEMP_STATE).is_file());
        let state = std::fs::metadata(dir_path.join(STATE)).unwrap();
        let mod1 = state.modified().unwrap();
        assert_eq!(start, mod1);

        token.write_state(&State::default()).unwrap();
        assert!(dir_path.join(TEMP_STATE).is_file());
        let state = std::fs::metadata(dir_path.join(STATE)).unwrap();
        let mod2 = state.modified().unwrap();
        assert_eq!(start, mod2);

        token.abort().unwrap();
        let state = std::fs::metadata(dir_path.join(STATE)).unwrap();
        let abort = state.modified().unwrap();
        assert_eq!(abort, mod1);
        assert!(!dir_path.join(TEMP_STATE).exists());
    }

    #[test]
    fn flush_write_state() {
        let dir = tempdir().unwrap();
        let dir_path = dir.path();
        let disk = DiskStructure::new(dir_path).unwrap();
        let state = std::fs::metadata(disk.base_path.join(STATE)).unwrap();
        let start = state.modified().unwrap();
        let token = disk.into_wstate();

        token.write_state(&State::default()).unwrap();
        token.flush().unwrap();
        let state = std::fs::metadata(dir_path.join(STATE)).unwrap();
        let mod1 = state.modified().unwrap();
        assert!(!dir_path.join(TEMP_STATE).exists());
        assert!(start < mod1);
    }

    #[test]
    fn sudden_abort() {
        let dir = tempdir().unwrap();
        let dir_path = dir.path();
        let disk = DiskStructure::new(dir_path).unwrap();
        let token = disk.into_wstate();
        token.write_state(&State::default()).unwrap();
        std::mem::drop(token); // leaves inconsistent state
        assert!(dir_path.join(TEMP_STATE).is_file());
        assert!(dir_path.join(STATE).is_file());
        match DiskStructure::new(dir_path) {
            Err(DiskError::IOErr(err)) => {
                assert_eq!(err.kind(), std::io::ErrorKind::InvalidData)
            }
            _ => panic!(),
        }
    }
}
