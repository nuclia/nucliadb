// TODO:
// -> workflow tests

use std::fs::{DirBuilder, File, OpenOptions};
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

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
    dir: PathBuf,
    #[allow(unused)]
    lock: File,
}
impl Lock {
    fn open_lock(path: &Path) -> DiskResult<File> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        Ok(file)
    }
    fn exclusive(dir: &Path) -> DiskResult<Lock> {
        let dir = dir.to_path_buf();
        let lock = Lock::open_lock(&dir.join(LOCK_FILE))?;
        lock.lock_exclusive()?;
        Ok(Lock { lock, dir })
    }
    fn shared(dir: &Path) -> DiskResult<Lock> {
        let dir = dir.to_path_buf();
        let lock = Lock::open_lock(&dir.join(LOCK_FILE))?;
        lock.lock_shared()?;
        Ok(Lock { lock, dir })
    }
}
impl AsRef<Path> for Lock {
    fn as_ref(&self) -> &Path {
        &self.dir
    }
}

pub struct ELock(Lock);
impl ELock {
    pub fn new(path: &Path) -> DiskResult<ELock> {
        Lock::exclusive(path).map(ELock)
    }
    pub fn commit(self) -> DiskResult<()> {
        let new = self.as_ref().join(TEMP_STATE);
        let old = self.as_ref().join(STATE);
        if new.exists() {
            std::fs::remove_file(&old)?;
            std::fs::rename(&new, &old)?;
        }
        Ok(())
    }
}
impl std::ops::Deref for ELock {
    type Target = Lock;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl AsRef<Path> for ELock {
    fn as_ref(&self) -> &Path {
        self.0.as_ref()
    }
}

pub struct SLock(Lock);
impl SLock {
    pub fn new(path: &Path) -> DiskResult<SLock> {
        Lock::shared(path).map(SLock)
    }
}
impl std::ops::Deref for SLock {
    type Target = Lock;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl AsRef<Path> for SLock {
    fn as_ref(&self) -> &Path {
        self.0.as_ref()
    }
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Version(SystemTime);

pub struct WSegment {
    txn_id: usize,
    len: usize,
    handle: BufWriter<File>,
}
impl WSegment {
    fn new(txn_id: usize, handle: BufWriter<File>) -> WSegment {
        WSegment {
            len: 0,
            handle,
            txn_id,
        }
    }
    pub fn write(&mut self, buf: &[u8]) -> DiskResult<Address> {
        use std::io::Write;
        let (start, end) = (self.len, self.len + buf.len());
        self.handle.write_all(buf)?;
        self.handle.flush()?;
        self.len += buf.len();
        Ok(Address {
            txn_id: self.txn_id,
            slice: SegmentSlice { start, end },
        })
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

fn transaction_path(path: &Path, id: usize) -> PathBuf {
    path.join(TRANSACTIONS).join(&format!("txn_{id}"))
}
fn is_healthy(path: &Path) -> bool {
    !path.join(STAMP).is_file()
        || (path.join(DATABASE).is_dir()
            && path.join(TRANSACTIONS).is_dir()
            && path.join(STATE).is_file()
            && path.join(LOCK_FILE).is_file())
}

pub fn exclusive_lock(path: &Path) -> DiskResult<ELock> {
    ELock::new(path)
}
pub fn shared_lock(path: &Path) -> DiskResult<SLock> {
    SLock::new(path)
}
pub fn init_env(lock: &Lock) -> DiskResult<()> {
    use std::io::{Error, ErrorKind, Write};
    let base_path = lock.as_ref();
    if !is_healthy(base_path) {
        Err(Error::new(ErrorKind::InvalidData, "Corrupted disk").into())
    } else if !base_path.join(STAMP).exists() {
        DirBuilder::new().create(base_path.join(TRANSACTIONS))?;
        DirBuilder::new().create(base_path.join(DATABASE))?;
        let _db = VectorDB::new(base_path.join(DATABASE))?;
        let _stamp = File::create(base_path.join(STAMP))?;
        let mut state = File::create(base_path.join(STATE))?;
        state.write_all(&bincode::serialize(&State::default())?)?;
        state.flush()?;
        Ok(())
    } else {
        Ok(())
    }
}
pub fn delete_txn(lock: &ELock, txn_id: usize) -> DiskResult<()> {
    let base_path = transaction_path(lock.as_ref(), txn_id);
    Ok(std::fs::remove_dir_all(base_path)?)
}
pub fn write_state(lock: &Lock, data: &State) -> DiskResult<()> {
    let writer = BufWriter::new(File::create(lock.as_ref().join(TEMP_STATE))?);
    bincode::serialize_into(writer, &data)?;
    Ok(())
}
pub fn create_txn(lock: &Lock, txn_id: usize) -> DiskResult<(WSegment, WDeletelog)> {
    let base_path = transaction_path(lock.as_ref(), txn_id);
    DirBuilder::new().create(&base_path)?;
    let dlog_handle = BufWriter::new(File::create(base_path.join(DELETE_LOG))?);
    let segment_handle = BufWriter::new(File::create(base_path.join(SEGMENT))?);
    let wdlog = WDeletelog(dlog_handle);
    let wsegment = WSegment::new(txn_id, segment_handle);
    Ok((wsegment, wdlog))
}
pub fn read_segment(lock: &Lock, txn_id: usize) -> DiskResult<Segment> {
    let path = transaction_path(lock.as_ref(), txn_id).join(SEGMENT);
    Ok(Segment::new(path)?)
}
pub fn read_delete_log(lock: &Lock, txn_id: usize) -> DiskResult<DeleteLog> {
    let path = transaction_path(lock.as_ref(), txn_id).join(DELETE_LOG);
    let reader = BufReader::new(File::open(path)?);
    Ok(bincode::deserialize_from(reader)?)
}
pub fn open_db(lock: &Lock) -> DiskResult<VectorDB> {
    Ok(VectorDB::new(lock.as_ref().join(DATABASE))?)
}
pub fn read_state(lock: &Lock) -> DiskResult<(Version, State)> {
    let path = lock.as_ref().join(STATE);
    let meta = std::fs::metadata(&path)?;
    let updated = Version(meta.modified()?);
    let reader = BufReader::new(File::open(&path)?);
    Ok((updated, bincode::deserialize_from(reader)?))
}
pub fn update(lock: &Lock, current: Version, state: State) -> DiskResult<(Version, State)> {
    let meta = std::fs::metadata(lock.as_ref().join(STATE))?;
    let updated = Version(meta.modified()?);
    if updated > current {
        read_state(lock)
    } else {
        Ok((current, state))
    }
}

#[cfg(test)]
pub fn txn_exists(lock: &Lock, txn_id: usize) -> bool {
    transaction_path(lock.as_ref(), txn_id).is_dir()
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::index::DeleteLog;
    use tempfile::tempdir;

    #[test]
    fn create_test() {
        let dir = tempdir().unwrap();
        let lock = shared_lock(dir.path()).unwrap();
        init_env(&lock).unwrap();
        assert!(dir.path().join(DATABASE).is_dir());
        assert!(dir.path().join(TRANSACTIONS).is_dir());
        assert!(dir.path().join(STATE).is_file());
        assert!(dir.path().join(LOCK_FILE).is_file());
        assert!(dir.path().join(STAMP).is_file());
        read_state(&lock).unwrap();
        open_db(&lock).unwrap();
        assert!(!txn_exists(&lock, 0));
        assert!(read_segment(&lock, 0).is_err());
        assert!(read_delete_log(&lock, 0).is_err());
    }
    #[test]
    fn multiple_shared_test() {
        let dir = tempdir().unwrap();
        let lock0 = shared_lock(dir.path()).unwrap();
        let lock1 = shared_lock(dir.path()).unwrap();
        init_env(&lock0).unwrap();
        init_env(&lock1).unwrap();
    }
    #[test]
    fn create_txn_test() {
        let dir = tempdir().unwrap();
        let lock = exclusive_lock(dir.path()).unwrap();
        init_env(&lock).unwrap();
        let (wsegment, wdlog) = create_txn(&lock, 0).unwrap();
        wdlog.write(&DeleteLog::default()).unwrap();
        let metadata =
            std::fs::metadata(&transaction_path(lock.as_ref(), 0).join(SEGMENT)).unwrap();
        assert_eq!(metadata.len(), 0);
        drop(wsegment);

        let (mut wsegment, wdlog) = create_txn(&lock, 1).unwrap();
        wdlog.write(&DeleteLog::default()).unwrap();
        let Address {
            slice: slice0 @ SegmentSlice { start: s0, end: e0 },
            ..
        } = wsegment.write(&[1; 12]).unwrap();
        assert_eq!(s0, 0);
        assert_eq!(e0, 12);
        let metadata =
            std::fs::metadata(&transaction_path(lock.as_ref(), 1).join(SEGMENT)).unwrap();
        assert_eq!(metadata.len(), 12);
        let Address {
            slice: slice1 @ SegmentSlice { start: s1, end: e1 },
            ..
        } = wsegment.write(&[2; 12]).unwrap();
        assert_eq!(s1, e0);
        assert_eq!(e1, 24);
        let metadata =
            std::fs::metadata(&transaction_path(lock.as_ref(), 1).join(SEGMENT)).unwrap();
        assert_eq!(metadata.len(), 24);
        drop(wsegment);

        assert!(txn_exists(&lock, 0));
        read_segment(&lock, 0).unwrap();
        read_delete_log(&lock, 0).unwrap();

        assert!(txn_exists(&lock, 1));
        read_segment(&lock, 1).unwrap();
        read_delete_log(&lock, 1).unwrap();
        let segment = read_segment(&lock, 1).unwrap();
        let slice0 = segment.get_vector(slice0);
        let slice1 = segment.get_vector(slice1);
        assert_eq!(slice0, Some([1; 12].as_slice()));
        assert_eq!(slice1, Some([2; 12].as_slice()));
    }

    #[test]
    fn delete_txn_test() {
        let dir = tempdir().unwrap();
        let dir_path = dir.path();
        let lock = exclusive_lock(dir_path).unwrap();
        init_env(&lock).unwrap();
        assert!(delete_txn(&lock, 0).is_err());
        let (wsegment, wdlog) = create_txn(&lock, 0).unwrap();
        wdlog.write(&DeleteLog::default()).unwrap();
        drop(wsegment);
        delete_txn(&lock, 0).unwrap();
        assert!(!txn_exists(&lock, 0));
        assert!(read_segment(&lock, 0).is_err());
        assert!(read_delete_log(&lock, 0).is_err());
        assert!(!transaction_path(lock.as_ref(), 0).exists());
    }

    #[test]
    fn flush_write_state() {
        let dir = tempdir().unwrap();
        let lock = exclusive_lock(dir.path()).unwrap();
        init_env(&lock).unwrap();
        let state = std::fs::metadata(lock.as_ref().join(STATE)).unwrap();
        let start = state.modified().unwrap();
        write_state(&lock, &State::default()).unwrap();
        assert!(lock.as_ref().join(TEMP_STATE).exists());
        assert!(lock.as_ref().join(STATE).exists());
        let state = std::fs::metadata(lock.as_ref().join(STATE)).unwrap();
        let mod1 = state.modified().unwrap();
        assert!(start == mod1);
        lock.commit().unwrap();
        let lock = shared_lock(dir.path()).unwrap();
        let state = std::fs::metadata(lock.as_ref().join(STATE)).unwrap();
        let mod2 = state.modified().unwrap();
        assert!(!lock.as_ref().join(TEMP_STATE).exists());
        assert!(start < mod2);
    }

    #[test]
    fn sudden_abort() {
        let dir = tempdir().unwrap();
        let lock = exclusive_lock(dir.path()).unwrap();
        init_env(&lock).unwrap();
        write_state(&lock, &State::default()).unwrap();
        std::mem::drop(lock); // leaves garbage
        assert!(dir.path().join(TEMP_STATE).is_file());
        assert!(dir.path().join(STATE).is_file());

        // Is still possible to try again
        let lock = exclusive_lock(dir.path()).unwrap();
        init_env(&lock).unwrap(); // Still a valid state
        write_state(&lock, &State::default()).unwrap();
        lock.commit().unwrap();
        assert!(!dir.path().join(TEMP_STATE).is_file());
        assert!(dir.path().join(STATE).is_file());
    }

    #[test]
    fn invalid_state() {
        let dir = tempdir().unwrap();
        let lock = exclusive_lock(dir.path()).unwrap();
        init_env(&lock).unwrap();
        write_state(&lock, &State::default()).unwrap();
        std::mem::drop(lock);
        std::fs::remove_file(dir.path().join(STATE)).unwrap(); // Invalid state
        assert!(dir.path().join(TEMP_STATE).is_file());
        let lock = shared_lock(dir.path()).unwrap();
        if let Err(DiskError::IOErr(err)) = init_env(&lock) {
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidData)
        } else {
            panic!("Expecting an error")
        }
        std::fs::rename(dir.path().join(TEMP_STATE), dir.path().join(STATE)).unwrap();
        assert!(init_env(&lock).is_ok());
    }
}
