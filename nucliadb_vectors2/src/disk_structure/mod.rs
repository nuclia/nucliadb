// All stored data types, must be serialized and deserealized with bincode and will implment
// Default.
//
// The disk structure should be:
// -> stamp.nuclia $empty file that ensures that the current directory is a vectors index
// -> dir.lock  $file use to handle cocurrent accesses to the directory
// -> hnsw.bincode $file with the hnsw index;
// -> transactions/ $directory where the transactions are stored
// -> txn_0/ $transaction with id 0
// -> delete_log.bincode $file with the vectors deleted in this transaction
// -> segment.bincode, $file with the vectors added in this transaction
// -> txn_1/
// -> ...
// -> log.bincode $file that will be used to identify which transactions where processed.
// -> database/  $storage folder for the lmdb
//
//
// this module should provide the following functionality:
// -> given a path, check if is a vectors index, if not initialize
// -> get an unlocked lock to the structure
// -> /* Only if a locked lock is provided */ get the transactions log
// -> /* Only if a locked lock is provided */ get the hnsw
// -> /* Only if a locked lock is provided */ given a txn id, get the delete log
// -> /* Only if a locked lock is provided */ given a txn id, get the mmaped segment
// -> /* Only if a locked lock is provided */ given a txn id and the delete log, write it
// -> /* Only if a locked lock is provided */ given the hnsw, write it
// -> /* Only if a locked lock is provided */ given the transactions log, write it
// -> /* Only if a locked lock is provided */ open and return an instance of the database
// -> /* Only if a locked lock is provided */ create a new txn with a given id and return segment
// and delete_log files ready to be writen to.
//
//
// types:
// Hnsw (the hnsw index), in crate::hnsw::Hnsw;
// TransactionLog (the transaction log ) in crate::index::TransactionLog
// DeleteLog (the delete log) in crate::delete_log::DeleteLog
// Segment (type use for mmaping segments) in crate::segment::Segment
// LMBDStorage (type use for using the lmdb (with open and create)) in crate::database::LMBDStorage;

use std::fs::{DirBuilder, File, OpenOptions};
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};

use fs2::FileExt;

use crate::database::VectorDB;
use crate::hnsw::Hnsw;
use crate::index::*;

const LOCK_FILE: &str = "dir.lock";
const HNSW: &str = "hnsw.bincode";
const STAMP: &str = "stamp.nuclia";
const TXN_LOG: &str = "log.bincode";
const TRANSACTIONS: &str = "transactions";
const SEGMENT: &str = "segment.vectors";
const DELETE_LOG: &str = "delete_log.bincode";
const DATABASE: &str = "database";

pub type DiskResult<T> = Result<T, DiskError>;
pub trait DiskWritable<T> {
    fn write(&self, data: &T) -> DiskResult<()>;
}
pub trait DiskReadable<T> {
    fn read(&self) -> DiskResult<T>;
}

pub enum DiskError {
    IOErr(std::io::Error),
    BincodeErr(Box<bincode::ErrorKind>),
}

impl From<std::io::Error> for DiskError {
    fn from(e: std::io::Error) -> Self {
        DiskError::IOErr(e)
    }
}

impl From<Box<bincode::ErrorKind>> for DiskError {
    fn from(e: Box<bincode::ErrorKind>) -> Self {
        DiskError::BincodeErr(e)
    }
}

pub struct Lock {
    file: File,
}

impl Lock {
    pub fn new(path: &Path) -> DiskResult<Lock> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        file.lock_exclusive()?;
        Ok(Lock { file })
    }
}

pub struct TxnEntity<'a> {
    pub base_path: &'a Path,
    pub txn_id: usize,
}

impl<'a> TxnEntity<'a> {
    pub fn create(base_path: &Path, txn_id: usize) -> DiskResult<TxnEntity> {
        let txn_ent = TxnEntity { base_path, txn_id };
        let txn_path = format!("txn_{}", txn_id);
        let seg_path = base_path
            .join(TRANSACTIONS)
            .join(txn_path.clone())
            .join(SEGMENT);
        let del_log_path = base_path
            .join(TRANSACTIONS)
            .join(txn_path.clone())
            .join(DELETE_LOG);
        DirBuilder::new().create(base_path.join(TRANSACTIONS).join(txn_path))?;
        File::create(seg_path.clone())?;
        File::create(del_log_path.clone())?;
        txn_ent.write(&DeleteLog::default())?;
        Ok(txn_ent)
    }
}

impl<'a> DiskReadable<Segment> for TxnEntity<'a> {
    fn read(&self) -> DiskResult<Segment> {
        let txn_path = format!("txn_{}", self.txn_id);
        Ok(Segment::new(
            self.base_path.join(SEGMENT).join(txn_path).as_path(),
        ))
    }
}

impl<'a> DiskReadable<DeleteLog> for TxnEntity<'a> {
    fn read(&self) -> DiskResult<DeleteLog> {
        let txn_path = format!("txn_{}", self.txn_id);
        let reader = BufReader::new(File::open(self.base_path.join(DELETE_LOG).join(txn_path))?);
        Ok(bincode::deserialize_from(reader)?)
    }
}

impl<'a> DiskWritable<DeleteLog> for TxnEntity<'a> {
    fn write(&self, data: &DeleteLog) -> DiskResult<()> {
        let txn_path = format!("txn_{}", self.txn_id);
        let writer = BufWriter::new(File::open(self.base_path.join(DELETE_LOG).join(txn_path))?);
        Ok(bincode::serialize_into(writer, &data)?)
    }
}
pub struct LockedDiskStructure<'a> {
    pub lock: Lock,
    pub base_path: &'a Path,
}

impl<'a> LockedDiskStructure<'a> {
    pub fn new(path: &'a Path) -> DiskResult<LockedDiskStructure<'a>> {
        let lock = if !path.join(STAMP).exists() {
            DirBuilder::new().create(path)?;
            DirBuilder::new().create(path.join(TRANSACTIONS))?;
            DirBuilder::new().create(path.join(DATABASE))?;
            File::create(path.join(TXN_LOG))?;
            File::create(path.join(HNSW))?;
            VectorDB::create(path.join(DATABASE).as_path());
            File::create(path.join(STAMP))?;
            Lock::new(path.join(LOCK_FILE).as_path())?
        } else {
            Lock::new(path.join(LOCK_FILE).as_path())?
        };
        Ok(LockedDiskStructure {
            lock,
            base_path: path,
        })
    }
    pub fn with_txn_id(&self, txn_id: usize) -> TxnEntity {
        TxnEntity {
            txn_id,
            base_path: self.base_path,
        }
    }
}

impl<'a> DiskReadable<Hnsw> for LockedDiskStructure<'a> {
    fn read(&self) -> DiskResult<Hnsw> {
        let reader = BufReader::new(File::open(self.base_path.join(HNSW))?);
        Ok(bincode::deserialize_from(reader)?)
    }
}

impl<'a> DiskWritable<Hnsw> for LockedDiskStructure<'a> {
    fn write(&self, data: &Hnsw) -> DiskResult<()> {
        let writer = BufWriter::new(File::open(self.base_path.join(HNSW))?);
        Ok(bincode::serialize_into(writer, &data)?)
    }
}

impl<'a> DiskReadable<TransactionLog> for LockedDiskStructure<'a> {
    fn read(&self) -> DiskResult<TransactionLog> {
        let reader = BufReader::new(File::open(self.base_path.join(TXN_LOG))?);
        Ok(bincode::deserialize_from(reader)?)
    }
}

impl<'a> DiskWritable<TransactionLog> for LockedDiskStructure<'a> {
    fn write(&self, data: &TransactionLog) -> DiskResult<()> {
        let writer = BufWriter::new(File::open(self.base_path.join(TXN_LOG))?);
        Ok(bincode::serialize_into(writer, &data)?)
    }
}

impl<'a> DiskReadable<VectorDB> for LockedDiskStructure<'a> {
    fn read(&self) -> DiskResult<VectorDB> {
        Ok(VectorDB::open(self.base_path.join(DATABASE).as_path()))
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::path::Path;

    use tempfile::tempdir;

    use super::{DiskReadable, DiskStructure, DiskWritable};
    use crate::delete_log::DeleteLog;
    use crate::disk_structure::{DATABASE, HNSW, LOCK_FILE, STAMP, TRANSACTIONS, TXN_LOG};
    use crate::hnsw::Hnsw;

    #[test]
    fn check_init() {
        let dir = tempdir().unwrap();
        let dir_path = dir.path();

        let _ = DiskStructure::new(&dir_path.to_owned()).unwrap();

        assert!(Path::new(&dir_path.join(DATABASE)).exists());
        assert!(Path::new(&dir_path.join(TRANSACTIONS)).exists());
        assert!(Path::new(&dir_path.join(TRANSACTIONS).join(TXN_LOG)).exists());
        assert!(Path::new(&dir_path.join(HNSW)).exists());
        assert!(Path::new(&dir_path.join(LOCK_FILE)).exists());
        assert!(Path::new(&dir_path.join(STAMP)).exists());
    }

    #[test]
    fn check_init_on_existed() {
        let dir = tempdir().unwrap();
        let dir_path = dir.path();

        let create_struct = DiskStructure::new(dir_path).unwrap();
        drop(create_struct);

        assert!(DiskStructure::new(dir_path).is_ok())
    }

    #[test]
    fn check_fail_on_corrupted() {
        let dir = tempdir().unwrap();
        let dir_path = dir.path();

        let create_struct = DiskStructure::new(dir_path).unwrap();
        drop(create_struct);

        std::fs::remove_file(dir_path.join(LOCK_FILE)).unwrap();
        assert!(DiskStructure::new(dir_path).is_err());
        let _ = std::fs::File::create(dir_path.join(LOCK_FILE)).unwrap();

        std::fs::remove_file(dir_path.join(STAMP)).unwrap();
        assert!(DiskStructure::new(dir_path).is_err());
        let _ = std::fs::File::create(dir_path.join(STAMP)).unwrap();

        std::fs::remove_file(dir_path.join(HNSW)).unwrap();
        assert!(DiskStructure::new(dir_path).is_err());
        let _ = std::fs::File::create(dir_path.join(HNSW)).unwrap();

        std::fs::remove_dir_all(dir_path.join(TRANSACTIONS)).unwrap();
        assert!(DiskStructure::new(dir_path).is_err());
        std::fs::DirBuilder::new()
            .create(dir_path.join(TRANSACTIONS))
            .unwrap();
        assert!(DiskStructure::new(dir_path).is_err());
        let _ = std::fs::File::create(dir_path.join(TRANSACTIONS).join(TXN_LOG)).unwrap();

        std::fs::remove_dir_all(dir_path.join(DATABASE)).unwrap();
        assert!(DiskStructure::new(dir_path).is_err());
    }

    #[test]
    fn check_lock_unlock() {
        let dir = tempdir().unwrap();
        let dir_path = dir.path();

        let disk_struct = DiskStructure::new(dir_path).unwrap();
        assert!(File::open(dir_path.join(LOCK_FILE)).is_err());
        std::mem::drop(disk_struct);
        assert!(File::open(dir_path.join(LOCK_FILE)).is_ok());
    }
}
