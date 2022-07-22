/*
    TODO: 
        -> Segment writing
        -> txn_creation
        -> workflow tests
*/

use crate::database::{DBErr, VectorDB};
use crate::hnsw::Hnsw;
use crate::index::*;
use fs2::FileExt;
use std::fs::{DirBuilder, File, OpenOptions};
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};
use thiserror::Error;

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
        File::create(seg_path)?;
        File::create(del_log_path)?;
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
        bincode::serialize_into(writer, &data)?;
        Ok(())
    }
}
pub struct DiskStructure<'a> {
    pub lock: Lock,
    pub base_path: &'a Path,
}

impl<'a> DiskStructure<'a> {
    pub fn new(path: &'a Path) -> DiskResult<DiskStructure<'a>> {
        let lock = if !path.join(STAMP).exists() {
            DirBuilder::new().create(path.join(TRANSACTIONS))?;
            DirBuilder::new().create(path.join(DATABASE))?;
            File::create(path.join(TXN_LOG))?;
            File::create(path.join(HNSW))?;
            VectorDB::new(path.join(DATABASE))?;
            File::create(path.join(STAMP))?;
            Lock::new(path.join(LOCK_FILE))?
        } else {
            Lock::new(path.join(LOCK_FILE).as_path())?
        };
        Ok(DiskStructure {
            lock,
            base_path: path,
        })
    }
    pub fn get_txn(&self, txn_id: usize) -> TxnEntity {
        TxnEntity {
            txn_id,
            base_path: self.base_path,
        }
    }
}

impl<'a> DiskReadable<Hnsw> for DiskStructure<'a> {
    fn read(&self) -> DiskResult<Hnsw> {
        let reader = BufReader::new(File::open(self.base_path.join(HNSW))?);
        Ok(bincode::deserialize_from(reader)?)
    }
}

impl<'a> DiskWritable<Hnsw> for DiskStructure<'a> {
    fn write(&self, data: &Hnsw) -> DiskResult<()> {
        let writer = BufWriter::new(File::open(self.base_path.join(HNSW))?);
        bincode::serialize_into(writer, &data)?;
        Ok(())
    }
}

impl<'a> DiskReadable<TransactionLog> for DiskStructure<'a> {
    fn read(&self) -> DiskResult<TransactionLog> {
        let reader = BufReader::new(File::open(self.base_path.join(TXN_LOG))?);
        Ok(bincode::deserialize_from(reader)?)
    }
}

impl<'a> DiskWritable<TransactionLog> for DiskStructure<'a> {
    fn write(&self, data: &TransactionLog) -> DiskResult<()> {
        let writer = BufWriter::new(File::open(self.base_path.join(TXN_LOG))?);
        bincode::serialize_into(writer, &data)?;
        Ok(())
    }
}

impl<'a> DiskReadable<VectorDB> for DiskStructure<'a> {
    fn read(&self) -> DiskResult<VectorDB> {
        Ok(VectorDB::new(self.base_path.join(DATABASE))?)
    }
}

#[cfg(test)]
mod tests {
    use super::{DiskReadable, DiskStructure, DiskWritable, Lock};
    use crate::disk_structure::{DATABASE, HNSW, LOCK_FILE, STAMP, TRANSACTIONS, TXN_LOG};
    use crate::hnsw::Hnsw;
    use crate::index::DeleteLog;
    use std::fs::File;
    use std::path::Path;
    use tempfile::tempdir;

    #[test]
    fn create() {
        let dir = tempdir().unwrap();
        let dir_path = dir.path();
        let _ = DiskStructure::new(dir_path).unwrap();
        assert!(Path::new(&dir_path.join(DATABASE)).exists());
        assert!(Path::new(&dir_path.join(TRANSACTIONS)).exists());
        assert!(Path::new(&dir_path.join(TXN_LOG)).exists());
        assert!(Path::new(&dir_path.join(HNSW)).exists());
        assert!(Path::new(&dir_path.join(LOCK_FILE)).exists());
        assert!(Path::new(&dir_path.join(STAMP)).exists());
    }
    #[test]
    fn open() {
        let dir = tempdir().unwrap();
        let dir_path = dir.path();
        let create_struct = DiskStructure::new(dir_path).unwrap();
        drop(create_struct);
        assert!(DiskStructure::new(dir_path).is_ok())
    }
}
