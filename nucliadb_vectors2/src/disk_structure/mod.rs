// All st&&ored data types, must be serialized and deserealized with bincode and will implment
// Default.
//
// The disk structure should be:
// -> stamp.nuclia $empty file that ensures that the current directory is a vectors index
// -> dir.lock  $file use to handle cocurrent accesses to the directory
// -> hnsw.bincode $file with the hnsw index;
// -> transactions/ $directory where the transactions are stored
// -> txn_0/ $transaction with id 0
// -> delete_log.bincode $file with the vectors deleted in this transaction
// -> segment.vectors, $file with the vectors added in this transaction
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
// -> /* Only if a locked lock is provided */ given the hnsw, write it
// -> /* Only if a locked lock is provided */ given the transactions log, write it
// -> /* Only if a locked lock is provided */ open and return an instance of the database
// -> /* Only if a locked lock is provided */ given a txn id, get the delete log
// -> /* Only if a locked lock is provided */ given a txn id, get the mmaped segment
// -> /* Only if a locked lock is provided */ given a txn id and the delete log, write it
// -> /* Only if a locked lock is provided */ given a txn id, create a new txn with a given id and
// return segment and delete_log files ready to be writen to.
//
//
// types:
// Hnsw (the hnsw index), in crate::hnsw::Hnsw;
// TransactionLog (the transaction log ) in crate::index::TransactionLog
// DeleteLog (the delete log) in crate::delete_log::DeleteLog
// Segment (type use for mmaping segments) in crate::segment::Segment
// LMBDStorage (type use for using the lmdb (with open and create)) in crate::database::LMBDStorage;

mod errors;
mod locked_struct;
mod txn_entity;

use std::fs::{DirBuilder, File};
use std::path::{Path, PathBuf};

use errors::DiskStructResult;
use heed::Database;
use locked_struct::LockedDiskStructure;
use nucliadb_service_interface::prelude::ServiceResult;
use txn_entity::TxnEntity;

use self::errors::DiskStructError;
use crate::database::VectorDB;
use crate::lock::Lock;

pub(in crate::disk_structure) const LOCK_FILE: &str = "dir.lock";
pub(in crate::disk_structure) const HNSW: &str = "hnsw.bincode";
pub(in crate::disk_structure) const STAMP: &str = "stamp.nuclia";
pub(in crate::disk_structure) const TXN_LOG: &str = "log.bincode";
pub(in crate::disk_structure) const TRANSACTIONS: &str = "transactions";
pub(in crate::disk_structure) const SEGMENT: &str = "segment.vectors";
pub(in crate::disk_structure) const DELETE_LOG: &str = "delete_log.bincode";
pub(in crate::disk_structure) const DATABASE: &str = "database";

pub(crate) trait DiskWritable<T> {
    fn write(&self, data: T) -> DiskStructResult<()>;
}
pub(crate) trait DiskReadable<T> {
    fn read(&self) -> DiskStructResult<T>;
}

pub(crate) struct DiskStructure {}

impl<'a> DiskStructure {
    pub fn new(path: &'a Path) -> DiskStructResult<LockedDiskStructure<'a>> {
        let lockfile = if path.exists() {
            if !(path.join(STAMP).exists()
                && path.join(TRANSACTIONS).exists()
                && path.join(DATABASE).exists()
                && path.join(TXN_LOG).exists()
                && path.join(HNSW).exists()
                && path.join(LOCK_FILE).exists())
            {
                return Err(DiskStructError::InitErr);
            }
            Lock::new(path.join(LOCK_FILE).as_path())
        } else {
            DirBuilder::new().create(path)?;
            DirBuilder::new().create(path.join(TRANSACTIONS))?;
            DirBuilder::new().create(path.join(DATABASE))?;
            File::create(path.join(TXN_LOG))?;
            File::create(path.join(HNSW))?;
            VectorDB::create(path.join(DATABASE).as_path());
            File::create(path.join(STAMP))?;
            Lock::new(path.join(LOCK_FILE).as_path())
        };
        Ok(LockedDiskStructure {
            lockfile,
            base_path: path,
        })
    }
}

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
