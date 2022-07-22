use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;

use super::errors::DiskStructResult;
use super::txn_entity::TxnEntity;
use super::{DiskReadable, DiskWritable, DATABASE, HNSW, TXN_LOG};
use crate::database::VectorDB;
use crate::hnsw::Hnsw;
use crate::index::TransactionLog;
use crate::lock::Lock;

pub(crate) struct LockedDiskStructure<'a> {
    pub lockfile: Lock,
    pub base_path: &'a Path,
}

impl<'a> LockedDiskStructure<'a> {
    pub fn with_txn_id(&self, txn_id: usize) -> TxnEntity {
        TxnEntity {
            txn_id,
            base_path: self.base_path,
        }
    }
}

impl<'a> DiskReadable<Hnsw> for LockedDiskStructure<'a> {
    fn read(&self) -> DiskStructResult<Hnsw> {
        let reader = BufReader::new(File::open(self.base_path.join(HNSW))?);
        Ok(bincode::deserialize_from(reader)?)
    }
}

impl<'a> DiskWritable<Hnsw> for LockedDiskStructure<'a> {
    fn write(&self, data: Hnsw) -> DiskStructResult<()> {
        let writer = BufWriter::new(File::open(self.base_path.join(HNSW))?);
        Ok(bincode::serialize_into(writer, &data)?)
    }
}

impl<'a> DiskReadable<TransactionLog> for LockedDiskStructure<'a> {
    fn read(&self) -> DiskStructResult<TransactionLog> {
        let reader = BufReader::new(File::open(self.base_path.join(TXN_LOG))?);
        Ok(bincode::deserialize_from(reader)?)
    }
}

impl<'a> DiskWritable<TransactionLog> for LockedDiskStructure<'a> {
    fn write(&self, data: TransactionLog) -> DiskStructResult<()> {
        let writer = BufWriter::new(File::open(self.base_path.join(TXN_LOG))?);
        Ok(bincode::serialize_into(writer, &data)?)
    }
}

impl<'a> DiskReadable<VectorDB> for LockedDiskStructure<'a> {
    fn read(&self) -> DiskStructResult<VectorDB> {
        Ok(VectorDB::open(self.base_path.join(DATABASE).as_path()))
    }
}
