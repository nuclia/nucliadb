use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;

use nucliadb_service_interface::prelude::async_std::fs::DirBuilder;

use super::errors::DiskStructResult;
use super::{DiskReadable, DiskWritable, DELETE_LOG, SEGMENT, TRANSACTIONS};
use crate::delete_log::DeleteLog;
use crate::segment::Segment;

pub(crate) struct TxnEntity<'a> {
    pub base_path: &'a Path,
    pub txn_id: usize,
}

impl<'a> TxnEntity<'a> {
    pub fn create(&self, txn_id: usize) -> DiskStructResult<(Segment, DeleteLog)> {
        let txn_path = format!("txn_{}", self.txn_id);
        let seg_path = self
            .base_path
            .join(TRANSACTIONS)
            .join(txn_path.clone())
            .join(SEGMENT);
        let del_log_path = self
            .base_path
            .join(TRANSACTIONS)
            .join(txn_path.clone())
            .join(DELETE_LOG);
        DirBuilder::new().create(self.base_path.join(TRANSACTIONS).join(txn_path));
        File::create(seg_path.clone())?;
        File::create(del_log_path.clone())?;
        Ok((
            Segment::new(seg_path.as_path()),
            DeleteLog::new(del_log_path.as_path()),
        ))
    }
}

impl<'a> DiskReadable<Segment> for TxnEntity<'a> {
    fn read(&self) -> DiskStructResult<Segment> {
        let txn_path = format!("txn_{}", self.txn_id);
        Ok(Segment::new(
            self.base_path.join(SEGMENT).join(txn_path).as_path(),
        ))
    }
}

impl<'a> DiskReadable<DeleteLog> for TxnEntity<'a> {
    fn read(&self) -> DiskStructResult<DeleteLog> {
        let txn_path = format!("txn_{}", self.txn_id);
        let reader = BufReader::new(File::open(self.base_path.join(DELETE_LOG).join(txn_path))?);
        Ok(bincode::deserialize_from(reader)?)
    }
}

impl<'a> DiskWritable<DeleteLog> for TxnEntity<'a> {
    fn write(&self, data: DeleteLog) -> DiskStructResult<()> {
        let txn_path = format!("txn_{}", self.txn_id);
        let writer = BufWriter::new(File::open(self.base_path.join(DELETE_LOG).join(txn_path))?);
        Ok(bincode::serialize_into(writer, &data)?)
    }
}
