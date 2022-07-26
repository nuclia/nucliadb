use super::TransactionLog;
use crate::disk_structure::{DiskResult, DiskStructure};
use std::path::Path;

pub trait GCTransactionLog {
    type Iter: Iterator<Item = (usize, bool)>; // (txn_id, is_up)
    fn txns(&self) -> Self::Iter;
    fn get_txn(&self, txn_id: usize) -> bool; // true = txn_id is up, false otherwise
}

/*
    Given a DiskStructure and a value of type T (which implements GCTransactionLog) this function will:
        -> remove from disk all the transactions that are no longer up.
    This function should return an error if any of the disk operations fail.
*/
fn collect_garbage<'a, T, I>(disk: &'a DiskStructure<'a>, txn_log: &T) -> DiskResult<()>
where
    T: GCTransactionLog<Iter = I>,
{
    todo!()
}
