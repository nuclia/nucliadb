use crate::disk_structure::{DiskResult, DiskStructure};

/// Remove from disk all transactions that are no longer up.
/// Return an error if any of the disk operations fail
pub fn collect_garbage<'a, T>(disk: &'a DiskStructure<'a>, txn_log: T) -> DiskResult<()>
where
    T: Iterator<Item = &'a (usize, bool)>, // (txn_id, is_up)
{
    for (txn_id, is_up) in txn_log {
        if !is_up {
            disk.delete_txn(*txn_id)?;
        }
    }

    // todo!("Delete also delete logs from database");

    Ok(())
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn test_collect_garbage() {
        // Create some transactions to work with.
        // Use a mock iterator to mark transactions to delete.
        // Call `collect_garbage` and test the correct transaction
        // deletion.

        let dir = tempdir().unwrap();
        let dir_path = dir.path();
        let disk = DiskStructure::new(dir_path).unwrap();

        disk.create_txn(0).unwrap();
        disk.create_txn(1).unwrap();
        disk.create_txn(2).unwrap();
        disk.create_txn(3).unwrap();

        let fake_txns = vec![(0, true), (1, false), (2, false), (3, true)];

        let result = collect_garbage(&disk, fake_txns.iter());
        assert!(result.is_ok());

        assert!(disk.txn_exists(0));
        assert!(!disk.txn_exists(1));
        assert!(!disk.txn_exists(2));
        assert!(disk.txn_exists(3));
    }
}
