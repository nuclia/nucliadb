// Copyright (C) 2021 Bosutech XXI S.L.
//
// nucliadb is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at info@nuclia.com.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.
//

use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{Seek, Write};
use std::path::{Path, PathBuf};

use super::fs_state::{self, FsResult, Version};
use fs2::FileExt;
use serde::{Deserialize, Serialize};

use crate::data_point::{DpId, Journal};
use crate::data_types::dtrie_ram::DTrie;
use crate::data_types::DeleteLog;
use crate::VectorR;

use super::state_v1;

type TxId = u64;
type SegmentId = DpId;

#[derive(Clone, Copy)]
struct TimeSensitiveDLog<'a> {
    dlog: &'a DTrie<u64>,
    transaction: TxId,
}
impl<'a> DeleteLog for TimeSensitiveDLog<'a> {
    fn is_deleted(&self, key: &[u8]) -> bool {
        self.dlog.get(key).map(|t| t > self.transaction).unwrap_or_default()
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Copy)]
struct JournalEntry {
    txid: TxId,
    operation: Operation,
}

impl JournalEntry {
    fn apply(&self, segments: &mut HashMap<SegmentId, TxId>) {
        self.operation.apply(segments, self.txid);
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Copy)]
enum Operation {
    AddSegment(SegmentId),
    DeleteSegment(SegmentId),
}

impl Operation {
    fn apply(&self, segments: &mut HashMap<SegmentId, TxId>, txid: TxId) {
        match self {
            Operation::AddSegment(dpid) => {
                segments.insert(*dpid, txid);
            }
            Operation::DeleteSegment(dpid) => {
                segments.remove(dpid);
            }
        };
    }
}

#[derive(Serialize, Deserialize, Default, Clone)]
struct State {
    journal: Vec<JournalEntry>,
    delete_log: DTrie<u64>,
    no_nodes: usize,
}

#[derive(Default)]
pub struct Transaction {
    operations: Vec<Operation>,
    deleted_entries: Vec<String>,
    no_nodes: i64,
}

impl Transaction {
    pub fn add_segment(&mut self, dp_journal: Journal) {
        self.operations.push(Operation::AddSegment(dp_journal.id()));
        self.no_nodes += dp_journal.no_nodes() as i64;
    }

    pub fn delete_segment(&mut self, dp_journal: Journal) {
        self.operations.push(Operation::DeleteSegment(dp_journal.id()));
        self.no_nodes -= dp_journal.no_nodes() as i64;
    }

    pub fn delete_entry(&mut self, prefix: String) {
        self.deleted_entries.push(prefix);
    }

    pub fn is_empty(&self) -> bool {
        self.operations.is_empty() && self.deleted_entries.is_empty()
    }
}

const STATE_FILE_EXTENSION: &str = "segstate";

struct StateFile {
    path: PathBuf,
    file: File,
}

impl StateFile {
    fn new(path: PathBuf) -> FsResult<Self> {
        let reader_id = uuid::Uuid::new_v4();
        let tmp_path = path.join(format!("{}.{STATE_FILE_EXTENSION}-tmp", reader_id));
        let lock_path = path.join(format!("{}.{STATE_FILE_EXTENSION}", reader_id));

        // Open and lock the file atomically via rename
        let file = OpenOptions::new().create(true).write(true).open(&tmp_path)?;
        file.lock_exclusive()?;
        fs::rename(tmp_path, lock_path)?;

        Ok(StateFile {
            path,
            file,
        })
    }

    fn write(&mut self, data: impl Serialize) -> FsResult<()> {
        self.file.seek(std::io::SeekFrom::Start(0))?;
        bincode::serialize_into(&mut self.file, &data)?;
        self.file.flush()?;

        Ok(())
    }

    fn try_read<D: for<'a> Deserialize<'a>>(path: &Path) -> VectorR<Option<D>> {
        let Some(extension) = path.extension() else {
            return Ok(None);
        };
        if extension != OsStr::new(STATE_FILE_EXTENSION) {
            return Ok(None);
        }
        let file = File::open(path)?;
        let is_locked = file.try_lock_exclusive().is_err();
        if !is_locked {
            std::fs::remove_file(path)?;
            return Ok(None);
        }
        let data = bincode::deserialize_from(file)?;
        Ok(Some(data))
    }
}

impl Drop for StateFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

pub struct SegmentManager {
    state: State,
    state_version: Version,

    segments: HashMap<SegmentId, TxId>,
    path: PathBuf,
    state_file: StateFile,
}

impl SegmentManager {
    fn txid(&self) -> TxId {
        self.state.journal.last().map(|e| e.txid).unwrap_or(0)
    }

    pub fn commit(&mut self, transaction: Transaction) -> VectorR<()> {
        let old_state = self.state.clone();

        let mut next_txid = self.txid() + 1;
        for prefix in &transaction.deleted_entries {
            self.state.delete_log.insert(prefix.as_bytes(), next_txid)
        }

        let has_operations = !transaction.operations.is_empty();
        let mut operations = None;
        if has_operations {
            let has_delete = transaction.operations.iter().any(|op| matches!(op, Operation::DeleteSegment(_)));
            let new_no_nodes = self.state.no_nodes as i64 + transaction.no_nodes;
            self.state.no_nodes = new_no_nodes.try_into().unwrap_or(0);

            let pos = self.state.journal.len();
            for operation in transaction.operations {
                self.state.journal.push(JournalEntry {
                    operation,
                    txid: next_txid,
                });
                next_txid += 1;
            }

            operations = Some(&self.state.journal[pos..]);

            // We can prune the delete_log at the point of the oldest segment still in use
            if has_delete {
                self.state.delete_log.prune(self.oldest_live_txid());
            }
        }

        match self.save() {
            Ok(_) => {
                // Apply the changes to the segment view
                for entry in operations.unwrap_or(&[]) {
                    entry.apply(&mut self.segments);
                }
                Ok(())
            }
            Err(e) => {
                // Rollback
                self.state = old_state;
                Err(e)
            }
        }?;
        self.write_state()?;

        Ok(())
    }

    fn oldest_live_txid(&self) -> TxId {
        *self.segments.values().min().unwrap_or(&0)
    }

    pub fn no_nodes(&self) -> usize {
        self.state.no_nodes
    }

    pub fn compact(&mut self) -> VectorR<()> {
        // We can only compact transactions that are not in use by any other reader (oldest_txid_in_use)
        let oldest_txid_in_use = self.oldest_txid_in_use()?.unwrap_or(self.txid());

        let mut segments = HashMap::new();
        let mut count = 0;
        for entry in &self.state.journal {
            if entry.txid > oldest_txid_in_use {
                break;
            }
            count += 1;
            entry.apply(&mut segments);
        }
        if count == 0 {
            return Ok(());
        }

        // Remove segments that were later deleted and deletion operations. Remove empty transactions.
        let new_transactions: Vec<JournalEntry> = self.state.journal[0..count]
            .iter()
            .filter(|e| {
                if let Operation::AddSegment(s) = e.operation {
                    segments.contains_key(&s)
                } else {
                    false
                }
            })
            .copied()
            .collect();

        let old_state = self.state.clone();
        self.state.journal.splice(0..count, new_transactions);

        if let Err(e) = self.save() {
            // Rollback
            self.state = old_state;
            return Err(e.into());
        };

        Ok(())
    }

    fn oldest_txid_in_use(&self) -> VectorR<Option<TxId>> {
        let mut oldest = None;
        for dir_entry in std::fs::read_dir(&self.path)? {
            let dir_entry = dir_entry?;
            let path = dir_entry.path();
            if !path.is_file() {
                continue;
            };
            let reader_version: Option<TxId> = StateFile::try_read(&path)?;
            if let Some(reader_version) = reader_version {
                oldest = match oldest {
                    None => Some(reader_version),
                    Some(oldest) => Some(std::cmp::min(oldest, reader_version)),
                };
            }
        }
        Ok(oldest)
    }

    pub fn save(&self) -> FsResult<()> {
        fs_state::persist_state(&self.path, &self.state)
    }

    fn write_state(&mut self) -> VectorR<()> {
        self.state_file.write(self.txid())?;
        Ok(())
    }

    pub fn open(path: PathBuf) -> VectorR<Self> {
        let state_version = fs_state::crnt_version(&path)?;
        let state: State = fs_state::load_state(&path)?;
        let mut segments = HashMap::new();
        for entry in state.journal.iter() {
            entry.apply(&mut segments);
        }

        let state_file = StateFile::new(path.clone())?;
        let mut sm = SegmentManager {
            state,
            path,
            state_file,
            state_version,
            segments,
        };
        sm.write_state()?;
        Ok(sm)
    }

    pub fn create(path: PathBuf) -> VectorR<Self> {
        fs_state::initialize_disk(&path, State::default)?;

        Self::open(path)
    }

    pub fn from_v1_state(path: &Path, v1: state_v1::State, state_version: Version) -> VectorR<Self> {
        let mut state = State {
            no_nodes: v1.no_nodes(),
            ..Default::default()
        };

        let mut time_map = Vec::new();
        let mut segments = HashMap::new();
        let mut v1_segments: Vec<_> = v1.data_point_iterator().collect();
        v1_segments.sort_by_key(|j| j.time());
        let mut txid = 0u64;
        for segment in v1_segments {
            let time = segment.time();
            txid += 1;
            time_map.push(time);

            state.journal.push(JournalEntry {
                txid,
                operation: Operation::AddSegment(segment.id()),
            });
            segments.insert(segment.id(), txid);
        }
        state.delete_log = v1.delete_log.convert(&|delete_time| {
            let mut new_value = 0;
            for (tx_idx, tx_time) in time_map.iter().enumerate() {
                if delete_time > tx_time {
                    // Delete time is later than this transaction, so
                    // we can say it happens at the next transaction
                    // The transactions are 1-indexed and vec is 0-indexed
                    // So idx + 1 = txid. txid + 1 = next_txid
                    new_value = tx_idx as u64 + 2;
                }
            }
            new_value
        });

        let state_file = StateFile::new(path.to_path_buf())?;

        let mut sm = SegmentManager {
            state,
            state_version,
            segments,
            path: path.to_path_buf(),
            state_file,
        };
        sm.write_state()?;
        Ok(sm)
    }

    pub fn refresh(&mut self) -> VectorR<()> {
        let mut old_txid = self.txid();
        self.state = fs_state::load_state(&self.path)?;
        self.state_version = fs_state::crnt_version(&self.path)?;

        if self.txid() < old_txid {
            // The new txid is older than the previous one, this can happen if the shard is cleaned/upgraded in-place
            // In this case, we reset our segments view to force to reload the entire journal
            self.segments.clear();
            old_txid = 0;
        }

        // Apply the changes to our segments view from the journal, considering transactions since the last we had
        for entry in &self.state.journal {
            if entry.txid <= old_txid {
                continue;
            }
            entry.apply(&mut self.segments);
        }

        self.write_state()?;
        Ok(())
    }

    pub fn needs_refresh(&self) -> FsResult<bool> {
        Ok(fs_state::crnt_version(&self.path)? > self.state_version)
    }

    // Returns active segments
    pub fn segment_iterator(&self) -> impl Iterator<Item = (impl DeleteLog + '_, SegmentId)> {
        self.segments.iter().map(|(id, time)| {
            (
                TimeSensitiveDLog {
                    transaction: *time,
                    dlog: &self.state.delete_log,
                },
                *id,
            )
        })
    }

    // Returns all segments in the log, including deleted ones
    pub fn all_segments_iterator(&self) -> impl Iterator<Item = &SegmentId> {
        self.state.journal.iter().filter_map(|op| {
            if let Operation::AddSegment(ref id) = op.operation {
                Some(id)
            } else {
                None
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tempfile::TempDir;

    use super::{SegmentId, SegmentManager};
    use crate::data_point_provider::segment_manager::{JournalEntry, Operation, Transaction};
    use crate::VectorR;

    #[test]
    fn test_compact() -> VectorR<()> {
        let dir = TempDir::new().unwrap();
        let mut manager = SegmentManager::create(dir.path().to_path_buf()).unwrap();
        let segments =
            [SegmentId::new_v4(), SegmentId::new_v4(), SegmentId::new_v4(), SegmentId::new_v4(), SegmentId::new_v4()];
        // Insert (txid=1)
        manager.commit(Transaction {
            operations: vec![Operation::AddSegment(segments[0])],
            ..Default::default()
        })?;
        // Insert (txid=2)
        manager.commit(Transaction {
            operations: vec![Operation::AddSegment(segments[1])],
            ..Default::default()
        })?;
        // Insert (txid=3)
        manager.commit(Transaction {
            operations: vec![Operation::AddSegment(segments[2])],
            ..Default::default()
        })?;
        // Merge (txid=4,5,6)
        manager.commit(Transaction {
            operations: vec![
                Operation::DeleteSegment(segments[0]),
                Operation::DeleteSegment(segments[2]),
                Operation::AddSegment(segments[3]),
            ],
            ..Default::default()
        })?;
        // Insert (txid=7)
        manager.commit(Transaction {
            operations: vec![Operation::AddSegment(segments[4])],
            ..Default::default()
        })?;

        let mut expected = HashMap::new();
        expected.insert(segments[1], 2);
        expected.insert(segments[3], 6);
        expected.insert(segments[4], 7);
        assert_eq!(manager.segments, expected);

        manager.compact()?;
        // Does not change the current state
        assert_eq!(manager.segments, expected);

        // But changes the first entries of the journal
        assert_eq!(
            manager.state.journal,
            [
                JournalEntry {
                    txid: 2,
                    operation: Operation::AddSegment(segments[1])
                },
                JournalEntry {
                    txid: 6,
                    operation: Operation::AddSegment(segments[3])
                },
                JournalEntry {
                    txid: 7,
                    operation: Operation::AddSegment(segments[4])
                },
            ]
        );

        Ok(())
    }

    #[test]
    fn test_reader_state() -> VectorR<()> {
        let dir = TempDir::new()?;
        let mut writer = SegmentManager::create(dir.path().to_path_buf())?;
        let segments = [SegmentId::new_v4(), SegmentId::new_v4(), SegmentId::new_v4(), SegmentId::new_v4()];
        writer.commit(Transaction {
            operations: vec![Operation::AddSegment(segments[0])],
            ..Default::default()
        })?;
        writer.commit(Transaction {
            operations: vec![Operation::AddSegment(segments[1])],
            ..Default::default()
        })?;
        let mut reader_1 = SegmentManager::open(dir.path().to_path_buf())?;

        writer.commit(Transaction {
            operations: vec![
                Operation::AddSegment(segments[2]),
                Operation::DeleteSegment(segments[0]),
                Operation::DeleteSegment(segments[1]),
            ],
            ..Default::default()
        })?;

        let mut reader_2 = SegmentManager::open(dir.path().to_path_buf())?;

        writer.commit(Transaction {
            operations: vec![Operation::AddSegment(segments[3])],
            ..Default::default()
        })?;

        // Writer(pos 6), Reader1(pos 2), Reader2(pos 5)
        assert_eq!(writer.oldest_txid_in_use()?.unwrap(), 2);
        writer.compact()?;
        // Nothing changes
        assert_eq!(
            writer.state.journal,
            [
                JournalEntry {
                    txid: 1,
                    operation: Operation::AddSegment(segments[0])
                },
                JournalEntry {
                    txid: 2,
                    operation: Operation::AddSegment(segments[1])
                },
                JournalEntry {
                    txid: 3,
                    operation: Operation::AddSegment(segments[2])
                },
                JournalEntry {
                    txid: 4,
                    operation: Operation::DeleteSegment(segments[0])
                },
                JournalEntry {
                    txid: 5,
                    operation: Operation::DeleteSegment(segments[1])
                },
                JournalEntry {
                    txid: 6,
                    operation: Operation::AddSegment(segments[3])
                },
            ]
        );

        reader_1.refresh()?;

        // Writer(pos 6), Reader1(pos 6), Reader2(pos 5)
        assert_eq!(writer.oldest_txid_in_use()?.unwrap(), 5);
        writer.compact()?;
        // Remove pre-merge transactions
        assert_eq!(
            writer.state.journal,
            [
                JournalEntry {
                    txid: 3,
                    operation: Operation::AddSegment(segments[2])
                },
                JournalEntry {
                    txid: 6,
                    operation: Operation::AddSegment(segments[3])
                },
            ]
        );

        writer.commit(Transaction {
            operations: vec![Operation::AddSegment(segments[0])],
            ..Default::default()
        })?;
        writer.commit(Transaction {
            operations: vec![Operation::AddSegment(segments[1])],
            ..Default::default()
        })?;

        // Writer(pos 8), Reader1(pos 6), Reader2(pos 5)
        assert_eq!(writer.oldest_txid_in_use()?.unwrap(), 5);

        reader_2.refresh()?;
        // Writer(pos 8), Reader1(pos 6), Reader2(pos 8)
        assert_eq!(writer.oldest_txid_in_use()?.unwrap(), 6);

        reader_1.refresh()?;
        // Writer(pos 8), Reader1(pos 8), Reader2(pos 8)
        assert_eq!(writer.oldest_txid_in_use()?.unwrap(), 8);

        Ok(())
    }
}
