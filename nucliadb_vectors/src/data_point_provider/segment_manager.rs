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

use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{Seek, Write};
use std::path::{Path, PathBuf};

use fs2::FileExt;
use nucliadb_core::fs_state::{self, FsResult, Version};
use serde::{Deserialize, Serialize};

use crate::data_point::{DpId, Journal};
use crate::data_types::dtrie_ram::DTrie;
use crate::data_types::DeleteLog;
use crate::VectorR;

type TxId = u64;
type SegmentId = DpId;

#[derive(Clone, Copy)]
struct TimeSensitiveDLog<'a> {
    dlog: &'a DTrie,
    time: TxId,
}
impl<'a> DeleteLog for TimeSensitiveDLog<'a> {
    fn is_deleted(&self, key: &[u8]) -> bool {
        self.dlog
            .get(key)
            .map(|t| t > self.time)
            .unwrap_or_default()
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
struct JournalTransaction {
    txid: TxId,
    operations: Vec<Operation>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
enum Operation {
    AddSegment(SegmentId),
    DeleteSegment(SegmentId),
}

#[derive(Serialize, Deserialize, Default, Clone)]
struct State {
    journal: Vec<JournalTransaction>,
    delete_log: DTrie,
    no_nodes: usize,
}

#[derive(Default)]
pub struct Transaction {
    operations: Vec<Operation>,
    deleted_entries: Vec<String>,
    no_nodes: usize,
}

impl Transaction {
    pub fn add_segment(&mut self, dp_journal: Journal) {
        self.operations.push(Operation::AddSegment(dp_journal.id()));
        self.no_nodes += dp_journal.no_nodes();
    }

    pub fn replace_segments(&mut self, old: Vec<SegmentId>, new: SegmentId) {
        for dpid in old {
            self.operations.push(Operation::DeleteSegment(dpid));
        }
        self.operations.push(Operation::AddSegment(new));
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
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(path.join(format!("{}.{STATE_FILE_EXTENSION}", SegmentId::new_v4())))?;
        file.lock_exclusive()?;

        Ok(StateFile { path, file })
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
        self.state.journal.last().map_or(0, |e| e.txid)
    }

    pub fn commit(&mut self, transaction: Transaction) -> VectorR<()> {
        let old_state = self.state.clone();

        let next_txid = self.txid() + 1;
        for prefix in &transaction.deleted_entries {
            self.state.delete_log.insert(prefix.as_bytes(), next_txid)
        }

        let has_operations = !transaction.operations.is_empty();
        if has_operations {
            self.state.no_nodes += transaction.no_nodes;
            self.state.journal.push(JournalTransaction {
                txid: next_txid,
                operations: transaction.operations,
            });

            // We can prune the delete_log at the point of the oldest segment still in use
            self.state.delete_log.prune(self.oldest_live_txid());
        }

        match self.save() {
            Ok(_) => {
                if has_operations {
                    // Apply the changes to the segment view
                    for op in &self.state.journal.last().unwrap().operations {
                        match op {
                            Operation::AddSegment(dpid) => self.segments.insert(*dpid, next_txid),
                            Operation::DeleteSegment(dpid) => self.segments.remove(dpid),
                        };
                    }
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
        // We can only compact transactions that:
        // - Are not in use by any other reader (oldest_txid_in_use)
        // - And do not reference any unmerged segment (might have delete_log entries)
        let oldest_txid_in_use = self.oldest_txid_in_use()?.unwrap_or(self.txid());
        let oldest_safe_txid = std::cmp::min(oldest_txid_in_use, self.oldest_live_txid());

        let mut segments = HashSet::new();
        let mut count = 0;
        for transaction in &self.state.journal {
            if transaction.txid > oldest_safe_txid {
                break;
            }
            count += 1;
            for op in &transaction.operations {
                match op {
                    Operation::AddSegment(segment) => segments.insert(*segment),
                    Operation::DeleteSegment(segment) => segments.remove(segment),
                };
            }
        }
        if count <= 1 {
            return Ok(());
        }

        let old_state = self.state.clone();
        self.state.journal.splice(
            0..count,
            [JournalTransaction {
                txid: oldest_safe_txid,
                operations: segments
                    .iter()
                    .map(|segment| Operation::AddSegment(*segment))
                    .collect(),
            }],
        );

        if let Err(e) = self.save() {
            // Rollback
            self.state = old_state;
            return Err(e.into());
        };

        // Update any segments that have moved from txid and are not deleted later
        for s in &segments {
            if self.segments.contains_key(s) {
                self.segments.insert(*s, oldest_safe_txid);
            }
        }

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
        for (time, op) in state
            .journal
            .iter()
            .flat_map(|e| e.operations.iter().map(|op| (e.txid, op)))
        {
            match op {
                Operation::AddSegment(dpid) => segments.insert(*dpid, time),
                Operation::DeleteSegment(dpid) => segments.remove(dpid),
            };
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

    pub fn refresh(&mut self) -> VectorR<()> {
        let txid = self.txid();
        self.state = fs_state::load_state(&self.path)?;

        for transaction in &self.state.journal {
            if transaction.txid <= txid {
                continue;
            }
            for op in &transaction.operations {
                match op {
                    Operation::AddSegment(dpid) => self.segments.insert(*dpid, transaction.txid),
                    Operation::DeleteSegment(dpid) => self.segments.remove(dpid),
                };
            }
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
                    time: *time,
                    dlog: &self.state.delete_log,
                },
                *id,
            )
        })
    }

    // Returns all segments in the log, including deleted ones
    pub fn all_segments_iterator(&self) -> impl Iterator<Item = &SegmentId> {
        let all_operations = self.state.journal.iter().flat_map(|e| &e.operations);
        all_operations.filter_map(|op| {
            if let Operation::AddSegment(id) = op {
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
    use crate::data_point_provider::segment_manager::{JournalTransaction, Operation, Transaction};
    use crate::VectorR;

    #[test]
    fn test_compact() -> VectorR<()> {
        let dir = TempDir::new().unwrap();
        let mut manager = SegmentManager::create(dir.path().to_path_buf()).unwrap();
        let segments = [
            SegmentId::new_v4(),
            SegmentId::new_v4(),
            SegmentId::new_v4(),
            SegmentId::new_v4(),
            SegmentId::new_v4(),
        ];
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
        // Merge (txid=4)
        manager.commit(Transaction {
            operations: vec![
                Operation::DeleteSegment(segments[0]),
                Operation::DeleteSegment(segments[2]),
                Operation::AddSegment(segments[3]),
            ],
            ..Default::default()
        })?;
        // Insert (txid=5)
        manager.commit(Transaction {
            operations: vec![Operation::AddSegment(segments[4])],
            ..Default::default()
        })?;

        let mut expected = HashMap::new();
        expected.insert(segments[1], 2);
        expected.insert(segments[3], 4);
        expected.insert(segments[4], 5);
        assert_eq!(manager.segments, expected);

        manager.compact()?;
        // Does not change the current state
        assert_eq!(manager.segments, expected);

        // But changes the first entries of the journal
        let compact_tx = &manager.state.journal[0];
        assert_eq!(compact_tx.txid, 2);
        assert_eq!(compact_tx.operations.len(), 2);
        assert!(compact_tx
            .operations
            .contains(&Operation::AddSegment(segments[0])));
        assert!(compact_tx
            .operations
            .contains(&Operation::AddSegment(segments[1])));
        assert_eq!(
            manager.state.journal[1],
            JournalTransaction {
                txid: 3,
                operations: vec![Operation::AddSegment(segments[2])],
            },
        );

        Ok(())
    }

    #[test]
    fn test_reader_state() -> VectorR<()> {
        let dir = TempDir::new()?;
        let mut writer = SegmentManager::create(dir.path().to_path_buf())?;
        let segments = [
            SegmentId::new_v4(),
            SegmentId::new_v4(),
            SegmentId::new_v4(),
            SegmentId::new_v4(),
        ];
        writer.commit(Transaction {
            operations: vec![Operation::AddSegment(segments[0])],
            ..Default::default()
        })?;
        let mut reader_1 = SegmentManager::open(dir.path().to_path_buf())?;

        writer.commit(Transaction {
            operations: vec![
                Operation::AddSegment(segments[1]),
                Operation::AddSegment(segments[2]),
            ],
            ..Default::default()
        })?;

        let mut reader_2 = SegmentManager::open(dir.path().to_path_buf())?;

        writer.commit(Transaction {
            operations: vec![
                Operation::DeleteSegment(segments[0]),
                Operation::DeleteSegment(segments[2]),
            ],
            ..Default::default()
        })?;

        // Writer(pos 3), Reader1(pos 1), Reader2(pos 2)
        assert_eq!(writer.state.journal.first().unwrap().txid, 1);
        assert_eq!(writer.oldest_txid_in_use()?.unwrap(), 1);
        writer.compact()?;
        assert_eq!(writer.state.journal.first().unwrap().txid, 1);

        reader_1.refresh()?;

        // Writer(pos 3), Reader1(pos 3), Reader2(pos 2)
        assert_eq!(writer.state.journal.first().unwrap().txid, 1);
        assert_eq!(writer.oldest_txid_in_use()?.unwrap(), 2);
        writer.compact()?;
        assert_eq!(writer.state.journal.first().unwrap().txid, 2);

        writer.commit(Transaction {
            operations: vec![Operation::AddSegment(segments[0])],
            ..Default::default()
        })?;
        writer.commit(Transaction {
            operations: vec![Operation::AddSegment(segments[3])],
            ..Default::default()
        })?;

        // Writer(pos 5), Reader1(pos 3), Reader2(pos 2)
        assert_eq!(writer.oldest_txid_in_use()?.unwrap(), 2);

        reader_2.refresh()?;
        // Writer(pos 5), Reader1(pos 3), Reader2(pos 5)
        assert_eq!(writer.oldest_txid_in_use()?.unwrap(), 3);

        reader_1.refresh()?;
        // Writer(pos 5), Reader1(pos 5), Reader2(pos 5)
        assert_eq!(writer.oldest_txid_in_use()?.unwrap(), 5);

        Ok(())
    }
}
