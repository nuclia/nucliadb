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

use std::{
    collections::{HashMap, HashSet},
    ffi::OsStr,
    fs::{self, File, OpenOptions},
    io::{BufReader, Seek, Write},
    path::{Path, PathBuf},
    time::SystemTime,
};

use crate::{
    data_point::Journal,
    data_types::{dtrie_ram::DTrie, DeleteLog},
    VectorR,
};

use super::data_point::DpId;
use fs2::FileExt;
use nucliadb_core::fs_state::{self, FsResult};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy)]
struct TimeSensitiveDLog<'a> {
    dlog: &'a DTrie,
    time: u64,
}
impl<'a> DeleteLog for TimeSensitiveDLog<'a> {
    fn is_deleted(&self, key: &[u8]) -> bool {
        self.dlog
            .get(key)
            .map(|t| t > self.time)
            .unwrap_or_default()
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct JournalTransaction {
    entry_id: u64,
    operations: Vec<Operation>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
enum Operation {
    AddSegment(DpId),
    DeleteSegment(DpId),
}

#[derive(Serialize, Deserialize, Default)]
struct State {
    journal: Vec<JournalTransaction>,
    delete_log: DTrie,
    no_nodes: usize,
    oldest_txid_pruned: u64,
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

    pub fn replace_segments(&mut self, old: Vec<DpId>, new: DpId) {
        for dpid in old {
            self.operations.push(Operation::DeleteSegment(dpid));
        }
        self.operations.push(Operation::AddSegment(new));
    }

    pub fn delete_entry(&mut self, prefix: String) {
        self.deleted_entries.push(prefix);
    }
}

pub struct SegmentManager {
    state: State,
    state_version: SystemTime,

    segments: HashMap<DpId, u64>,
    path: PathBuf,
    state_file: File, // TODO: Lock file that auto-destroys on clean exit
}

impl SegmentManager {
    fn txid(&self) -> u64 {
        self.state.journal.last().map_or(0, |e| e.entry_id)
    }

    pub fn commit(&mut self, transaction: Transaction) -> VectorR<()> {
        let next_txid = self.txid() + 1;
        for prefix in &transaction.deleted_entries {
            self.state.delete_log.insert(prefix.as_bytes(), next_txid)
        }

        if !transaction.operations.is_empty() {
            // TODO Only modify segments/deleted_entries if save succeeds?
            for op in &transaction.operations {
                match op {
                    Operation::AddSegment(dpid) => self.segments.insert(*dpid, next_txid),
                    Operation::DeleteSegment(dpid) => self.segments.remove(dpid),
                };
            }
            self.state.no_nodes += transaction.no_nodes;
            self.state.journal.push(JournalTransaction {
                entry_id: next_txid,
                operations: transaction.operations,
            });

            let oldest_segment = self.segments.values().min().unwrap_or(&0);
            self.state.delete_log.prune(*oldest_segment);
        }
        self.save()?;

        self.write_state();

        Ok(())
    }

    pub fn no_nodes(&self) -> usize {
        self.state.no_nodes
    }

    pub fn compact(&mut self) -> VectorR<()> {
        let oldest_txid_in_use = self.oldest_txid_in_use().unwrap_or(self.txid());
        let oldest_safe_txid = oldest_txid_in_use; //std::cmp::min(oldest_txid_in_use, self.state.oldest_txid_pruned);

        let mut segments = HashSet::new();
        let mut count = 0;
        for entry in &self.state.journal {
            if entry.entry_id > oldest_safe_txid {
                break;
            }
            count += 1;
            for op in &entry.operations {
                match op {
                    Operation::AddSegment(segment) => segments.insert(*segment),
                    Operation::DeleteSegment(segment) => segments.remove(segment),
                };
            }
        }
        if count == 0 {
            return Ok(());
        }
        self.state.journal.splice(
            0..count,
            [JournalTransaction {
                entry_id: oldest_safe_txid,
                operations: segments
                    .iter()
                    .map(|segment| Operation::AddSegment(*segment))
                    .collect(),
            }],
        );
        for s in &segments {
            self.segments.insert(*s, oldest_safe_txid);
        }

        self.save()?;

        Ok(())
    }

    fn oldest_txid_in_use(&self) -> Option<u64> {
        let mut oldest = None;
        for dir_entry in std::fs::read_dir(&self.path).unwrap() {
            let dir_entry = dir_entry.unwrap();
            let path = dir_entry.path();
            if !path.is_file() || path.extension() != Some(OsStr::new("segstate")) {
                continue;
            }
            // Try taking a lock to see if the process is still alive
            let is_locked = fs_state::try_exclusive_lock(&path).is_err();
            if !is_locked {
                std::fs::remove_file(path).unwrap();
                continue;
            }
            let reader_version: u64 = bincode::deserialize_from(File::open(path).unwrap()).unwrap();
            oldest = match oldest {
                None => Some(reader_version),
                Some(oldest) => Some(std::cmp::min(oldest, reader_version)),
            };
        }
        oldest
    }

    pub fn save(&self) -> FsResult<()> {
        fs_state::persist_state(&self.path, &self.state)
    }

    fn write_state(&mut self) {
        let txid = self.txid();
        self.state_file.seek(std::io::SeekFrom::Start(0)).unwrap();
        bincode::serialize_into(&mut self.state_file, &txid).unwrap();
        self.state_file.flush().unwrap();
    }

    pub fn open(path: PathBuf) -> VectorR<Self> {
        // Take a lock (to mark this process as alive)
        let state_file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(path.join(format!("{}.segstate", DpId::new_v4())))
            .unwrap();
        state_file.lock_exclusive().unwrap();

        let state_version = fs::metadata(path.join("state.bincode"))
            .unwrap()
            .modified()
            .unwrap();
        let state: State = fs_state::load_state(&path)?;
        let mut segments = HashMap::new();
        for (time, op) in state
            .journal
            .iter()
            .flat_map(|e| e.operations.iter().map(|op| (e.entry_id, op)))
        {
            match op {
                Operation::AddSegment(dpid) => segments.insert(*dpid, time),
                Operation::DeleteSegment(dpid) => segments.remove(dpid),
            };
        }

        let mut sm = SegmentManager {
            state,
            path,
            state_file,
            state_version,
            segments,
        };
        sm.write_state();
        Ok(sm)
    }

    pub fn create(path: PathBuf) -> VectorR<Self> {
        // Take a lock (to mark this process as alive)
        let state_file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(path.join(format!("{}.segstate", DpId::new_v4())))
            .unwrap();
        state_file.lock_exclusive().unwrap();

        let mut sm = SegmentManager {
            state: Default::default(),
            path,
            state_file,
            state_version: SystemTime::now(),
            segments: HashMap::new(),
        };
        sm.save()?;
        sm.write_state();
        Ok(sm)
    }

    pub fn refresh(&mut self) -> VectorR<()> {
        let txid = self.txid();
        self.state = fs_state::load_state(&self.path)?;
        for transaction in &self.state.journal {
            if transaction.entry_id <= txid {
                continue;
            }
            // TODO reuse this code
            for op in &transaction.operations {
                match op {
                    Operation::AddSegment(dpid) => {
                        self.segments.insert(*dpid, transaction.entry_id)
                    }
                    Operation::DeleteSegment(dpid) => self.segments.remove(dpid),
                };
            }
        }

        self.write_state();
        Ok(())
    }

    pub fn needs_refresh(&self) -> bool {
        let meta = fs::metadata(self.path.join("state.bincode"));
        if let Ok(meta) = meta {
            meta.modified().unwrap() > self.state_version
        } else {
            false
        }
    }

    // Returns active segments
    pub fn segment_iterator(&self) -> impl Iterator<Item = (&DpId, impl DeleteLog + '_)> {
        self.segments.iter().map(|(id, time)| {
            (
                id,
                TimeSensitiveDLog {
                    time: *time,
                    dlog: &self.state.delete_log,
                },
            )
        })
    }

    // Returns all segments in the log, including deleted ones
    pub fn all_segments_iterator(&self) -> impl Iterator<Item = &DpId> {
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

// TODO tests
// #[cfg(test)]
// mod tests {
//     use std::collections::HashSet;

//     use tempfile::TempDir;

//     use super::super::data_point::DpId;

//     use super::{SegmentJournal, SegmentJournalEntry, SegmentJournalOperation, SegmentManager};

//     #[test]
//     fn test_compact() {
//         let dir = TempDir::new().unwrap();
//         let mut manager = SegmentManager::create(dir.path().to_path_buf());
//         let segments = [
//             DpId::new_v4(),
//             DpId::new_v4(),
//             DpId::new_v4(),
//             DpId::new_v4(),
//         ];
//         manager.add_segment(segments[0]);
//         manager.commit();
//         manager.add_segment(segments[1]);
//         manager.add_segment(segments[2]);
//         manager.commit();
//         manager.remove_segment(segments[0]);
//         manager.remove_segment(segments[2]);
//         manager.commit();
//         manager.add_segment(segments[0]);
//         manager.commit();
//         manager.add_segment(segments[3]);
//         manager.commit();
//         assert_eq!(
//             manager.segments,
//             HashSet::from([segments[0], segments[1], segments[3]])
//         );

//         manager.compact(4);

//         assert_eq!(
//             manager.segments,
//             HashSet::from([segments[0], segments[1], segments[3]])
//         );
//         // TODO: Ordering
//         // assert_eq!(
//         //     manager.journal.entries,
//         //     vec![
//         //         SegmentJournalEntry {
//         //             entry_id: 4,
//         //             operations: vec![
//         //                 SegmentJournalOperation::AddSegment(segments[0]),
//         //                 SegmentJournalOperation::AddSegment(segments[1])
//         //             ],
//         //         },
//         //         SegmentJournalEntry {
//         //             entry_id: 5,
//         //             operations: vec![SegmentJournalOperation::AddSegment(segments[3])],
//         //         },
//         //     ],
//         // )
//     }

//     #[test]
//     fn test_reader_state() {
//         let dir = TempDir::new().unwrap();
//         let mut writer = SegmentManager::create(dir.path().to_path_buf());
//         let segments = [
//             DpId::new_v4(),
//             DpId::new_v4(),
//             DpId::new_v4(),
//             DpId::new_v4(),
//         ];
//         writer.add_segment(segments[0]);
//         writer.commit();

//         let mut reader_1 = SegmentManager::open(dir.path().to_path_buf());

//         writer.add_segment(segments[1]);
//         writer.add_segment(segments[2]);
//         writer.commit();

//         let mut reader_2 = SegmentManager::open(dir.path().to_path_buf());

//         writer.remove_segment(segments[0]);
//         writer.remove_segment(segments[2]);
//         writer.commit();

//         // Writer(pos 3), Reader1(pos 1), Reader2(pos 2)
//         assert_eq!(writer.journal.entries.first().unwrap().entry_id, 1);
//         assert_eq!(writer.oldest_entry_in_use().unwrap(), 1);
//         writer.compact(1);
//         assert_eq!(writer.journal.entries.first().unwrap().entry_id, 1);

//         reader_1.refresh();

//         // Writer(pos 3), Reader1(pos 3), Reader2(pos 2)
//         assert_eq!(writer.journal.entries.first().unwrap().entry_id, 1);
//         assert_eq!(writer.oldest_entry_in_use().unwrap(), 2);
//         writer.compact(2);
//         assert_eq!(writer.journal.entries.first().unwrap().entry_id, 2);

//         writer.add_segment(segments[0]);
//         writer.commit();
//         writer.add_segment(segments[3]);
//         writer.commit();

//         // Writer(pos 5), Reader1(pos 3), Reader2(pos 2)
//         assert_eq!(writer.oldest_entry_in_use().unwrap(), 2);

//         reader_2.refresh();
//         // Writer(pos 5), Reader1(pos 3), Reader2(pos 5)
//         assert_eq!(writer.oldest_entry_in_use().unwrap(), 3);

//         reader_1.refresh();
//         // Writer(pos 5), Reader1(pos 5), Reader2(pos 5)
//         assert_eq!(writer.oldest_entry_in_use().unwrap(), 5);
//     }
// }
