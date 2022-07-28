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

// mod garbage_collector;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};

use memmap2::Mmap;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::database::{DBErr, RoTxn, VectorDB};
use crate::disk_structure::{self, DiskError, Lock, WDeletelog, WSegment};
use crate::hnsw::Hnsw;

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct SegmentSlice {
    pub start: usize,
    pub end: usize,
}

#[derive(
    Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct Address {
    pub txn_id: usize,
    pub slice: SegmentSlice,
}

#[derive(Default, Serialize, Deserialize)]
pub struct DeleteLog {
    pub log: Vec<Address>,
}

#[derive(Default, Deserialize, Serialize)]
pub struct TransactionLog {
    pub fresh: usize,
    pub entries: Vec<usize>,
}

#[derive(Default, Deserialize, Serialize)]
pub struct State {
    txn_log: TransactionLog,
    hnsw: Hnsw,
}

pub struct DataRetriever<'a> {
    temp: &'a [u8],
    segments: &'a HashMap<usize, Segment>,
}
impl<'a> DataRetriever<'a> {
    pub fn find(&self, x: Address) -> &[u8] {
        self.segments
            .get(&x.txn_id)
            .and_then(|segment| segment.get_vector(x.slice))
            .unwrap_or(self.temp)
    }
}

pub struct Segment {
    mmaped: Mmap,
}
impl Segment {
    pub fn new<T: AsRef<Path>>(path: T) -> std::io::Result<Segment> {
        let file = OpenOptions::new().read(true).open(path)?;
        let mmaped = unsafe { Mmap::map(&file)? };
        Ok(Segment { mmaped })
    }
    pub fn get_vector(&self, slice: SegmentSlice) -> Option<&[u8]> {
        let range = (slice.start as usize)..(slice.end as usize);
        self.mmaped.get(range)
    }
}

#[derive(Default)]
pub struct Batch {
    add_keys: Vec<String>,
    add_vectors: Vec<Vec<f32>>,
    add_labels: Vec<Vec<String>>,
    rmv_keys: Vec<String>,
}
impl Batch {
    pub fn new() -> Batch {
        Batch::default()
    }
    pub fn add_vector(&mut self, key: String, vector: Vec<f32>, labels: Vec<String>) {
        self.add_keys.push(key);
        self.add_vectors.push(vector);
        self.add_labels.push(labels);
    }
    pub fn rmv_vector(&mut self, key: String) {
        self.rmv_keys.push(key);
    }
}

#[derive(Error, Debug)]
pub enum IdxError {
    #[error("Error in disk: {0}")]
    Disk(#[from] DiskError),
    #[error("Error in database {0}")]
    DB(#[from] DBErr),
}
type IdxResult<T> = Result<T, IdxError>;

pub struct Index {
    version: disk_structure::Version,
    address: PathBuf,
    database: VectorDB,
    segments: RefCell<HashMap<usize, Segment>>,
    txn_log: RefCell<TransactionLog>,
    hnsw: RefCell<Hnsw>,
}

impl Index {
    fn update(&self, lock: &Lock) -> IdxResult<()> {
        let state = State {
            txn_log: self.txn_log.take(),
            hnsw: self.hnsw.take(),
        };
        let (version, state) = disk_structure::update(lock, self.version, state)?;
        if version > self.version {
            self.segments
                .replace(Index::map_segments(lock, &state.txn_log)?);
        }
        self.txn_log.replace(state.txn_log);
        self.hnsw.replace(state.hnsw);
        Ok(())
    }
    fn store_vectors(
        &self,
        mut wsegment: WSegment,
        vectors: &[Vec<f32>],
    ) -> IdxResult<Vec<Address>> {
        use crate::vector::encode_vector;
        vectors
            .iter()
            .map(|v| encode_vector(v))
            .map(|buf| wsegment.write(&buf).map_err(|e| e.into()))
            .collect()
    }

    fn store_delete_log(
        &self,
        wdlog: WDeletelog,
        to_delelte: &[String],
    ) -> IdxResult<Vec<Address>> {
        let txn = self.database.ro_txn()?;
        let log = to_delelte
            .iter()
            .map_while(|v| self.database.get_address(&txn, v).transpose())
            .collect::<Result<Vec<_>, _>>()?;
        let dlog = DeleteLog { log };
        wdlog.write(&dlog)?;
        Ok(dlog.log)
    }

    fn hnsw_update(
        &self,
        txn: &RoTxn,
        hnsw: &mut Hnsw,
        add: &[Address],
        rmv: &[Address],
    ) -> IdxResult<()> {
        use crate::hnsw::ops::HnswOps;
        let ops = HnswOps {
            txn,
            vector_db: &self.database,
            tracker: &DataRetriever {
                temp: &[],
                segments: &self.segments.borrow(),
            },
        };
        add.iter().copied().for_each(|x| ops.insert(x, hnsw));
        rmv.iter().copied().for_each(|x| ops.delete(x, hnsw));
        Ok(())
    }

    fn segment_store(&mut self, id: usize, segment: Segment) -> IdxResult<()> {
        self.txn_log.get_mut().entries.push(id);
        self.segments.get_mut().insert(id, segment);
        Ok(())
    }

    fn record_txn(&mut self, bch: Batch) -> IdxResult<()> {
        // Exclusive lock scope starts
        let lock = disk_structure::exclusive_lock(&self.address)?;

        // bch is stored in a new txn on disk, recorded in memory
        let txn_id = self.txn_log.borrow().fresh;
        self.txn_log.get_mut().fresh += 1;
        let (wsegment, wdlog) = disk_structure::create_txn(&lock, txn_id)?;
        let additions = self.store_vectors(wsegment, &bch.add_vectors)?;
        let deletions = self.store_delete_log(wdlog, &bch.rmv_keys)?;
        let new_segment = disk_structure::read_segment(&lock, txn_id)?;
        self.segment_store(txn_id, new_segment)?;

        // Database and disk state update
        // Taking txn_log and hnsw from the state to be updated
        let mut rw = self.database.rw_txn()?;
        bch.add_keys
            .iter()
            .zip(additions.iter().copied())
            .zip(bch.add_labels.iter())
            .map(|((i, j), k)| self.database.add_address(&mut rw, i, j, k))
            .collect::<Result<Vec<_>, _>>()?;

        // The hnsw index is updated
        let mut hnsw = self.hnsw.take();
        self.hnsw_update(&rw, &mut hnsw, &additions, &deletions)?;
        let txn_log = self.txn_log.take();
        let state = State { txn_log, hnsw };
        disk_structure::write_state(&lock, &state)?;
        self.hnsw = state.hnsw.into();
        self.txn_log = state.txn_log.into();
        // Trying to end the transaction
        rw.commit()
            .map_err(IdxError::from)
            .and_then(|_| lock.commit().map_err(IdxError::from))?;
        // Exclusive lock scope ends
        Ok(())
    }
    pub fn write(&mut self, bch: Batch) -> IdxResult<()> {
        match self.record_txn(bch) {
            err @ Err(_) => {
                // Memory status may be corrupted,
                // Changes are forgotten in favor of disk.
                std::mem::take(&mut self.txn_log);
                std::mem::take(&mut self.hnsw);
                // Disk recovery may fail
                let lock = disk_structure::shared_lock(&self.address).unwrap();
                disk_structure::init_env(&lock).unwrap();
                let (version, state) = disk_structure::read_state(&lock).unwrap();
                // going back to a safe state
                self.segments = Index::map_segments(&lock, &state.txn_log).unwrap().into();
                self.version = version;
                self.txn_log = state.txn_log.into();
                self.hnsw = state.hnsw.into();
                err
            }
            v => v,
        }
    }
    fn map_segments(lock: &Lock, txn_log: &TransactionLog) -> IdxResult<HashMap<usize, Segment>> {
        txn_log
            .entries
            .iter()
            .copied()
            .map(|id| disk_structure::read_segment(lock, id).map(|s| (id, s)))
            .map(|v| v.map_err(IdxError::from))
            .collect::<IdxResult<HashMap<_, _>>>()
    }
    pub fn search(
        &self,
        k_neighbours: usize,
        x: &[f32],
        with_filter: &[String],
    ) -> IdxResult<Vec<(String, f32)>> {
        use crate::hnsw::ops::HnswOps;
        use crate::vector::encode_vector;
        let ro = self.database.ro_txn()?;
        let lock = disk_structure::shared_lock(&self.address)?;
        self.update(&lock)?;
        let encoded = encode_vector(x);
        let temp_address = Address {
            txn_id: self.txn_log.borrow().fresh,
            slice: SegmentSlice { start: 0, end: 0 },
        };
        let ops = HnswOps {
            txn: &ro,
            vector_db: &self.database,
            tracker: &DataRetriever {
                temp: &encoded,
                segments: &self.segments.borrow(),
            },
        };
        ops.search(temp_address, &self.hnsw.borrow(), k_neighbours, with_filter)
            .neighbours
            .into_iter()
            .map_while(|(addr, dist)| {
                self.database
                    .get_address_key(&ro, addr)
                    .map_err(IdxError::from)
                    .map(|key| key.map(|key| (key.to_string(), dist)))
                    .transpose()
            })
            .collect()
    }
    pub fn new<P: AsRef<Path>>(path: P) -> IdxResult<Index> {
        let lock = disk_structure::shared_lock(path.as_ref())?;
        disk_structure::init_env(&lock)?;
        let (version, state) = disk_structure::read_state(&lock)?;
        let database = disk_structure::open_db(&lock)?;
        let segments = Index::map_segments(&lock, &state.txn_log)?;
        std::mem::drop(lock);
        Ok(Index {
            version,
            database,
            address: path.as_ref().to_path_buf(),
            hnsw: RefCell::new(state.hnsw),
            txn_log: RefCell::new(state.txn_log),
            segments: RefCell::new(segments),
        })
    }
}
