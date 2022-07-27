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
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};

use memmap2::Mmap;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::database::{DBErr, VectorDB};
use crate::disk_structure::*;
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
    address: PathBuf,
    segments: HashMap<usize, Segment>,
    database: VectorDB,
    txn_log: TransactionLog,
    hnsw: Hnsw,
}

impl Index {
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

    fn hnsw_update(&self, hnsw: &mut Hnsw, add: &[Address], rmv: &[Address]) -> IdxResult<()> {
        use crate::hnsw::ops::HnswOps;
        let ro = self.database.ro_txn()?;
        let ops = HnswOps {
            txn: &ro,
            vector_db: &self.database,
            tracker: &DataRetriever {
                temp: &[],
                segments: &self.segments,
            },
        };
        add.iter().copied().for_each(|x| ops.insert(x, hnsw));
        rmv.iter().copied().for_each(|x| ops.delete(x, hnsw));
        Ok(())
    }

    fn txn_log_store(&self, txn_log: &mut TransactionLog, id: usize) -> IdxResult<()> {
        txn_log.entries.push(id);
        Ok(())
    }

    fn record_txn(&mut self, bch: Batch) -> IdxResult<()> {
        // Taking txn_log and hnsw from the state to be updated
        let mut txn_log = std::mem::take(&mut self.txn_log);
        let mut hnsw = std::mem::take(&mut self.hnsw);

        // bch is stored in a new txn on disk
        let txn_id = txn_log.fresh;
        txn_log.fresh += 1;
        let disk = DiskStructure::new(&self.address)?;
        let (wsegment, wdlog) = disk.create_txn(txn_id)?;
        let additions = self.store_vectors(wsegment, &bch.add_vectors)?;
        let deletions = self.store_delete_log(wdlog, &bch.rmv_keys)?;

        // The hnsw and the txn_log are updated
        self.hnsw_update(&mut hnsw, &additions, &deletions)?;
        self.txn_log_store(&mut txn_log, txn_id)?;

        // Database and disk state update
        let state = State { txn_log, hnsw };
        let mut rw = self.database.rw_txn()?;
        let wstate = disk.into_wstate();
        bch.add_keys
            .into_iter()
            .zip(additions.into_iter())
            .zip(bch.add_labels.into_iter())
            .map(|((i, j), k)| self.database.add_address(&mut rw, &i, j, &k))
            .collect::<Result<Vec<_>, _>>()?;
        wstate.write_state(&state)?;
        rw.commit()
            .map_err(IdxError::from)
            .and_then(|_| wstate.flush().map_err(IdxError::from))?;

        // Update the state in the index
        self.hnsw = state.hnsw;
        self.txn_log = state.txn_log;
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
                let disk = DiskStructure::new(&self.address).unwrap();
                let state = disk.get_state().unwrap();
                // going back to a safe state
                self.txn_log = state.txn_log;
                self.hnsw = state.hnsw;
                err
            }
            v => v,
        }
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
        let encoded = encode_vector(x);
        let temp_address = Address {
            txn_id: self.txn_log.fresh,
            slice: SegmentSlice { start: 0, end: 0 },
        };
        let ops = HnswOps {
            txn: &ro,
            vector_db: &self.database,
            tracker: &DataRetriever {
                temp: &encoded,
                segments: &self.segments,
            },
        };
        ops.search(temp_address, &self.hnsw, k_neighbours, with_filter)
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
        let disk = DiskStructure::new(path.as_ref())?;
        let state = disk.get_state()?;
        let database = disk.get_db()?;
        let segments = state
            .txn_log
            .entries
            .iter()
            .copied()
            .map(|id| disk.get_segment(id).map(|s| (id, s)))
            .collect::<Result<HashMap<_, _>, _>>()?;
        std::mem::drop(disk);
        Ok(Index {
            address: path.as_ref().to_path_buf(),
            hnsw: state.hnsw,
            txn_log: state.txn_log,
            database,
            segments,
        })
    }
}
