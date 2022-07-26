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

mod garbage_collector;
use crate::database::{DBErr, VectorDB};
use crate::disk_structure::{DiskError, DiskStructure, TxnFiles};
use crate::hnsw::Hnsw;
use memmap2::Mmap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SegmentSlice {
    pub start: usize,
    pub end: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Hash, Serialize, Deserialize)]
pub struct Location {
    pub txn_id: usize,
    pub slice: SegmentSlice,
}

#[derive(Default, Serialize, Deserialize)]
pub struct DeleteLog {
    pub log: Vec<Location>,
}

#[derive(Default, Deserialize, Serialize)]
pub struct TransactionLog {
    pub fresh: usize,
    pub entries: Vec<(usize, bool)>,
}

#[derive(Default, Deserialize, Serialize)]
pub struct State {
    transaction_log: TransactionLog,
    hnsw: Hnsw,
}

pub struct DataRetriever {
    temp: Vec<u8>,
    segments: HashMap<usize, Segment>,
}
impl DataRetriever {
    pub fn find(&self, x: Location) -> &[u8] {
        self.segments
            .get(&x.txn_id)
            .and_then(|segment| segment.get_vector(x.slice))
            .unwrap_or(&self.temp)
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
pub enum IndexError {
    #[error("Error in disk: {0}")]
    Disk(#[from] DiskError),
    #[error("Error in database {0}")]
    DB(#[from] DBErr),
    #[error("Error during IO {0}")]
    IO(#[from] std::io::Error),
}
type IndexResult<T> = Result<T, IndexError>;

pub struct Index {
    address: PathBuf,
    tracker: DataRetriever,
    database: VectorDB,
    txn_log: TransactionLog,
    hnsw: Hnsw,
}

// impl Index {
//     fn store_vectors(
//         &self,
//         mut buff: BufWriter<File>,
//         txn_id: usize,
//         vectors: &[Vec<f32>],
//     ) -> IndexResult<Vec<Location>> {
//         use crate::vector::encode_vector;
//         use std::io::Write;
//         let mut start = 0;
//         let mut locations = Vec::with_capacity(vectors.len());
//         for vector in vectors {
//             let encoded = encode_vector(vector);
//             let end = start + encoded.len();
//             let location = Location {
//                 txn_id,
//                 slice: SegmentSlice { start, end },
//             };
//             buff.write_all(&encoded)?;
//             locations.push(location);
//             start = end;
//         }
//         buff.flush()?;
//         Ok(locations)
//     }

//     fn store_delete_log(&self, _: BufWriter<File>, _: &[String]) {
//         todo!()
//     }

//     pub fn record_txn(&mut self, txn: Batch) -> IndexResult<()> {
//         let txn_id = self.txn_log.fresh;
//         self.txn_log.fresh += 1;
//         let disk = DiskStructure::new(&self.address)?;
//         let TxnFiles {
//             mut segment,
//             mut delete_log,
//         } = disk.create_txn(txn_id)?;
//         let locations = self.store_vectors(segment, txn_id, &txn.add_vectors)?;
//         // let mut rw_db = self.database.rw_txn()?;
//         // // txn
//         // //     .add_keys
//         // //     .into_iter()
//         // //     .zip(txn.add_vectors.into_iter())
//         // //     .zip(txn.add_labels.into_iter());
//         // rw_db.commit()?;
//         self.txn_log.entries.push((txn_id, true));
//         Ok(())
//     }
// }
