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
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};

use memmap2::Mmap;
use serde::{Deserialize, Serialize};

use crate::database::VectorDB;
use crate::hnsw::Hnsw;

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Hash, Serialize, Deserialize)]
pub struct Location {
    txn_id: usize,
    slice: SegmentSlice,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SegmentSlice {
    pub start: u64,
    pub end: u64,
}

#[derive(Default, Serialize, Deserialize)]
pub struct DeleteLog {
    log: Vec<Location>,
}

#[derive(Default, Deserialize, Serialize)]
pub struct TransactionLog {
    entries: Vec<usize>,
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
    pub fn new(path: &Path) -> Segment {
        let file = OpenOptions::new().read(true).open(path).unwrap();
        Segment {
            mmaped: unsafe { Mmap::map(&file).unwrap() },
        }
    }
    pub fn get_vector(&self, slice: SegmentSlice) -> Option<&[u8]> {
        let range = (slice.start as usize)..(slice.end as usize);
        self.mmaped.get(range)
    }
}

pub struct Index {
    tracker: DataRetriever,
    database: VectorDB,
    hnsw: Hnsw,
}
