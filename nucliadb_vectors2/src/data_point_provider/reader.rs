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

use crate::data_point::{self, DataPointPin, OpenDataPoint};
pub use crate::data_point::{DpId, Neighbour};
use crate::data_point_provider::state::read_state;
use crate::data_point_provider::{IndexMetadata, SearchRequest, OPENING_FLAG, STATE};
use crate::data_types::dtrie_ram::DTrie;
use crate::data_types::DeleteLog;
use crate::{VectorErr, VectorR};
use fs2::FileExt;
use fxhash::{FxHashMap, FxHashSet};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

#[derive(Clone, Copy)]
struct TimeSensitiveDLog<'a> {
    dlog: &'a DTrie,
    time: SystemTime,
}
impl<'a> DeleteLog for TimeSensitiveDLog<'a> {
    fn is_deleted(&self, key: &[u8]) -> bool {
        self.dlog.get(key).map(|t| t > self.time).unwrap_or_default()
    }
}

// Fixed-sized sorted collection
struct Fssc {
    size: usize,
    with_duplicates: bool,
    seen: HashSet<Vec<u8>>,
    buff: HashMap<Neighbour, f32>,
}
impl From<Fssc> for Vec<Neighbour> {
    fn from(fssv: Fssc) -> Self {
        let mut result: Vec<_> = fssv.buff.into_keys().collect();
        result.sort_by(|a, b| b.score().partial_cmp(&a.score()).unwrap_or(Ordering::Less));
        result
    }
}
impl Fssc {
    fn is_full(&self) -> bool {
        self.buff.len() == self.size
    }
    fn new(size: usize, with_duplicates: bool) -> Fssc {
        Fssc {
            size,
            with_duplicates,
            seen: HashSet::new(),
            buff: HashMap::with_capacity(size),
        }
    }
    fn add(&mut self, candidate: Neighbour) {
        if !self.with_duplicates && self.seen.contains(candidate.vector()) {
            return;
        } else if !self.with_duplicates {
            let vector = candidate.vector().to_vec();
            self.seen.insert(vector);
        }

        let score = candidate.score();
        if self.is_full() {
            let smallest_bigger = self
                .buff
                .iter()
                .map(|(key, score)| (key.clone(), *score))
                .filter(|(_, v)| score > *v)
                .min_by(|(_, v0), (_, v1)| v0.partial_cmp(v1).unwrap())
                .map(|(key, _)| key);
            if let Some(key) = smallest_bigger {
                self.buff.remove_entry(&key);
                self.buff.insert(candidate, score);
            }
        } else {
            self.buff.insert(candidate, score);
        }
    }
}

fn last_modified(path: &Path) -> io::Result<SystemTime> {
    let meta = std::fs::metadata(path)?;
    meta.modified()
}

pub struct Reader {
    metadata: IndexMetadata,
    path: PathBuf,
    open_data_points: FxHashMap<DpId, OpenDataPoint>,
    data_point_pins: Vec<DataPointPin>,
    delete_log: DTrie,
    number_of_embeddings: usize,
    version: SystemTime,
    dimension: Option<u64>,
}

impl Reader {
    pub fn open(path: &Path) -> VectorR<Reader> {
        let lock_path = path.join(OPENING_FLAG);
        let lock_file = File::create(lock_path)?;
        lock_file.lock_shared()?;

        let metadata = IndexMetadata::open(path)?.map(Ok).unwrap_or_else(|| {
            // Old indexes may not have this file so in that case the
            // metadata file they should have is created.
            let metadata = IndexMetadata::default();
            metadata.write(path).map(|_| metadata)
        })?;

        let state_path = path.join(STATE);
        let state_file = File::open(&state_path)?;
        let version = last_modified(&state_path)?;
        let state = read_state(&state_file)?;
        let data_point_list = state.data_point_list;
        let delete_log = state.delete_log;
        let mut dimension = None;
        let mut data_point_pins = Vec::new();
        let mut open_data_points = FxHashMap::default();
        let mut number_of_embeddings = 0;

        for data_point_id in data_point_list {
            let data_point_pin = DataPointPin::open_pin(path, data_point_id)?;
            let open_data_point = data_point::open(&data_point_pin)?;
            let data_point_journal = open_data_point.journal();

            number_of_embeddings += data_point_journal.no_nodes();
            data_point_pins.push(data_point_pin);
            open_data_points.insert(data_point_id, open_data_point);
        }

        if let Some(data_point_pin) = data_point_pins.first() {
            let open_data_point = &open_data_points[&data_point_pin.id()];
            dimension = open_data_point.stored_len();
        }

        Ok(Reader {
            metadata,
            version,
            data_point_pins,
            open_data_points,
            delete_log,
            number_of_embeddings,
            dimension,
            path: path.to_path_buf(),
        })
    }

    pub fn update(&mut self) -> VectorR<()> {
        let state_path = self.path.join(STATE);
        let disk_version = last_modified(&state_path)?;

        if disk_version == self.version {
            return Ok(());
        }

        let state_file = File::open(state_path)?;
        let state = read_state(&state_file)?;
        let data_point_list = state.data_point_list;
        let new_delete_log = state.delete_log;
        let mut new_dimension = self.dimension;
        let mut new_number_of_embeddings = 0;
        let mut new_data_point_pins = Vec::new();
        let mut new_open_data_points = Vec::new();
        let mut data_points_to_eject: FxHashSet<_> = self.open_data_points.keys().copied().collect();

        for data_point_id in data_point_list {
            let data_point_pin = DataPointPin::open_pin(&self.path, data_point_id)?;

            if let Some(open_data_point) = self.open_data_points.get(&data_point_id) {
                let data_point_journal = open_data_point.journal();
                new_number_of_embeddings += data_point_journal.no_nodes();
                data_points_to_eject.remove(&data_point_id);
            } else {
                let open_data_point = data_point::open(&data_point_pin)?;
                let data_point_journal = open_data_point.journal();
                new_number_of_embeddings += data_point_journal.no_nodes();
                new_open_data_points.push(open_data_point);
            }

            new_data_point_pins.push(data_point_pin);
        }

        for open_data_point in new_open_data_points {
            let data_point_id = open_data_point.get_id();
            self.open_data_points.insert(data_point_id, open_data_point);
        }

        for data_point_id in data_points_to_eject {
            self.open_data_points.remove(&data_point_id);
        }

        if new_dimension.is_none() {
            if let Some(data_point_pin) = self.data_point_pins.first() {
                let open_data_point = &self.open_data_points[&data_point_pin.id()];
                new_dimension = open_data_point.stored_len();
            }
        }

        self.version = disk_version;
        self.delete_log = new_delete_log;
        self.data_point_pins = new_data_point_pins;
        self.dimension = new_dimension;
        self.number_of_embeddings = new_number_of_embeddings;

        Ok(())
    }

    pub fn search(&self, request: &dyn SearchRequest) -> VectorR<Vec<Neighbour>> {
        let Some(dimension) = self.dimension else {
            return Ok(Vec::with_capacity(0));
        };
        if dimension != request.get_query().len() as u64 {
            return Err(VectorErr::InconsistentDimensions);
        }

        let similarity = self.metadata.similarity;
        let query = request.get_query();
        let filter = request.get_filter();
        let with_duplicates = request.with_duplicates();
        let no_results = request.no_results();
        let min_score = request.min_score();
        let mut ffsv = Fssc::new(request.no_results(), with_duplicates);

        for open_data_point in self.open_data_points.values() {
            let data_point_journal = open_data_point.journal();
            let delete_log = TimeSensitiveDLog {
                time: data_point_journal.time(),
                dlog: &self.delete_log,
            };
            // Skipping the formatter only because the search interface is quite bad right now.
            #[rustfmt::skip] let partial_solution = open_data_point.search(
                &delete_log,
                query,
                filter,
                with_duplicates,
                no_results,
                similarity,
                min_score
            );
            for candidate in partial_solution {
                ffsv.add(candidate);
            }
        }

        Ok(ffsv.into())
    }

    pub fn keys(&self) -> VectorR<Vec<String>> {
        let mut keys = vec![];
        for open_data_point in self.open_data_points.values() {
            let data_point_journal = open_data_point.journal();
            let delete_log = TimeSensitiveDLog {
                time: data_point_journal.time(),
                dlog: &self.delete_log,
            };
            let mut results = open_data_point.get_keys(&delete_log);
            keys.append(&mut results);
        }
        Ok(keys)
    }

    pub fn size(&self) -> usize {
        self.number_of_embeddings
    }

    pub fn location(&self) -> &Path {
        &self.path
    }

    pub fn metadata(&self) -> &IndexMetadata {
        &self.metadata
    }

    pub fn embedding_dimension(&self) -> Option<u64> {
        self.dimension
    }
}
