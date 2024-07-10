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
use crate::data_point_provider::VectorConfig;
use crate::data_point_provider::{IndexMetadata, SearchRequest, OPENING_FLAG, STATE};
use crate::data_types::dtrie_ram::DTrie;
use crate::data_types::DeleteLog;
use crate::utils;
use crate::{VectorErr, VectorR};
use fs2::FileExt;
use fxhash::FxHashMap;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fs::File;
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

pub struct Reader {
    config: VectorConfig,
    path: PathBuf,
    open_data_points: FxHashMap<DpId, OpenDataPoint>,
    delete_log: DTrie,
    number_of_embeddings: usize,
    dimension: Option<usize>,
    state_last_modified: SystemTime,
}

fn last_modified(path: &Path) -> std::io::Result<SystemTime> {
    std::fs::metadata(path)?.modified()
}

impl Reader {
    pub fn open(path: &Path) -> VectorR<Reader> {
        let lock_path = path.join(OPENING_FLAG);
        let lock_file = File::create(lock_path)?;
        lock_file.lock_shared()?;

        let config = IndexMetadata::open(path)?.map(Ok).unwrap_or_else(|| {
            // Old indexes may not have this file so in that case the
            // metadata file they should have is created.
            let metadata = VectorConfig::default();
            IndexMetadata::write(&metadata, path).map(|_| metadata)
        })?;

        let state_path = path.join(STATE);
        let state_last_modified = last_modified(&state_path)?;
        let state_file = File::open(state_path)?;
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
            config,
            open_data_points,
            delete_log,
            number_of_embeddings,
            dimension,
            path: path.to_path_buf(),
            state_last_modified,
        })
    }

    pub fn search(&self, request: &dyn SearchRequest) -> VectorR<Vec<Neighbour>> {
        let normalized_query;
        let query = if self.config.normalize_vectors {
            normalized_query = utils::normalize_vector(request.get_query());
            &normalized_query
        } else {
            request.get_query()
        };

        // Validate vector dimensions
        let valid_dims = match self.config.vector_type {
            crate::config::VectorType::DenseF32Unaligned => {
                let Some(dimension) = self.dimension else {
                    return Ok(Vec::with_capacity(0));
                };
                dimension == query.len()
            }
            crate::config::VectorType::DenseF32 {
                dimension,
            } => dimension == query.len(),
        };
        if !valid_dims {
            println!("Inconsistent dimension, stored {:?}, query {}", self.config, query.len());
            return Err(VectorErr::InconsistentDimensions);
        }

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
            let partial_solution = open_data_point.search(
                &delete_log,
                query,
                filter,
                with_duplicates,
                no_results,
                &self.config,
                min_score,
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

    pub fn config(&self) -> &VectorConfig {
        &self.config
    }

    pub fn embedding_dimension(&self) -> Option<usize> {
        self.dimension
    }

    pub fn needs_update(&self) -> VectorR<bool> {
        let state_path = self.path.join(STATE);
        let state_modified = last_modified(&state_path)?;
        Ok(self.state_last_modified < state_modified)
    }
}
