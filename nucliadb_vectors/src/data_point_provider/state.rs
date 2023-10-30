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

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, LinkedList};
use std::mem;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

use super::{SearchRequest, VectorR};
use crate::data_point::{DataPoint, DpId, Journal, Neighbour, Similarity};
use crate::data_types::dtrie_ram::DTrie;
use crate::data_types::DeleteLog;

const BUFFER_CAP: usize = 5;

#[derive(Serialize, Deserialize)]
struct WorkUnit {
    pub age: SystemTime,
    pub load: Vec<Journal>,
}
impl Default for WorkUnit {
    fn default() -> Self {
        WorkUnit::new()
    }
}
impl WorkUnit {
    pub fn new() -> WorkUnit {
        WorkUnit {
            age: SystemTime::now(),
            load: vec![],
        }
    }
    pub fn add_unit(&mut self, dp: Journal) {
        self.load.push(dp);
    }
    pub fn size(&self) -> usize {
        self.load.len()
    }
}

#[derive(Clone, Copy)]
struct TimeSensitiveDLog<'a> {
    dlog: &'a DTrie,
    time: SystemTime,
}
impl<'a> DeleteLog for TimeSensitiveDLog<'a> {
    fn is_deleted(&self, key: &[u8]) -> bool {
        self.dlog
            .get(key)
            .map(|t| t > self.time)
            .unwrap_or_default()
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

#[derive(Serialize, Deserialize)]
pub struct State {
    // Deprecated, location must be passed as an argument.
    // WARNING: Can not use serde::skip nor move this field due to a bug in serde.
    #[allow(unused)]
    #[deprecated]
    location: PathBuf,

    // Total number of nodes stored. Some
    // may be marked as deleted but are waiting
    // for a merge to be fully removed.
    no_nodes: usize,

    // Current work unit
    current: WorkUnit,

    // Trie containing the deleted keys and the
    // time when they were deleted
    delete_log: DTrie,

    // Already closed WorkUnits waiting to be merged
    work_stack: LinkedList<WorkUnit>,

    // This field is deprecated and is only
    // used for old states. Always use
    // the data_point journal for time references
    data_points: HashMap<DpId, SystemTime>,

    // Deprecated field, not all vector clusters are
    // identified by a resource.
    #[serde(skip)]
    #[allow(unused)]
    #[deprecated]
    resources: HashMap<String, usize>,
}
impl State {
    fn data_point_iterator(&self) -> impl Iterator<Item = &Journal> {
        self.work_stack
            .iter()
            .flat_map(|u| u.load.iter())
            .chain(self.current.load.iter())
    }
    fn close_work_unit(&mut self) {
        let prev = mem::replace(&mut self.current, WorkUnit::new());
        self.work_stack.push_front(prev);
    }
    fn creation_time(&self, journal: Journal) -> SystemTime {
        self.data_points
            // if data_points contains a value for the id,
            // this data point is older than the refactor.
            .get(&journal.id())
            .cloned()
            // In the case the data_point was created
            // after the refactor, no entry for it appears.
            // Is safe to use the journal time.
            .unwrap_or_else(|| journal.time())
    }
    #[allow(deprecated)]
    pub fn new() -> State {
        State {
            location: PathBuf::default(),
            no_nodes: usize::default(),
            current: WorkUnit::default(),
            delete_log: DTrie::default(),
            work_stack: LinkedList::default(),
            data_points: HashMap::default(),
            resources: HashMap::default(),
        }
    }
    pub fn search(
        &self,
        location: &Path,
        request: &dyn SearchRequest,
        similarity: Similarity,
    ) -> VectorR<Vec<Neighbour>> {
        let query = request.get_query();
        let filter = request.get_filter();
        let with_duplicates = request.with_duplicates();
        let no_results = request.no_results();
        let min_score = request.min_score();
        let mut ffsv = Fssc::new(request.no_results(), with_duplicates);
        for journal in self.data_point_iterator().copied() {
            let delete_log = self.delete_log(journal);
            let data_point = DataPoint::open(location, journal.id())?;
            let partial_solution = data_point.search(
                &delete_log,
                query,
                filter,
                with_duplicates,
                no_results,
                similarity,
                min_score,
            );
            for candidate in partial_solution {
                ffsv.add(candidate);
            }
        }
        Ok(ffsv.into())
    }
    pub fn remove(&mut self, id: &str, deleted_since: SystemTime) {
        self.delete_log.insert(id.as_bytes(), deleted_since);
    }
    pub fn add(&mut self, journal: Journal) {
        self.no_nodes += journal.no_nodes();
        self.current.add_unit(journal);
        if self.current.size() == BUFFER_CAP {
            self.close_work_unit();
        }
    }
    pub fn replace_work_unit(&mut self, journal: Journal) {
        let Some(unit) = self.work_stack.pop_back() else {
            return;
        };
        let age_cap = self
            .work_stack
            .back()
            .and_then(|v| v.load.last().map(|l| l.time()));
        if let Some(age_cap) = age_cap {
            self.delete_log.prune(age_cap);
        }
        for dp in unit.load.iter() {
            // The data_point may be older that the refactor
            self.data_points.remove(&dp.id());
            self.no_nodes -= dp.no_nodes();
        }
        self.add(journal);
    }
    pub fn dpid_iter(&self) -> impl Iterator<Item = DpId> + '_ {
        self.data_point_iterator()
            .copied()
            .map(|journal| journal.id())
    }
    pub fn keys(&self, location: &Path) -> VectorR<Vec<String>> {
        let mut keys = vec![];
        for journal in self.data_point_iterator().copied() {
            let delete_log = self.delete_log(journal);
            let dp_id = journal.id();
            let data_point = DataPoint::open(location, dp_id)?;
            let mut results = data_point.get_keys(&delete_log);
            keys.append(&mut results);
        }
        Ok(keys)
    }
    pub fn delete_log(&self, journal: Journal) -> impl DeleteLog + '_ {
        TimeSensitiveDLog {
            time: self.creation_time(journal),
            dlog: &self.delete_log,
        }
    }
    pub fn no_nodes(&self) -> usize {
        self.no_nodes
    }
    pub fn work_stack_len(&self) -> usize {
        self.work_stack.len()
    }
    pub fn current_work_unit(&self) -> Option<&[Journal]> {
        self.work_stack.back().map(|wu| wu.load.as_slice())
    }
    pub fn stored_len(&self, location: &Path) -> VectorR<Option<u64>> {
        let Some(journal) = self.data_point_iterator().next() else {
            return Ok(None);
        };
        let data_point = DataPoint::open(location, journal.id())?;
        Ok(data_point.stored_len())
    }
}

#[cfg(test)]
mod test {
    use std::path::Path;

    use nucliadb_core::Channel;
    use rand::random;
    use uuid::Uuid;

    use super::*;
    use crate::data_point::{Elem, LabelDictionary, Similarity};

    #[test]
    fn fssv_test() {
        let values: &[Neighbour] = &[
            Neighbour::dummy_neighbour(b"k0", 4.0),
            Neighbour::dummy_neighbour(b"k1", 3.0),
            Neighbour::dummy_neighbour(b"k2", 2.0),
            Neighbour::dummy_neighbour(b"k3", 1.0),
            Neighbour::dummy_neighbour(b"k4", 0.0),
        ];

        let mut fssv = Fssc::new(2, true);
        values.iter().for_each(|i| fssv.add(i.clone()));
        let result: Vec<_> = fssv.into();
        assert_eq!(result[0], values[0]);
        assert_eq!(result[0].score(), values[0].score());
        assert_eq!(result[1], values[1]);
        assert_eq!(result[1].score(), values[1].score());
    }

    #[test]
    fn fssv_test_no_duplicates() {
        let values: &[Neighbour] = &[
            Neighbour::dummy_neighbour(b"k0", 4.0),
            Neighbour::dummy_neighbour(b"k1", 3.0),
            Neighbour::dummy_neighbour(b"k2", 2.0),
            Neighbour::dummy_neighbour(b"k3", 1.0),
            Neighbour::dummy_neighbour(b"k4", 0.0),
        ];

        let mut fssv = Fssc::new(2, false);
        values.iter().for_each(|i| fssv.add(i.clone()));
        let result: Vec<_> = fssv.into();
        assert_eq!(result.len(), 1);
    }

    struct DataPointProducer<'a> {
        dimension: usize,
        path: &'a Path,
    }
    impl<'a> DataPointProducer<'a> {
        pub fn new(path: &'a Path) -> DataPointProducer<'a> {
            DataPointProducer {
                dimension: 12,
                path,
            }
        }
    }
    impl<'a> Iterator for DataPointProducer<'a> {
        type Item = DataPoint;
        fn next(&mut self) -> Option<Self::Item> {
            let no_vectors = random::<usize>() % 20;
            let mut elems = vec![];
            for _ in 0..no_vectors {
                let key = Uuid::new_v4().to_string();
                let labels = LabelDictionary::new(vec![]);
                let vector = (0..self.dimension)
                    .map(|_| random::<f32>())
                    .collect::<Vec<_>>();
                elems.push(Elem::new(key, vector, labels, None));
            }
            Some(
                DataPoint::new(
                    self.path,
                    elems,
                    None,
                    Similarity::Cosine,
                    Channel::EXPERIMENTAL,
                )
                .unwrap(),
            )
        }
    }

    #[test]
    fn state_test() {
        let dir = tempfile::TempDir::new().unwrap();
        let mut state = State::new();
        let no_nodes = DataPointProducer::new(dir.path())
            .take(5)
            .map(|dp| {
                let journal = dp.journal();
                let no_nodes = journal.no_nodes();
                state.add(journal);
                no_nodes
            })
            .sum::<usize>();
        assert_eq!(state.no_nodes(), no_nodes);
        assert_eq!(state.work_stack.len(), 1);
        assert_eq!(state.current.size(), 0);
        let work = state.current_work_unit().unwrap();
        let work = work
            .iter()
            .map(|j| (state.delete_log(*j), j.id()))
            .collect::<Vec<_>>();
        let new =
            DataPoint::merge(dir.path(), &work, Similarity::Cosine, Channel::EXPERIMENTAL).unwrap();
        std::mem::drop(work);
        state.replace_work_unit(new.journal());
        assert!(state.current_work_unit().is_none());
        assert_eq!(state.work_stack.len(), 0);
        assert_eq!(state.current.size(), 1);
        assert_eq!(state.no_nodes(), no_nodes);
        assert_eq!(state.work_stack.len(), 0);
        assert_eq!(state.current.size(), 1);
        assert_eq!(state.no_nodes(), no_nodes);
    }
}
