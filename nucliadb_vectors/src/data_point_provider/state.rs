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

use std::collections::{HashMap, LinkedList};
use std::mem;
use std::path::PathBuf;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

use super::merge_worker::Worker;
use super::{merger, SearchRequest, VectorR};
use crate::data_point::{DataPoint, DpId, Journal};
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
    dlog: &'a DTrie<SystemTime>,
    time: SystemTime,
}
impl<'a> DeleteLog for TimeSensitiveDLog<'a> {
    fn is_deleted(&self, key: &str) -> bool {
        self.dlog
            .get(key.as_bytes())
            .map(|t| *t > self.time)
            .unwrap_or_default()
    }
}

// Fixed-sized sorted collection
struct Fssc {
    size: usize,
    buff: HashMap<String, f32>,
}
impl From<Fssc> for Vec<(String, f32)> {
    fn from(fssv: Fssc) -> Self {
        let mut result: Vec<_> = fssv.buff.into_iter().collect();
        result.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        result
    }
}
impl Fssc {
    fn is_full(&self) -> bool {
        self.buff.len() == self.size
    }
    fn new(size: usize) -> Fssc {
        Fssc {
            size,
            buff: HashMap::new(),
        }
    }
    fn add(&mut self, candidate: String, score: f32) {
        if self.is_full() {
            let smallest_bigger = self
                .buff
                .iter()
                .map(|(key, score)| (key.clone(), *score))
                .filter(|(_, v)| score > *v)
                .min_by(|(_, v0), (_, v1)| v0.partial_cmp(v1).unwrap());
            if let Some((key, _)) = smallest_bigger {
                self.buff.remove(&key);
                self.buff.insert(candidate, score);
            }
        } else {
            self.buff.insert(candidate, score);
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct State {
    location: PathBuf,

    // Total number of nodes stored. Some
    // may be marked as deleted but are waiting
    // for a merge to be fully removed.
    no_nodes: usize,

    // Current work unit
    current: WorkUnit,

    // Trie containing the deleted keys and the
    // time when they were deleted
    delete_log: DTrie<SystemTime>,

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
        let notifier = merger::get_notifier();
        if let Err(e) = notifier.send(Worker::request(self.location.clone())) {
            tracing::info!("Could not request merge: {}", e);
        }
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
    pub fn new(at: PathBuf) -> State {
        State {
            location: at,
            no_nodes: usize::default(),
            current: WorkUnit::default(),
            delete_log: DTrie::default(),
            work_stack: LinkedList::default(),
            data_points: HashMap::default(),
            resources: HashMap::default(),
        }
    }
    pub fn work_sanity_check(&self) {
        for _ in self.work_stack.iter() {
            let notifier = merger::get_notifier();
            if let Err(e) = notifier.send(Worker::request(self.location.clone())) {
                tracing::info!("Could not request merge: {}", e);
            }
        }
    }
    pub fn search(&self, request: &dyn SearchRequest) -> VectorR<Vec<(String, f32)>> {
        let mut ffsv = Fssc::new(request.no_results());
        for journal in self.data_point_iterator().copied() {
            let delete_log = self.delete_log(journal);
            let data_point = DataPoint::open(&self.location, journal.id())?;
            let results = data_point.search(
                &delete_log,
                request.get_query(),
                request.get_labels(),
                request.with_duplicates(),
                request.no_results(),
            );
            results
                .into_iter()
                .for_each(|(candidate, score)| ffsv.add(candidate, score));
        }
        Ok(ffsv.into())
    }
    pub fn remove(&mut self, id: &str, deleted_since: SystemTime) {
        self.delete_log.insert(id.as_bytes(), deleted_since);
    }
    pub fn add(&mut self, dp: DataPoint) {
        let meta = dp.meta();
        self.no_nodes += meta.no_nodes();
        self.current.add_unit(meta);
        if self.current.size() == BUFFER_CAP {
            self.close_work_unit();
        }
    }
    pub fn replace_work_unit(&mut self, new: DataPoint) {
        if let Some(unit) = self.work_stack.pop_back() {
            let age_cap = self
                .work_stack
                .back()
                .and_then(|v| v.load.last().map(|l| l.time()));
            let older = self
                .delete_log
                .iter()
                .filter(|(_, age)| age_cap.map(|cap| **age <= cap).unwrap_or_default())
                .map(|(key, _)| key)
                .collect::<Vec<_>>();
            older.iter().for_each(|v| self.delete_log.delete(v));
            unit.load.iter().cloned().for_each(|dp| {
                // The data_point may be older that the refactor
                self.data_points.remove(&dp.id());
                self.no_nodes -= dp.no_nodes();
            });
            self.add(new);
        }
    }
    pub fn keys(&self) -> VectorR<Vec<String>> {
        let mut keys = vec![];
        for journal in self.data_point_iterator().copied() {
            let delete_log = self.delete_log(journal);
            let dp_id = journal.id();
            let data_point = DataPoint::open(&self.location, dp_id)?;
            let mut results = data_point.get_keys(&delete_log);
            keys.append(&mut results);
        }
        Ok(keys)
    }
    pub fn no_nodes(&self) -> usize {
        self.no_nodes
    }
    pub fn delete_log(&self, journal: Journal) -> impl DeleteLog + '_ {
        TimeSensitiveDLog {
            time: self.creation_time(journal),
            dlog: &self.delete_log,
        }
    }
    pub fn current_work_unit(&self) -> Option<&[Journal]> {
        self.work_stack.back().map(|wu| wu.load.as_slice())
    }
}

#[cfg(test)]
mod test {
    use std::path::Path;

    use rand::random;
    use uuid::Uuid;

    use super::*;
    use crate::data_point::{Elem, LabelDictionary};
    #[test]
    fn fssv_test() {
        const VALUES: &[(&str, f32)] = &[
            ("k0", 4.0),
            ("k1", 3.0),
            ("k2", 2.0),
            ("k3", 1.0),
            ("k4", 0.0),
        ];

        let mut fssv = Fssc::new(2);
        VALUES
            .iter()
            .map(|(key, value)| (key.to_string(), *value))
            .for_each(|(k, v)| fssv.add(k, v));
        let result: Vec<_> = fssv.into();
        assert_eq!(&result[0].0, VALUES[0].0);
        assert_eq!(result[0].1, VALUES[0].1);
        assert_eq!(&result[1].0, VALUES[1].0);
        assert_eq!(result[1].1, VALUES[1].1);
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
                    .into_iter()
                    .map(|_| random::<f32>())
                    .collect::<Vec<_>>();
                elems.push(Elem::new(key, vector, labels));
            }
            Some(DataPoint::new(self.path, elems, None).unwrap())
        }
    }

    #[test]
    fn state_test() {
        let dir = tempfile::TempDir::new().unwrap();
        let mut state = State::new(dir.path().to_path_buf());
        let no_nodes = DataPointProducer::new(dir.path())
            .take(5)
            .map(|dp| {
                let no_nodes = dp.meta().no_nodes();
                state.add(dp);
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
        let new = DataPoint::merge(dir.path(), &work).unwrap();
        std::mem::drop(work);
        state.replace_work_unit(new);
        assert!(state.current_work_unit().is_none());
        assert_eq!(state.work_stack.len(), 0);
        assert_eq!(state.current.size(), 1);
        assert_eq!(state.no_nodes(), no_nodes);
        assert_eq!(state.work_stack.len(), 0);
        assert_eq!(state.current.size(), 1);
        assert_eq!(state.no_nodes(), no_nodes);
    }
}
