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
use super::{SearchRequest, VectorR};
use crate::utils::dtrie::DTrie;
use crate::utils::{merger, DeleteLog};
use crate::vectors::data_point::{DataPoint, DpId, Journal};
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
        let mut result = fssv.buff.into_iter().collect::<Vec<_>>();
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
    fn add(&mut self, (candidate, score): (String, f32)) {
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
    no_nodes: usize,
    current: WorkUnit,
    delete_log: DTrie<SystemTime>,
    work_stack: LinkedList<WorkUnit>,
    data_points: HashMap<DpId, SystemTime>,
    resources: HashMap<String, usize>,
}
impl State {
    fn close_work_unit(&mut self) {
        let prev = mem::replace(&mut self.current, WorkUnit::new());
        self.work_stack.push_front(prev);
        let notifier = merger::get_notifier();
        if let Err(e) = notifier.send(Worker::request(self.location.clone())) {
            tracing::info!("Could not request merge: {}", e);
        }
    }
    fn add_dp(&mut self, dp: DataPoint, time: SystemTime) {
        let mut meta = dp.meta();
        meta.update_time(time);
        self.data_points.insert(meta.id(), meta.time());
        self.current.add_unit(meta);
        if self.current.size() == BUFFER_CAP {
            self.close_work_unit();
        }
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
    pub fn get_no_nodes(&self) -> usize {
        self.no_nodes
    }
    pub fn get_keys(&self) -> impl Iterator<Item = &str> {
        self.resources.keys().map(|k| k.as_str())
    }
    pub fn create_dlog(&self, journal: Journal) -> impl DeleteLog + '_ {
        TimeSensitiveDLog {
            time: journal.time(),
            dlog: &self.delete_log,
        }
    }
    pub fn get_work(&self) -> Option<&[Journal]> {
        self.work_stack.back().map(|wu| wu.load.as_slice())
    }
    pub fn search(&self, request: &dyn SearchRequest) -> VectorR<Vec<(String, f32)>> {
        let mut ffsv = Fssc::new(request.no_results());
        let mut delete_log = TimeSensitiveDLog {
            dlog: &self.delete_log,
            time: SystemTime::now(),
        };
        for (dp_id, time) in self.data_points.iter() {
            delete_log.time = *time;
            let data_point = DataPoint::open(&self.location, *dp_id)?;
            let results = data_point.search(
                &delete_log,
                request.get_query(),
                request.get_labels(),
                request.with_duplicates(),
                request.no_results(),
            );
            results.into_iter().for_each(|r| ffsv.add(r));
        }
        Ok(ffsv.into())
    }
    pub fn has_id(&self, id: &str) -> bool {
        self.resources.contains_key(id)
    }
    pub fn remove(&mut self, id: &str) {
        if let Some(no_nodes) = self.resources.remove(id) {
            self.no_nodes -= no_nodes;
            self.delete_log.insert(id.as_bytes(), SystemTime::now());
            if self.current.size() > 0 {
                self.close_work_unit();
            }
        }
    }
    pub fn add(&mut self, id: String, dp: DataPoint) {
        self.remove(&id);
        self.resources.insert(id, dp.meta().no_nodes());
        self.no_nodes += dp.meta().no_nodes();
        self.add_dp(dp, SystemTime::now());
    }
    pub fn replace_work_unit(&mut self, new: DataPoint, ctime: SystemTime) {
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
                self.data_points.remove(&dp.id());
            });
            self.add_dp(new, ctime);
        }
    }
}

#[cfg(test)]
mod test {
    use std::path::Path;

    use rand::random;
    use uuid::Uuid;

    use super::*;
    use crate::vectors::data_point::{Elem, LabelDictionary};
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
            .for_each(|(k, v)| fssv.add((k, v)));
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
            Some(DataPoint::new(self.path, elems).unwrap())
        }
    }

    #[test]
    fn state_test() {
        let dir = tempfile::TempDir::new().unwrap();
        let mut state = State::new(dir.path().to_path_buf());
        let no_nodes = DataPointProducer::new(dir.path())
            .take(5)
            .map(|dp| {
                let id = dp.meta().id().to_string();
                let no_nodes = dp.meta().no_nodes();
                state.add(id, dp);
                no_nodes
            })
            .sum::<usize>();
        assert_eq!(state.get_no_nodes(), no_nodes);
        assert_eq!(state.work_stack.len(), 1);
        assert_eq!(state.current.size(), 0);
        let work = state.get_work().unwrap();
        let work = work
            .iter()
            .map(|j| (state.create_dlog(*j), j.id()))
            .collect::<Vec<_>>();
        let new = DataPoint::merge(dir.path(), &work).unwrap();
        std::mem::drop(work);
        state.replace_work_unit(new, SystemTime::now());
        assert!(state.get_work().is_none());
        assert_eq!(state.work_stack.len(), 0);
        assert_eq!(state.current.size(), 1);
        assert_eq!(state.get_no_nodes(), no_nodes);
        assert_eq!(state.work_stack.len(), 0);
        assert_eq!(state.current.size(), 1);
        assert_eq!(state.get_no_nodes(), no_nodes);
    }
}
