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

use std::collections::HashSet;

use nucliadb_vectors2::utils::DeleteLog;
use nucliadb_vectors2::vectors::data_point::{DataPoint, Elem, LabelDictionary};

#[derive(Default)]
pub struct Set(HashSet<String>);
impl DeleteLog for Set {
    fn is_deleted(&self, x: &str) -> bool {
        self.0.contains(x)
    }
}

fn create_query() -> Vec<f32> {
    vec![rand::random::<f32>; 178]
        .into_iter()
        .map(|f| f())
        .collect()
}

#[cfg(not(merge))]
fn main() {
    use std::time::{Duration, SystemTime};
    println!("NO MERGER");
    let temp_dir = tempfile::tempdir().unwrap();
    let mut labels = vec![];
    for i in 0..50 {
        labels.push(format!("LABEL_{}", i));
    }
    let labels_dictionary = LabelDictionary::new(labels.clone());
    let mut elems = Vec::new();
    for current_key in 0..1000000 {
        let key = format!("KEY_{}", current_key);
        let vector = create_query();
        let labels = labels_dictionary.clone();
        elems.push(Elem::new(key, vector, labels));
    }

    let timer = SystemTime::now();
    let reader = DataPoint::new(temp_dir.path(), elems).unwrap();
    let write_time = timer.elapsed().unwrap();
    println!("Indexed in {} seconds", write_time.as_secs());

    let mut read_time = Duration::from_secs(0);
    for _index in 0..1000000 {
        let query = create_query();
        let no_results = 5;
        let query_labels = labels.clone();
        let timer = SystemTime::now();
        let _result = reader.search(&Set(HashSet::new()), &query, &query_labels, no_results);
        read_time += timer.elapsed().unwrap();
    }
    println!("reading in {} seconds", read_time.as_secs());
}

#[cfg(merge)]
fn main() {
    use std::time::{Duration, SystemTime};
    println!("MERGER");
    let delete_log = Set::default();
    let temp_dir = tempfile::tempdir().unwrap();
    let mut labels = vec![];
    for i in 0..50 {
        labels.push(format!("LABEL_{}", i));
    }
    let labels_dictionary = LabelDictionary::new(labels.clone());
    let mut elems = Vec::new();
    for current_key in 0..500000 {
        let key = format!("KEY_{}", current_key);
        let vector = create_query();
        let labels = labels_dictionary.clone();
        elems.push(Elem::new(key, vector, labels));
    }
    let r0 = DataPoint::new(temp_dir.path(), elems).unwrap().get_id();

    let mut elems = Vec::new();
    for current_key in 500000..1000000 {
        let key = format!("KEY_{}", current_key);
        let vector = create_query();
        let labels = labels_dictionary.clone();
        elems.push(Elem::new(key, vector, labels));
    }
    let r1 = DataPoint::new(temp_dir.path(), elems).unwrap().get_id();

    let timer = SystemTime::now();
    let reader = DataPoint::merge(temp_dir.path(), &[r1, r0], &delete_log).unwrap();
    let write_time = timer.elapsed().unwrap();
    println!("no_nodes: {}", reader.meta().no_nodes());
    println!("Merged in {} seconds", write_time.as_secs());

    let mut read_time = Duration::from_secs(0);
    for _index in 0..1000 {
        let query = create_query();
        let no_results = 5;
        let query_labels = labels.clone();
        let timer = SystemTime::now();
        let _ = reader.search(&Set(HashSet::new()), &query, &query_labels, no_results);
        read_time += timer.elapsed().unwrap();
    }
    println!("reading in {} seconds", read_time.as_secs());
}
