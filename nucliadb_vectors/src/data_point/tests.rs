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

use std::collections::HashSet;

use crate::data_point::{DataPoint, DeleteLog, Elem, LabelDictionary};

fn create_query() -> Vec<f32> {
    vec![rand::random::<f32>; 178]
        .into_iter()
        .map(|f| f())
        .collect()
}

impl DeleteLog for HashSet<String> {
    fn is_deleted(&self, x: &str) -> bool {
        self.contains(x)
    }
}

#[test]
fn simple_flow() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut elems = vec![];
    let mut labels = vec![];
    for i in 0..50 {
        labels.push(format!("LABEL_{}", i));
    }
    let mut expected_keys = vec![];
    let label_dictionary = LabelDictionary::new(labels.clone());
    for i in 0..50 {
        let key = format!("KEY_{}", i);
        let vector = vec![rand::random::<f32>(); 8];
        let labels = label_dictionary.clone();
        elems.push(Elem::new(key.clone(), vector, labels));
        expected_keys.push(key);
    }
    let reader = DataPoint::new(temp_dir.path(), elems, None).unwrap();
    let id = reader.get_id();
    let reader = DataPoint::open(temp_dir.path(), id).unwrap();
    let query = vec![rand::random::<f32>(); 8];
    let no_results = 10;
    let result = reader.search(&HashSet::new(), &query, &labels[..20], true, no_results);
    let got_keys = reader.get_keys(&HashSet::new());
    assert!(got_keys.iter().all(|k| expected_keys.contains(k)));
    assert_eq!(got_keys.len(), expected_keys.len());
    assert_eq!(result.len(), no_results);
}

#[test]
fn accuracy_test() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut labels = vec![];
    for i in 0..50 {
        labels.push(format!("LABEL_{}", i));
    }
    let labels_dictionary = LabelDictionary::new(labels.clone());
    let mut elems = Vec::new();
    for i in 0..100 {
        let key = format!("KEY_{}", i);
        let vector = create_query();
        let labels = labels_dictionary.clone();
        elems.push(Elem::new(key, vector, labels));
    }
    let reader = DataPoint::new(temp_dir.path(), elems, None).unwrap();
    let query = create_query();
    println!("QUERY 0: {:?}", query);
    let no_results = 10;
    let result_0 = reader.search(&HashSet::new(), &query, &labels[..20], true, no_results);
    let mut result_0: Vec<_> = result_0.into_iter().map(|(k, _)| k).collect();
    result_0.sort();
    let query = loop {
        let v = create_query();
        if query != v {
            break v;
        }
    };
    println!("QUERY 1: {:?}", query);
    let no_results = 10;
    let result_1 = reader.search(&HashSet::new(), &query, &labels[..20], true, no_results);
    let mut result_1: Vec<_> = result_1.into_iter().map(|(k, _)| k).collect();
    result_1.sort();
    println!("RESULT0: {:?}", result_0);
    println!("RESULT1: {:?}", result_1);
    assert_ne!(result_0, result_1)
}

#[test]
fn single_graph() {
    let temp_dir = tempfile::tempdir().unwrap();
    let key = "KEY_0".to_string();
    let vector = create_query();

    let elems = vec![Elem::new(
        key.clone(),
        vector.clone(),
        LabelDictionary::default(),
    )];
    let reader = DataPoint::new(temp_dir.path(), elems.clone(), None).unwrap();
    let result = reader.search(
        &HashSet::from([key.clone()]),
        &vector,
        &[] as &[String],
        true,
        5,
    );
    assert_eq!(result.len(), 0);

    let reader = DataPoint::new(temp_dir.path(), elems, None).unwrap();
    let result = reader.search(&HashSet::new(), &vector, &[] as &[String], true, 5);
    assert_eq!(result.len(), 1);
    assert!(result[0].1 >= 0.9);
    assert!(result[0].0 == key);
}

#[test]
fn data_merge() {
    let temp_dir = tempfile::tempdir().unwrap();

    let key0 = "KEY_0".to_string();
    let vector0 = create_query();
    let elems0 = vec![Elem::new(
        key0.clone(),
        vector0.clone(),
        LabelDictionary::default(),
    )];
    let key1 = "KEY_1".to_string();
    let vector1 = create_query();
    let elems1 = vec![Elem::new(
        key1.clone(),
        vector1.clone(),
        LabelDictionary::default(),
    )];
    let dp_0 = DataPoint::new(temp_dir.path(), elems0, None).unwrap();
    let dp_1 = DataPoint::new(temp_dir.path(), elems1, None).unwrap();
    let dp = DataPoint::merge(
        temp_dir.path(),
        &[
            (HashSet::default(), dp_1.get_id()),
            (HashSet::default(), dp_0.get_id()),
        ],
    )
    .unwrap();

    let result = dp.search(&HashSet::new(), &vector1, &[] as &[String], true, 1);
    assert_eq!(result.len(), 1);
    assert!(result[0].1 >= 0.9);
    assert!(result[0].0 == key1);
    let result = dp.search(&HashSet::new(), &vector0, &[] as &[String], true, 1);
    assert_eq!(result.len(), 1);
    assert!(result[0].1 >= 0.9);
    assert!(result[0].0 == key0);
    let dlog = HashSet::from([key1, key0]);
    let dp = DataPoint::merge(
        temp_dir.path(),
        &[(&dlog, dp_1.get_id()), (&dlog, dp_0.get_id())],
    )
    .unwrap();
    assert_eq!(dp.meta().no_nodes(), 0);
}
