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

use nucliadb_core::Channel;

use crate::data_point::{DataPoint, DeleteLog, Elem, LabelDictionary, Similarity};
use crate::formula::{AtomClause, Formula};

const SIMILARITY: Similarity = Similarity::Cosine;

fn create_query() -> Vec<f32> {
    vec![rand::random::<f32>; 178]
        .into_iter()
        .map(|f| f())
        .collect()
}

impl DeleteLog for HashSet<String> {
    fn is_deleted(&self, x: &[u8]) -> bool {
        let as_str = String::from_utf8_lossy(x).to_string();
        self.contains(&as_str)
    }
}

#[test]
fn simple_flow() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut elems = vec![];
    let mut labels = vec![];
    let mut queries = vec![];
    for i in 0..50 {
        labels.push(format!("LABEL_{}", i));
        queries.push(AtomClause::label(format!("LABEL_{}", i)));
    }
    let mut expected_keys = vec![];
    let label_dictionary = LabelDictionary::new(labels.clone());
    for i in 0..50 {
        let key = format!("KEY_{}", i);
        let vector = vec![rand::random::<f32>(); 8];
        let labels = label_dictionary.clone();
        elems.push(Elem::new(key.clone(), vector, labels, None));
        expected_keys.push(key);
    }
    let reader = DataPoint::new(
        temp_dir.path(),
        elems,
        None,
        SIMILARITY,
        Channel::EXPERIMENTAL,
    )
    .unwrap();
    let id = reader.get_id();
    let reader = DataPoint::open(temp_dir.path(), id).unwrap();
    let query = vec![rand::random::<f32>(); 8];
    let no_results = 10;
    let formula = queries[..20].iter().fold(Formula::new(), |mut acc, i| {
        acc.extend(i.clone());
        acc
    });
    let result = reader.search(
        &HashSet::new(),
        &query,
        &formula,
        true,
        no_results,
        Similarity::Cosine,
        -1.0,
    );
    let got_keys = reader.get_keys(&HashSet::new());
    assert!(got_keys.iter().all(|k| expected_keys.contains(k)));
    assert_eq!(got_keys.len(), expected_keys.len());
    assert_eq!(result.count(), no_results);
}

#[test]
fn accuracy_test() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut labels = vec![];
    let mut queries = vec![];
    for i in 0..50 {
        labels.push(format!("LABEL_{}", i));
        queries.push(AtomClause::label(format!("LABEL_{}", i)));
    }
    let labels_dictionary = LabelDictionary::new(labels.clone());
    let mut elems = Vec::new();
    for i in 0..100 {
        let key = format!("KEY_{}", i);
        let vector = create_query();
        let labels = labels_dictionary.clone();
        elems.push(Elem::new(key, vector, labels, None));
    }
    let reader = DataPoint::new(
        temp_dir.path(),
        elems,
        None,
        SIMILARITY,
        Channel::EXPERIMENTAL,
    )
    .unwrap();
    let query = create_query();
    let no_results = 10;
    let formula = queries[..20].iter().fold(Formula::new(), |mut acc, i| {
        acc.extend(i.clone());
        acc
    });
    let mut result_0 = reader
        .search(
            &HashSet::new(),
            &query,
            &formula,
            true,
            no_results,
            Similarity::Cosine,
            -1.0,
        )
        .collect::<Vec<_>>();
    result_0.sort_by(|i, j| i.id().cmp(j.id()));
    let query: Vec<_> = query.into_iter().map(|v| v + 1.0).collect();
    let no_results = 10;
    let mut result_1 = reader
        .search(
            &HashSet::new(),
            &query,
            &formula,
            true,
            no_results,
            Similarity::Cosine,
            -1.0,
        )
        .collect::<Vec<_>>();
    result_1.sort_by(|i, j| i.id().cmp(j.id()));
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
        None,
    )];
    let reader = DataPoint::new(
        temp_dir.path(),
        elems.clone(),
        None,
        SIMILARITY,
        Channel::EXPERIMENTAL,
    )
    .unwrap();
    let formula = Formula::new();
    let result = reader.search(
        &HashSet::from([key.clone()]),
        &vector,
        &formula,
        true,
        5,
        Similarity::Cosine,
        -1.0,
    );
    assert_eq!(result.count(), 0);

    let reader = DataPoint::new(
        temp_dir.path(),
        elems,
        None,
        SIMILARITY,
        Channel::EXPERIMENTAL,
    )
    .unwrap();
    let result = reader
        .search(
            &HashSet::new(),
            &vector,
            &formula,
            true,
            5,
            Similarity::Cosine,
            -1.0,
        )
        .collect::<Vec<_>>();
    assert_eq!(result.len(), 1);
    assert!(result[0].score() >= 0.9);
    assert!(result[0].id() == key.as_bytes());
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
        None,
    )];
    let key1 = "KEY_1".to_string();
    let vector1 = create_query();
    let elems1 = vec![Elem::new(
        key1.clone(),
        vector1.clone(),
        LabelDictionary::default(),
        None,
    )];
    let dp_0 = DataPoint::new(
        temp_dir.path(),
        elems0,
        None,
        SIMILARITY,
        Channel::EXPERIMENTAL,
    )
    .unwrap();
    let dp_1 = DataPoint::new(
        temp_dir.path(),
        elems1,
        None,
        SIMILARITY,
        Channel::EXPERIMENTAL,
    )
    .unwrap();
    let dp = DataPoint::merge(
        temp_dir.path(),
        &[
            (HashSet::default(), dp_1.get_id()),
            (HashSet::default(), dp_0.get_id()),
        ],
        Similarity::Cosine,
        Channel::EXPERIMENTAL,
    )
    .unwrap();
    let formula = Formula::new();
    let result: Vec<_> = dp
        .search(
            &HashSet::new(),
            &vector1,
            &formula,
            true,
            1,
            Similarity::Cosine,
            -1.0,
        )
        .collect();
    assert_eq!(result.len(), 1);
    assert!(result[0].score() >= 0.9);
    assert!(result[0].id() == key1.as_bytes());
    let result: Vec<_> = dp
        .search(
            &HashSet::new(),
            &vector0,
            &formula,
            true,
            1,
            Similarity::Cosine,
            -1.0,
        )
        .collect();
    assert_eq!(result.len(), 1);
    assert!(result[0].score() >= 0.9);
    assert!(result[0].id() == key0.as_bytes());
    let dlog = HashSet::from([key1, key0]);
    let dp = DataPoint::merge(
        temp_dir.path(),
        &[(&dlog, dp_1.get_id()), (&dlog, dp_0.get_id())],
        Similarity::Cosine,
        Channel::EXPERIMENTAL,
    )
    .unwrap();
    assert_eq!(dp.journal().no_nodes(), 0);
}

#[test]
fn prefiltering_test() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut labels = vec![];
    let mut queries = vec![];
    for i in 0..100 {
        labels.push(format!("LABEL_{}", i));
    }
    for i in 0..5 {
        queries.push(AtomClause::label(format!("LABEL_{}", i)));
    }
    let mut elems = Vec::new();
    for i in 0..100 {
        let key = format!("KEY_{}", i);
        let vector = create_query();

        let labels = LabelDictionary::new(vec![format!("LABEL_{}", i)]);
        elems.push(Elem::new(key, vector, labels, None));
    }
    let reader = DataPoint::new(
        temp_dir.path(),
        elems,
        None,
        SIMILARITY,
        Channel::EXPERIMENTAL,
    )
    .unwrap();
    let query = create_query();
    let no_results = 10;

    for i in 0..5 {
        let formula = queries[i..i + 1].iter().fold(Formula::new(), |mut acc, i| {
            acc.extend(i.clone());
            acc
        });
        let result_0 = reader
            .search(
                &HashSet::new(),
                &query,
                &formula,
                true,
                no_results,
                Similarity::Cosine,
                -1.0,
            )
            .collect::<Vec<_>>();
        assert_eq!(result_0.len(), 1);

        let delete_log: HashSet<_> = result_0
            .into_iter()
            .map(|n| String::from_utf8_lossy(n.id()).to_string())
            .collect();
        let result_with_deleted = reader
            .search(
                &delete_log,
                &query,
                &formula,
                true,
                no_results,
                Similarity::Cosine,
                -1.0,
            )
            .collect::<Vec<_>>();
        assert_eq!(result_with_deleted.len(), 0);
    }
}
