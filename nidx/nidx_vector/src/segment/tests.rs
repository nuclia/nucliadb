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
use std::time::Instant;

use tempfile::tempdir;

use crate::VectorR;
use crate::config::{Similarity, VectorCardinality, VectorConfig, flags};
use crate::data_store::{DataStoreV1, DataStoreV2};
use crate::formula::{AtomClause, Clause, Formula};
use crate::segment::{self, Elem};

const CONFIG: VectorConfig = VectorConfig {
    similarity: Similarity::Cosine,
    normalize_vectors: false,
    vector_type: crate::config::VectorType::DenseF32 { dimension: 178 },
    flags: vec![],
    vector_cardinality: VectorCardinality::Single,
};

fn create_query() -> Vec<f32> {
    let v: Vec<_> = vec![rand::random::<f32>; 178].into_iter().map(|f| f()).collect();
    let mut modulus = 0.0;
    for w in &v {
        modulus += w * w;
    }
    modulus = f32::powf(modulus, 0.5);

    v.into_iter().map(|w| w / modulus).collect()
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
    for i in 0..50 {
        let key = format!("9cb39c75f8d9498d8f82d92b173011f5/f/field/0-{i}");
        let vector = vec![rand::random::<f32>(); 178];
        elems.push(Elem::new(key.clone(), vector, labels.clone(), None));
        expected_keys.push(key);
    }
    let segment = segment::create(temp_dir.path(), elems, &CONFIG, HashSet::new()).unwrap();
    let query = vec![rand::random::<f32>(); 178];
    let no_results = 10;
    let formula = queries[..20].iter().fold(Formula::new(), |mut acc, i| {
        acc.extend(i.clone());
        acc
    });
    let result = segment.search(&query, &formula, true, no_results, &CONFIG, -1.0);
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
    let mut elems = Vec::new();
    for i in 0..100 {
        let key = format!("9cb39c75f8d9498d8f82d92b173011f5/f/field/0-{i}");
        let vector = create_query();
        elems.push(Elem::new(key, vector, labels.clone(), None));
    }
    let segment = segment::create(temp_dir.path(), elems, &CONFIG, HashSet::new()).unwrap();
    let query = create_query();
    let no_results = 10;
    let formula = queries[..20].iter().fold(Formula::new(), |mut acc, i| {
        acc.extend(i.clone());
        acc
    });
    let mut result_0 = segment
        .search(&query, &formula, true, no_results, &CONFIG, -1.0)
        .collect::<Vec<_>>();
    result_0.sort_by_key(|i| i.paragraph());
    let query: Vec<_> = query.into_iter().map(|v| v + 1.0).collect();
    let no_results = 10;
    let mut result_1 = segment
        .search(&query, &formula, true, no_results, &CONFIG, -1.0)
        .collect::<Vec<_>>();
    result_1.sort_by_key(|i| i.paragraph());

    let mut equal = true;
    for (r0, r1) in result_0.iter().zip(result_1.iter()) {
        if r0.vector() != r1.vector() {
            equal = false;
        }
    }
    assert!(!equal);
}

#[test]
fn single_graph() {
    let temp_dir = tempfile::tempdir().unwrap();
    let key = "9cb39c75f8d9498d8f82d92b173011f5/f/field/0-100".to_string();
    let vector = create_query();

    let elems = vec![Elem::new(key.clone(), vector.clone(), vec![], None)];
    let mut segment = segment::create(temp_dir.path(), elems.clone(), &CONFIG, HashSet::new()).unwrap();
    let formula = Formula::new();
    segment.apply_deletion(&key);
    let result = segment.search(&vector, &formula, true, 5, &CONFIG, -1.0);
    assert_eq!(result.count(), 0);

    let temp_dir = tempfile::tempdir().unwrap();
    let segment = segment::create(temp_dir.path(), elems, &CONFIG, HashSet::new()).unwrap();
    let result = segment
        .search(&vector, &formula, true, 5, &CONFIG, -1.0)
        .collect::<Vec<_>>();
    assert_eq!(result.len(), 1);
    assert!(result[0].score() >= 0.9);
    assert!(segment.get_paragraph(result[0].paragraph()).id() == key);
}

#[test]
fn data_merge() -> anyhow::Result<()> {
    let mut v1_config = CONFIG.clone();
    v1_config.flags.push(flags::FORCE_DATA_STORE_V1.to_string());

    let key0 = "9cb39c75f8d9498d8f82d92b173011f5/f/field/0-100".to_string();
    let vector0 = create_query();
    let elems0 = vec![Elem::new(key0.clone(), vector0.clone(), vec![], None)];
    let key1 = "29ee1f6e4585423585f31ded0202ee3a/f/field/0-100".to_string();
    let vector1 = create_query();
    let elems1 = vec![Elem::new(key1.clone(), vector1.clone(), vec![], None)];

    let dp0_path = tempdir()?;
    let dp0 = segment::create(dp0_path.path(), elems0, &v1_config, HashSet::new()).unwrap();
    assert!(dp0.data_store.as_any().downcast_ref::<DataStoreV1>().is_some());

    let dp1_path = tempdir()?;
    let dp1 = segment::create(dp1_path.path(), elems1, &v1_config, HashSet::new()).unwrap();
    assert!(dp1.data_store.as_any().downcast_ref::<DataStoreV1>().is_some());

    let work = &[&dp1, &dp0];

    let dp_path = tempdir()?;
    let dp = segment::merge(dp_path.path(), work, &v1_config).unwrap();
    assert!(dp.data_store.as_any().downcast_ref::<DataStoreV1>().is_some());

    let formula = Formula::new();
    let result: Vec<_> = dp.search(&vector1, &formula, true, 1, &v1_config, -1.0).collect();
    assert_eq!(result.len(), 1);
    assert!(result[0].score() >= 0.9);
    assert!(dp.get_paragraph(result[0].paragraph()).id() == key1);
    let result: Vec<_> = dp.search(&vector0, &formula, true, 1, &v1_config, -1.0).collect();
    assert_eq!(result.len(), 1);
    assert!(result[0].score() >= 0.9);
    assert!(dp.get_paragraph(result[0].paragraph()).id() == key0);
    let mut dp0 = segment::open(dp0.metadata, &v1_config).unwrap();
    let mut dp1 = segment::open(dp1.metadata, &v1_config).unwrap();
    dp0.apply_deletion(&key0);
    dp0.apply_deletion(&key1);
    dp1.apply_deletion(&key0);
    dp1.apply_deletion(&key1);
    let work = &[&dp1, &dp0];

    let dp_path = tempdir()?;
    let dp = segment::merge(dp_path.path(), work, &v1_config).unwrap();

    assert_eq!(dp.metadata.records, 0);

    Ok(())
}

#[test]
fn data_merge_v2() -> anyhow::Result<()> {
    let key0 = "9cb39c75f8d9498d8f82d92b173011f5/f/field/0-100".to_string();
    let vector0 = create_query();
    let elems0 = vec![Elem::new(key0.clone(), vector0.clone(), vec![], None)];
    let key1 = "29ee1f6e4585423585f31ded0202ee3a/f/field/0-100".to_string();
    let vector1 = create_query();
    let elems1 = vec![Elem::new(key1.clone(), vector1.clone(), vec![], None)];

    let dp0_path = tempdir()?;
    let dp0 = segment::create(dp0_path.path(), elems0, &CONFIG, HashSet::new()).unwrap();
    assert!(dp0.data_store.as_any().downcast_ref::<DataStoreV2>().is_some());

    let dp1_path = tempdir()?;
    let dp1 = segment::create(dp1_path.path(), elems1, &CONFIG, HashSet::new()).unwrap();
    assert!(dp1.data_store.as_any().downcast_ref::<DataStoreV2>().is_some());

    let work = &[&dp1, &dp0];

    let dp_path = tempdir()?;
    let dp = segment::merge(dp_path.path(), work, &CONFIG).unwrap();
    assert!(dp.data_store.as_any().downcast_ref::<DataStoreV2>().is_some());

    let formula = Formula::new();
    let result: Vec<_> = dp.search(&vector1, &formula, true, 1, &CONFIG, -1.0).collect();
    assert_eq!(result.len(), 1);
    assert!(result[0].score() >= 0.9);
    assert!(dp.get_paragraph(result[0].paragraph()).id() == key1);
    let result: Vec<_> = dp.search(&vector0, &formula, true, 1, &CONFIG, -1.0).collect();
    assert_eq!(result.len(), 1);
    assert!(result[0].score() >= 0.9);
    assert!(dp.get_paragraph(result[0].paragraph()).id() == key0);
    let mut dp0 = segment::open(dp0.metadata, &CONFIG).unwrap();
    let mut dp1 = segment::open(dp1.metadata, &CONFIG).unwrap();
    dp0.apply_deletion(&key0);
    dp0.apply_deletion(&key1);
    dp1.apply_deletion(&key0);
    dp1.apply_deletion(&key1);
    let work = &[&dp1, &dp0];

    let dp_path = tempdir()?;
    let dp = segment::merge(dp_path.path(), work, &CONFIG).unwrap();

    assert_eq!(dp.metadata.records, 0);

    Ok(())
}

#[test]
fn data_merge_mixed() -> anyhow::Result<()> {
    let key0 = "9cb39c75f8d9498d8f82d92b173011f5/f/field/0-100".to_string();
    let vector0 = create_query();
    let elems0 = vec![Elem::new(key0.clone(), vector0.clone(), vec![], None)];
    let key1 = "29ee1f6e4585423585f31ded0202ee3a/f/field/0-100".to_string();
    let vector1 = create_query();
    let elems1 = vec![Elem::new(key1.clone(), vector1.clone(), vec![], None)];

    let mut v1_config = CONFIG.clone();
    v1_config.flags.push(flags::FORCE_DATA_STORE_V1.to_string());

    let dp0_path = tempdir()?;
    let dp0 = segment::create(dp0_path.path(), elems0, &v1_config, HashSet::new()).unwrap();
    assert!(dp0.data_store.as_any().downcast_ref::<DataStoreV1>().is_some());

    let dp1_path = tempdir()?;
    let dp1 = segment::create(dp1_path.path(), elems1, &CONFIG, HashSet::new()).unwrap();
    assert!(dp1.data_store.as_any().downcast_ref::<DataStoreV2>().is_some());

    let work = &[&dp1, &dp0];

    let dp_path = tempdir()?;
    let dp = segment::merge(dp_path.path(), work, &CONFIG).unwrap();
    assert!(dp.data_store.as_any().downcast_ref::<DataStoreV2>().is_some());

    let formula = Formula::new();
    let result: Vec<_> = dp.search(&vector1, &formula, true, 1, &CONFIG, -1.0).collect();
    assert_eq!(result.len(), 1);
    assert!(result[0].score() >= 0.9);
    assert!(dp.get_paragraph(result[0].paragraph()).id() == key1);
    let result: Vec<_> = dp.search(&vector0, &formula, true, 1, &CONFIG, -1.0).collect();
    assert_eq!(result.len(), 1);
    assert!(result[0].score() >= 0.9);
    assert!(dp.get_paragraph(result[0].paragraph()).id() == key0);
    let mut dp0 = segment::open(dp0.metadata, &CONFIG).unwrap();
    let mut dp1 = segment::open(dp1.metadata, &CONFIG).unwrap();
    dp0.apply_deletion(&key0);
    dp0.apply_deletion(&key1);
    dp1.apply_deletion(&key0);
    dp1.apply_deletion(&key1);
    let work = &[&dp1, &dp0];

    let dp_path = tempdir()?;
    let dp = segment::merge(dp_path.path(), work, &CONFIG).unwrap();

    assert_eq!(dp.metadata.records, 0);

    Ok(())
}

#[test]
fn label_filtering_test() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut queries = vec![];
    for i in 0..5 {
        queries.push(AtomClause::label(format!("LABEL_{}", i)));
    }
    let mut elems = Vec::new();
    for i in 0..100 {
        let key = format!("6e5a546a9a5c480f8579472016b1ee14/f/field/{}-{}", i, i + 1);
        let vector = create_query();

        let labels = vec![format!("LABEL_{}", i)];
        elems.push(Elem::new(key, vector, labels, None));
    }

    let mut segment = segment::create(temp_dir.path(), elems, &CONFIG, HashSet::new()).unwrap();
    let query = create_query();
    let no_results = 10;

    for i in 0..5 {
        let formula = queries[i..i + 1].iter().fold(Formula::new(), |mut acc, i| {
            acc.extend(i.clone());
            acc
        });
        let result_0 = segment
            .search(&query, &formula, true, no_results, &CONFIG, -1.0)
            .collect::<Vec<_>>();
        assert_eq!(result_0.len(), 1);
    }

    segment.apply_deletion("6e5a546a9a5c480f8579472016b1ee14/f/field");
    for i in 0..5 {
        let formula = queries[i..i + 1].iter().fold(Formula::new(), |mut acc, i| {
            acc.extend(i.clone());
            acc
        });
        let result_0 = segment
            .search(&query, &formula, true, no_results, &CONFIG, -1.0)
            .collect::<Vec<_>>();
        assert_eq!(result_0.len(), 0);
    }
}

#[test]
fn label_prefix_search_test() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut elems = Vec::new();
    for i in 0..5 {
        let key = format!("6e5a546a9a5c480f8579472016b1ee14/f/field/{}-{}", i, i + 1);
        let vector = create_query();

        let labels = vec![format!("/l/labelset/LABEL_{}", i)];
        elems.push(Elem::new(key, vector, labels, None));
    }

    let segment = segment::create(temp_dir.path(), elems, &CONFIG, HashSet::new()).unwrap();
    let query = create_query();

    // Searching for the labelset, returns all results
    let mut formula = Formula::new();
    formula.extend(Clause::Atom(AtomClause::Label("/l/labelset".into())));
    let result = segment
        .search(&query, &formula, true, 10, &CONFIG, -1.0)
        .collect::<Vec<_>>();
    assert_eq!(result.len(), 5);

    // Searching for a label, returns one results
    let mut formula = Formula::new();
    formula.extend(Clause::Atom(AtomClause::Label("/l/labelset/LABEL_0".into())));
    let result = segment
        .search(&query, &formula, true, 10, &CONFIG, -1.0)
        .collect::<Vec<_>>();
    assert_eq!(result.len(), 1);

    // Searching for a label prefix, returns no results
    let mut formula = Formula::new();
    formula.extend(Clause::Atom(AtomClause::Label("/l/labelset/LABEL".into())));
    let result = segment
        .search(&query, &formula, true, 10, &CONFIG, -1.0)
        .collect::<Vec<_>>();
    assert_eq!(result.len(), 0);
}

#[test]
fn fast_data_merge() -> VectorR<()> {
    let search_vectors = [create_query(), create_query(), create_query(), create_query()];

    let big_segment_dir = tempfile::tempdir()?;
    let mut elems: Vec<_> = (0..100)
        .map(|k| {
            Elem::new(
                format!("75a6eed3f94e456daa3f2d578a2254b7/t/trash/0-{k}"),
                create_query(),
                vec![],
                None,
            )
        })
        .collect();
    elems.push(Elem::new(
        "00000000000000000000000000000000/f/file/0-100".into(),
        search_vectors[0].clone(),
        vec![],
        None,
    ));
    elems.push(Elem::new(
        "00000000000000000000000000000001/f/file/0-100".into(),
        search_vectors[1].clone(),
        vec![],
        None,
    ));
    let mut big_segment = segment::create(big_segment_dir.path(), elems, &CONFIG, HashSet::new())?;

    let small_segment_dir = tempfile::tempdir()?;
    let mut small_segment = segment::create(
        small_segment_dir.path(),
        vec![
            Elem::new(
                "00000000000000000000000000000002/f/file/0-100".into(),
                search_vectors[2].clone(),
                vec![],
                None,
            ),
            Elem::new(
                "00000000000000000000000000000003/f/file/0-100".into(),
                search_vectors[3].clone(),
                vec![],
                None,
            ),
        ],
        &CONFIG,
        HashSet::new(),
    )?;

    // Merge without deletions
    let work = [&big_segment, &small_segment];
    let output_dir = tempfile::tempdir()?;
    let t = Instant::now();
    let dp = segment::merge(output_dir.path(), &work, &CONFIG)?;
    let fast_merge_time = t.elapsed();

    for (i, v) in search_vectors.iter().enumerate() {
        let formula = Formula::new();
        let result: Vec<_> = dp.search(v, &formula, true, 1, &CONFIG, 0.999).collect();
        assert_eq!(result.len(), 1);
        assert!(result[0].score() >= 0.999);
        assert!(
            dp.get_paragraph(result[0].paragraph()).id() == format!("0000000000000000000000000000000{i}/f/file/0-100")
        );
    }

    // Merge with deletions
    big_segment.apply_deletion("00000000000000000000000000000000/f/file/0-100");
    small_segment.apply_deletion("00000000000000000000000000000002/f/file/0-100");
    let work = [&big_segment, &small_segment];
    let output_dir = tempfile::tempdir()?;
    let t = Instant::now();
    let dp = segment::merge(output_dir.path(), &work, &CONFIG)?;
    let slow_merge_time = t.elapsed();

    for (i, v) in search_vectors.iter().enumerate() {
        let formula = Formula::new();
        let result: Vec<_> = dp.search(v, &formula, true, 1, &CONFIG, 0.999).collect();
        if i == 0 || i == 2 {
            // These were deleted, they should not be in the merge
            assert_eq!(result.len(), 0);
        } else {
            assert_eq!(result.len(), 1);
            assert!(result[0].score() >= 0.999);
            assert!(
                dp.get_paragraph(result[0].paragraph()).id()
                    == format!("0000000000000000000000000000000{i}/f/file/0-100")
            );
        }
    }

    // If there are deletions, we cannot reuse the HNSW, and we are much slower
    assert!(slow_merge_time.as_secs_f64() > 2.0 * fast_merge_time.as_secs_f64());

    Ok(())
}
