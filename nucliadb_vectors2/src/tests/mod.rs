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

use crate::index::{Batch, Index};

#[test]
fn simple_flow() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut writer = Index::new(temp_dir.path()).unwrap();
    let reader = Index::new(temp_dir.path()).unwrap();
    let mut batch = Batch::new();
    let mut labels = vec![];
    for i in 0..50 {
        labels.push(format!("LABEL_{}", i));
    }
    let mut delete = vec![];
    for i in 0..50 {
        let key = format!("KEY_{}", i);
        let vec = vec![rand::random::<f32>(); 8];
        if rand::random::<usize>() % 2 == 0 {
            delete.push(key.clone());
        }
        batch.add_vector(key, vec, labels.clone());
    }
    for delete in &delete {
        // Deletions must be already in the index
        // These deletions will not be effective.
        batch.rmv_vector(delete.clone());
    }
    writer.write(batch).unwrap();

    let mut batch = Batch::new();
    for delete in &delete {
        // These deletions will be effective.
        batch.rmv_vector(delete.clone());
    }
    writer.write(batch).unwrap();

    let query = vec![rand::random::<f32>(); 8];
    let no_results = 10;
    let result = reader.search(no_results, &query, &labels[..20]).unwrap();
    assert_eq!(result.len(), no_results);
}

#[test]
fn accuracy_test() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut writer = Index::new(temp_dir.path()).unwrap();
    let reader = Index::new(temp_dir.path()).unwrap();
    let mut labels = vec![];
    for i in 0..50 {
        labels.push(format!("LABEL_{}", i));
    }
    let mut batch = Batch::new();
    for i in 0..100 {
        let key = format!("KEY_{}", i);
        let vec = create_query();
        batch.add_vector(key, vec, labels.clone());
    }
    writer.write(batch).unwrap();
    std::mem::drop(writer);
    let query = create_query();
    println!("QUERY 0: {:?}", query);
    let no_results = 10;
    let result_0 = reader.search(no_results, &query, &labels[..20]).unwrap();
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
    let result_1 = reader.search(no_results, &query, &labels[..20]).unwrap();
    let mut result_1: Vec<_> = result_1.into_iter().map(|(k, _)| k).collect();
    result_1.sort();
    println!("RESULT0: {:?}", result_0);
    println!("RESULT1: {:?}", result_1);
    assert_ne!(result_0, result_1)
}


#[test]
fn single_graph() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut writer = Index::new(temp_dir.path()).unwrap();
    let reader = Index::new(temp_dir.path()).unwrap();
    let key = "KEY_0".to_string();
    let vec = create_query();

    let mut batch = Batch::new();
    batch.add_vector(key.clone(), vec.clone(), vec![]);
    writer.write(batch).unwrap();
    let result = reader.search(5, &vec, &[]).unwrap();
    assert_eq!(result.len(), 1);
    assert!(result[0].1 >= 0.9);
    assert!(result[0].0 >= key);

    let mut batch = Batch::new();
    batch.rmv_vector(key.clone());
    writer.write(batch).unwrap();
    let result = reader.search(5, &vec, &[]).unwrap();
    assert_eq!(result.len(), 0);

    let mut batch = Batch::new();
    batch.add_vector(key.clone(), vec.clone(), vec![]);
    writer.write(batch).unwrap();
    let result = reader.search(5, &vec, &[]).unwrap();
    assert_eq!(result.len(), 1);
    assert!(result[0].1 >= 0.9);
    assert!(result[0].0 >= key);
}

fn create_query() -> Vec<f32> {
    vec![rand::random::<f32>; 178]
        .into_iter()
        .map(|f| f())
        .collect()
}

// #[test]
#[allow(unused)]
fn stress_test() {
    use std::time::{Duration, SystemTime};
    let temp_dir = tempfile::tempdir().unwrap();
    let mut writer = Index::new(temp_dir.path()).unwrap();
    let reader = Index::new(temp_dir.path()).unwrap();
    let mut total = Duration::from_secs(0);
    let mut labels = vec![];
    for i in 0..50 {
        labels.push(format!("LABEL_{}", i));
    }

    let mut batch = Batch::new();
    for current_key in 0..1000 {
        let key = format!("KEY_{}", current_key);
        let vec = create_query();
        let query_labels = labels.clone();
        batch.add_vector(key.clone(), vec, query_labels);
    }

    let timer = SystemTime::now();
    println!("INSERTION");
    writer.write(batch).unwrap();
    total += timer.elapsed().unwrap();
    for index in 0..1000 {
        let query = create_query();
        let no_results = 5;
        let query_labels = labels.clone();
        let timer = SystemTime::now();
        let result = reader.search(no_results, &query, &query_labels).unwrap();
        let set: std::collections::HashSet<_> = result.iter().map(|(k, v)| k.clone()).collect();
        assert!(result.len() == no_results);
        assert_eq!(result.len(), set.len());
        total += timer.elapsed().unwrap();
        println!("READ {:?}", result);
    }

    println!("Took: {}", total.as_secs());
}

//#[test]
#[allow(unused)]
fn concurrency_test() {
    fn reader_process(reader: Index) {
        let mut index = 1;
        loop {
            let query = create_query();
            let no_results = 5;
            let result = reader.search(no_results, &query, &[]).unwrap();
            println!("READ {:?}", result);
            std::thread::sleep(std::time::Duration::from_millis(10));
            index += 1;
        }
    }

    fn writer_process(mut writer: Index) {
        let mut current_key = 0;
        let mut labels = vec![];
        for i in 0..50 {
            labels.push(format!("LABEL_{}", i));
        }
        loop {
            let mut delete = vec![];
            let mut batch = Batch::new();
            for _ in 0..50 {
                let key = format!("KEY_{}", current_key);
                let vec = create_query();
                if rand::random::<usize>() % 2 == 0 {
                    delete.push(key.clone());
                }
                batch.add_vector(key.clone(), vec, labels.clone());
                current_key += 1;
            }
            println!("INSERT");
            writer.write(batch).unwrap();

            let mut batch = Batch::new();
            for delete in delete {
                batch.rmv_vector(delete.clone());
            }
            println!("DELETE");
            writer.write(batch).unwrap();

        }
    }

    let temp_dir = tempfile::tempdir().unwrap();
    let mut writer = Index::new(temp_dir.path()).unwrap();
    let reader = Index::new(temp_dir.path()).unwrap();
    let rp = std::thread::spawn(move || reader_process(reader));
    let wp = std::thread::spawn(move || writer_process(writer));
    rp.join().unwrap();
    wp.join().unwrap();
}
