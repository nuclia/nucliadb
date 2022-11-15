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

use std::sync::{Arc, Mutex};

use crate::reader::Reader;
use crate::writer::Writer;

#[test]
fn simple_flow() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut writer = Writer::new(temp_dir.path().to_str().unwrap());
    let mut reader = Reader::new(temp_dir.path().to_str().unwrap());
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
        writer.insert(key, vec, labels.clone());
    }
    for delete in delete {
        writer.delete_vector(delete.clone());
    }
    writer.commit();
    for i in 0..50 {
        let key = format!("KEY_{}", i + 50);
        let vec = vec![rand::random::<f32>(); 8];
        writer.insert(key, vec, labels.clone());
    }
    writer.commit();
    reader.reload();
    let query = vec![rand::random::<f32>(); 8];
    let no_results = 10;
    let result = reader.search(query, labels[..20].to_vec(), no_results);
    assert_eq!(result.len(), no_results);
}

#[test]
fn accuracy_test() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut writer = Writer::new(temp_dir.path().to_str().unwrap());
    let mut reader = Reader::new(temp_dir.path().to_str().unwrap());
    let mut labels = vec![];
    for i in 0..50 {
        labels.push(format!("LABEL_{}", i));
    }
    for i in 0..100 {
        let key = format!("KEY_{}", i);
        let vec = create_query();
        writer.insert(key, vec, labels.clone());
    }
    writer.commit();
    reader.reload();
    std::mem::drop(writer);
    reader.reload();
    let query = create_query();
    println!("QUERY 0: {:?}", query);
    let no_results = 10;
    let result_0 = reader.search(query, labels[..20].to_vec(), no_results);
    let mut result_0: Vec<_> = result_0.into_iter().map(|(k, _)| k).collect();
    result_0.sort();
    let query = create_query();
    println!("QUERY 1: {:?}", query);
    let no_results = 10;
    let result_1 = reader.search(query, labels[..20].to_vec(), no_results);
    let mut result_1: Vec<_> = result_1.into_iter().map(|(k, _)| k).collect();
    result_1.sort();
    println!("RESULT0: {:?}", result_0);
    println!("RESULT1: {:?}", result_1);
    assert_ne!(result_0, result_1)
}

#[test]
fn insert_delete_all() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut writer = Writer::new(temp_dir.path().to_str().unwrap());
    for i in 0..50 {
        let key = format!("KEY_{}", i);
        let vec = create_query();
        writer.insert(key, vec, vec![]);
    }
    writer.commit();
    assert_eq!(writer.no_vectors(), 50);
    writer.delete_document("KEY".to_string());
    assert_eq!(writer.no_vectors(), 0);
}

#[test]
fn single_graph() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut writer = Writer::new(temp_dir.path().to_str().unwrap());
    let mut reader = Reader::new(temp_dir.path().to_str().unwrap());
    let key = "KEY_0".to_string();
    let vec = create_query();
    writer.insert(key.clone(), vec.clone(), vec![]);
    writer.commit();
    writer.delete_vector(key.clone());
    writer.commit();
    writer.insert(key, vec.clone(), vec![]);
    writer.commit();
    assert_eq!(writer.no_vectors(), 1);
    assert_eq!(reader.no_vectors(), 0);
    reader.reload();
    assert_eq!(reader.no_vectors(), 1);
    let result = reader.search(vec, vec![], 5);
    assert_eq!(result.len(), 1);
    assert!(result[0].1 >= 0.9);
}

fn create_query() -> Vec<f32> {
    vec![rand::random::<f32>; 178]
        .into_iter()
        .map(|f| f())
        .collect()
}

//#[test]
#[allow(unused)]
fn stress_test() {
    use std::time::{Duration, SystemTime};
    let temp_dir = tempfile::tempdir().unwrap();
    let mut writer = Writer::new(temp_dir.path().to_str().unwrap());
    let mut reader = Reader::new(temp_dir.path().to_str().unwrap());
    let mut total = Duration::from_secs(0);
    let mut labels = vec![];
    for i in 0..50 {
        labels.push(format!("LABEL_{}", i));
    }
    for current_key in 0..1000 {
        let key = format!("KEY_{}", current_key);
        let vec = create_query();
        let query_labels = labels.clone();
        let timer = SystemTime::now();
        writer.insert(key.clone(), vec, query_labels);
        total += timer.elapsed().unwrap();
        println!("INSERT {key}");
    }
    let timer = SystemTime::now();
    let stats = writer.stats();
    writer.commit();
    reader.reload();
    total += timer.elapsed().unwrap();
    for index in 0..1000 {
        let query = create_query();
        let no_results = 5;
        let query_labels = labels.clone();
        let timer = SystemTime::now();
        let result = reader.search(query, query_labels, no_results);
        let set: std::collections::HashSet<_> = result.iter().map(|(k, v)| k.clone()).collect();
        assert!(result.len() == no_results);
        assert_eq!(result.len(), set.len());
        total += timer.elapsed().unwrap();
        println!("READ {:?}", result);
        if index % 100 == 0 {
            let timer = SystemTime::now();
            reader.reload();
            total += timer.elapsed().unwrap();
        }
    }
    println!("Took: {}", total.as_secs());
    println!("Reader: {:?}", reader.stats());
    println!("Writer: {:?}", writer.stats());
}

//#[test]
#[allow(unused)]
fn concurrency_test() {
    fn reader_process(mut reader: Reader, _: Arc<Mutex<()>>) {
        let mut index = 1;
        loop {
            let query = create_query();
            let no_results = 5;
            let result = reader.search(query, vec![], no_results);
            println!("READ {:?}", result);
            std::thread::sleep(std::time::Duration::from_millis(10));
            reader.reload();
            index += 1;
        }
    }

    fn writer_process(mut writer: Writer, _: Arc<Mutex<()>>) {
        let mut current_key = 0;
        let mut labels = vec![];
        for i in 0..50 {
            labels.push(format!("LABEL_{}", i));
        }
        loop {
            let mut delete = vec![];
            for _ in 0..50 {
                let key = format!("KEY_{}", current_key);
                let vec = create_query();
                if rand::random::<usize>() % 2 == 0 {
                    delete.push(key.clone());
                }
                writer.insert(key.clone(), vec, labels.clone());
                println!("INSERT {key}");

                current_key += 1;
            }
            for delete in delete {
                writer.delete_vector(delete.clone());
                println!("DELETE {delete}");
            }
            writer.commit();
        }
    }
    let lock = Arc::new(Mutex::new(()));
    let temp_dir = tempfile::tempdir().unwrap();
    let writer = Writer::new(temp_dir.path().to_str().unwrap());
    let reader = Reader::new(temp_dir.path().to_str().unwrap());
    let reader_lock = lock.clone();
    let writer_lock = lock;
    let rp = std::thread::spawn(move || reader_process(reader, reader_lock));
    let wp = std::thread::spawn(move || writer_process(writer, writer_lock));
    rp.join().unwrap();
    wp.join().unwrap();
}
