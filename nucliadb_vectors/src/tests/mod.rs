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
    let reader = Reader::new(temp_dir.path().to_str().unwrap());
    let mut labels = vec![];
    for i in 0..50 {
        labels.push(format!("LABEL_{}", i));
    }
    let mut delete = vec![];
    for i in 0..100 {
        let key = format!("KEY_{}", i);
        let vec = vec![rand::random::<f32>(); 8];
        if rand::random::<usize>() % 2 == 0 {
            delete.push(key.clone());
        }
        writer.insert(key, vec, labels.clone());
        writer.flush();
        reader.reload();
    }
    for delete in delete {
        writer.delete_vector(delete.clone());
        writer.flush();
        reader.reload();
    }
    writer.flush();
    for i in 0..50 {
        let key = format!("KEY_{}", i + 50);
        let vec = vec![rand::random::<f32>(); 8];
        writer.insert(key, vec, labels.clone());
        writer.flush();
        reader.reload();
    }
    writer.flush();
    std::mem::drop(writer);
    reader.reload();
    let query = vec![rand::random::<f32>(); 8];
    let no_results = 10;
    let result = reader.search(query, labels[..20].to_vec(), no_results);
    assert_eq!(result.len(), no_results);
}

#[test]
fn insert_delete_all() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut writer = Writer::new(temp_dir.path().to_str().unwrap());
    for i in 0..50 {
        let key = format!("KEY_{}", i);
        let vec = vec![rand::random::<f32>(); 8];
        writer.insert(key, vec, vec![]);
    }
    writer.flush();
    assert_eq!(writer.no_vectors(), 50);
    writer.delete_document("KEY".to_string());
    assert_eq!(writer.no_vectors(), 0);
}

fn _concurrency_test() {
    fn reader_process(reader: Reader, _: Arc<Mutex<()>>) {
        loop {
            let query = vec![rand::random::<f32>(); 8];
            let no_results = 10;
            // let l = lock.lock().unwrap();
            let result = reader.search(query, vec![], no_results);
            println!("READ {}", result.len());
            // std::mem::drop(l);
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
            for _ in 0..100 {
                let key = format!("KEY_{}", current_key);
                let vec = vec![rand::random::<f32>(); 8];
                if rand::random::<usize>() % 2 == 0 {
                    delete.push(key.clone());
                }
                // let l = lock.lock().unwrap();
                writer.insert(key.clone(), vec, labels.clone());
                writer.flush();
                println!("INSERT {key}");
                // std::mem::drop(l);

                current_key += 1;
            }
            for delete in delete {
                // let l = lock.lock().unwrap();
                writer.delete_vector(delete.clone());
                writer.flush();
                println!("DELETE {delete}");
                // std::mem::drop(l);
            }
        }
    }
    let lock = Arc::new(Mutex::new(()));
    let temp_dir = tempfile::tempdir().unwrap();
    let writer = Writer::new(temp_dir.path().to_str().unwrap());
    let reader = Reader::new(temp_dir.path().to_str().unwrap());
    let reader_lock = lock.clone();
    let writer_lock = lock.clone();
    let rp = std::thread::spawn(move || reader_process(reader, reader_lock));
    let wp = std::thread::spawn(move || writer_process(writer, writer_lock));
    rp.join().unwrap();
    wp.join().unwrap();
}
