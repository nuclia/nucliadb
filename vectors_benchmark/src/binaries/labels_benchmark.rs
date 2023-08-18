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
use std::io::Write;
use std::iter::Iterator;
use std::time::Instant;
use std::{fs, mem};

use byte_unit::Byte;
use nucliadb_vectors::labels::{Label, LabelDictionary};
use nucliadb_vectors::VectorR;
use rand::distributions::Alphanumeric;
use rand::seq::index::sample;
use rand::Rng;
use serde_json::json;
use tempfile::{tempdir, TempDir};
use vectors_benchmark::cli_interface::*;
use vectors_benchmark::json_writer::write_json;

const NUM_LABELS: usize = 100_000;
const MIN_KEY_LEN: usize = 5;
const MAX_KEY_LEN: usize = 200;
const MIN_DOC_IDS_LEN: usize = 1;
const MAX_DOC_IDS_LEN: usize = 1000;

fn generate_random_key(min_len: usize, max_len: usize) -> String {
    let string_len = rand::thread_rng().gen_range(min_len..=max_len);
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(string_len)
        .map(char::from)
        .collect()
}

fn generate_random_doc_ids(min_len: usize, max_len: usize) -> Vec<u64> {
    let vec_len = rand::thread_rng().gen_range(min_len..=max_len);
    (0..vec_len).map(|_| rand::thread_rng().gen()).collect()
}

fn generate_labels(num_structs: usize) -> Vec<Label> {
    let mut structs = Vec::with_capacity(num_structs);
    for _ in 0..num_structs {
        let key = generate_random_key(MIN_KEY_LEN, MAX_KEY_LEN);
        let doc_ids = generate_random_doc_ids(MIN_DOC_IDS_LEN, MAX_DOC_IDS_LEN);
        structs.push(Label { key, doc_ids });
    }
    structs.sort_by_key(|s| s.key.clone());
    structs
}

fn search_benchmark(dir: &TempDir, keys: Vec<String>) -> VectorR<u128> {
    let labels_dict = LabelDictionary::open(dir.path())?;
    let mut total_elapsed_micros = 0;

    for (cycle, key) in keys.iter().enumerate() {
        print!("{} ", cycle);
        let _ = std::io::stdout().flush();
        let start_time = Instant::now();
        let _ = match labels_dict.get_label(key)? {
            None => {
                println!("Could not find `{}`", key);
                Err("Not found")
            }
            Some(_) => Ok(()),
        };
        total_elapsed_micros += start_time.elapsed().as_micros();
    }
    println!(" - OK.");
    Ok(total_elapsed_micros / keys.len() as u128)
}

fn indexing_benchmark(dir: &TempDir, random_labels: Vec<Label>) -> VectorR<u128> {
    let mut total_elapsed_ms = 0;
    for cycle in 0..20 {
        print!("{} ", cycle);
        let _ = std::io::stdout().flush();
        let start_time = Instant::now();
        let _ = LabelDictionary::new(dir.path(), random_labels.iter());
        total_elapsed_ms += start_time.elapsed().as_millis();
    }
    println!(" - OK.");
    Ok(total_elapsed_ms / 20)
}

fn main() -> VectorR<()> {
    let args = Args::new();
    println!("Generating {} random labels", NUM_LABELS);
    // generating random labels
    let random_labels = generate_labels(NUM_LABELS);

    let total_memory = random_labels.iter().fold(0, |acc, label| {
        let element_size = mem::size_of_val(label);
        let vec_size = label.doc_ids.len() * mem::size_of::<u64>();
        acc + element_size + vec_size
    });

    println!(
        "Total memory taken by the labels is {}",
        Byte::from_bytes(total_memory as u128).get_appropriate_unit(true),
    );

    // picking 20 random label keys for search
    let mut rng = rand::thread_rng();
    let indices: Vec<usize> = sample(&mut rng, NUM_LABELS, 20).into_vec();
    let random_keys: Vec<String> = indices
        .iter()
        .map(|&index| random_labels[index].key.clone())
        .collect();

    let label_dir = tempdir().unwrap();

    println!("Indexing {} labels in FST", NUM_LABELS);
    let index_time = indexing_benchmark(&label_dir, random_labels)?;
    println!("Indexing average time: {} ms", index_time);

    let fst_size = fs::metadata(label_dir.path().join("labels.fst"))
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    println!(
        "Size of `labels.fst` is {}",
        Byte::from_bytes(fst_size as u128).get_appropriate_unit(true),
    );

    let idx_size = fs::metadata(label_dir.path().join("labels.idx"))
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    println!(
        "Size of `labels.idx` is {}",
        Byte::from_bytes(idx_size as u128).get_appropriate_unit(true),
    );

    // now let's search for our key
    println!("Search benchmark");
    let search_time = search_benchmark(&label_dir, random_keys)?;
    println!("Searching average time : {} µs", search_time,);

    // write output for the CI
    let json_results = vec![
        json!({
                "name": "Labels Index Time",
                "unit": "ms",
                "value": index_time,
            }
        ),
        json!({
                "name": "Labels Search Time",
                "unit": "µs",
                "value": search_time,
            }
        ),
    ];

    write_json(args.json_output(), json_results)?;

    Ok(())
}
