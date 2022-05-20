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

use clap::Parser;
use nucliadb_vectors::writer::Writer;
use tempfile::*;

/// Vectors Structural Integrity Check
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Path to an existing index (empty = create temporal index)
    #[clap(short, long)]
    path: Option<String>,
    /// Path where the output should be located
    #[clap(short, long, default_value_t = String::from("./"))]
    output: String,
    /// Dimension of the vectors to be inserted.
    /// Only revelant for temporal indexes.
    #[clap(short, long, default_value_t = 178)]
    dim: usize,
}

fn create_query(dim: usize) -> Vec<f32> {
    vec![rand::random::<f32>; dim]
        .into_iter()
        .map(|f| f())
        .collect()
}

fn analyze_path(path: &str) -> Option<std::path::PathBuf> {
    use std::path::*;
    let path = PathBuf::from(&path).canonicalize().unwrap();
    if path.exists() {
        Some(path)
    } else {
        None
    }
}

fn writer_process(temp_dir: &TempDir, args: &Args) {
    println!("Creating the index, this will take some minutes. Maybe a cup of coffee?");
    let mut writer = Writer::new(temp_dir.path().to_str().unwrap());
    let mut current_key = 0;
    let mut labels = vec![];
    for i in 0..50 {
        labels.push(format!("LABEL_{}", i));
    }
    let time = std::time::SystemTime::now();
    while time.elapsed().unwrap().as_secs() < 180 {
        let mut delete = vec![];
        for _ in 0..50 {
            let key = format!("KEY_{}", current_key);
            let vec = create_query(args.dim);
            if rand::random::<usize>() % 2 == 0 {
                delete.push(key.clone());
            }
            writer.insert(key.clone(), vec, labels.clone());

            current_key += 1;
        }
        for delete in delete {
            writer.delete_vector(delete.clone());
        }
        writer.commit();
        writer.run_garbage_collection();
    }
    let no_nodes = writer.stats().nodes_in_total;
    println!("Index created, there are {no_nodes} nodes");
}

fn main() {
    let args = Args::parse();
    let ouput = analyze_path(&args.output);
    match (&args.path, ouput) {
        (Some(path), Some(output)) => match analyze_path(path) {
            Some(path) => nucliadb_vectors::sic::checks(path.as_path(), output.as_path()),
            None => println!("Invalid path {path}"),
        },
        (None, Some(output)) => {
            let temp_dir = tempfile::tempdir().unwrap();
            writer_process(&temp_dir, &args);
            nucliadb_vectors::sic::checks(temp_dir.path(), output.as_path());
        }
        _ => println!("aborting due to invalid arguments"),
    }
}
