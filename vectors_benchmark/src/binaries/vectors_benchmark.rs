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
use std::error::Error;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::{fs, thread};

use byte_unit::Byte;
use nucliadb_vectors::data_point_provider::{Index, IndexMetadata, Merger};
use serde_json::json;
use vectors_benchmark::cli_interface::*;
use vectors_benchmark::json_writer::write_json;

fn dir_size(path: &Path) -> Result<Byte, Box<dyn Error>> {
    let mut total_size: u128 = 0;

    if path.is_dir() {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let entry_path = entry.path();

            if entry_path.is_dir() {
                match dir_size(&entry_path) {
                    Ok(size) => total_size += u128::from(size),
                    Err(err) => {
                        eprintln!("Error calculating directory size: {}", err);
                        return Ok(Byte::from_bytes(0));
                    }
                }
            } else {
                match entry.metadata() {
                    Ok(metadata) => total_size += metadata.len() as u128,
                    Err(err) => {
                        eprintln!("Error getting file metadata: {}", err);
                        return Ok(Byte::from_bytes(0));
                    }
                }
            }
        }
    }

    Ok(Byte::from_bytes(total_size))
}

fn main() -> std::io::Result<()> {
    let _ = Merger::install_global().map(std::thread::spawn);
    let args = Args::new();
    let stop_point = Arc::new(AtomicBool::new(false));
    let at = tempfile::TempDir::new().unwrap();
    let location = at.path().join("vectors");
    println!("Vector location: {:?}", location);

    let writer = Index::new(&location, IndexMetadata::default()).unwrap();
    let batch_size = args.batch_size();
    let plotw = PlotWriter::new(args.writer_plot().unwrap());
    let vector_it = RandomVectors::new(args.embedding_dim()).take(args.index_len());
    let writer_handler = thread::spawn(move || writer::write_benchmark(batch_size, writer, plotw, vector_it));

    let stop = stop_point.clone();
    let reader = Index::open(&location, false).unwrap();
    let no_results = args.neighbours();
    let plotw = PlotWriter::new(args.reader_plot().unwrap());
    let query_it = RandomVectors::new(args.embedding_dim());
    let reader_handler = thread::spawn(move || reader::read_benchmark(stop, no_results, reader, plotw, query_it));

    writer_handler.join().unwrap();
    stop_point.store(true, Ordering::SeqCst);
    reader_handler.join().unwrap();

    let storage_size = dir_size(location.as_ref()).unwrap();

    let json_results = vec![json!({
        "name": "Total Storage Size",
        "unit": "bytes",
        "value": storage_size.get_bytes(),
    })];

    write_json(args.json_output(), json_results, args.merge).unwrap();

    println!("Total vector storage size: {}", storage_size.get_appropriate_unit(true));
    Ok(())
}
