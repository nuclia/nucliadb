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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

use nucliadb_vectors::data_point_provider::{Index, IndexCheck};
use vectors_benchmark::cli_interface::*;
fn main() {
    let args = Args::new();
    let stop_point = Arc::new(AtomicBool::new(false));
    let at = tempfile::TempDir::new().unwrap();
    let writer = Index::new(at.path(), IndexCheck::None).unwrap();
    let batch_size = args.batch_size();
    let plotw = PlotWriter::new(args.writer_plot().unwrap());
    let vector_it = RandomVectors::new(args.embedding_dim()).take(args.index_len());
    let writer_handler =
        thread::spawn(move || writer::write_benchmark(batch_size, writer, plotw, vector_it));

    let stop = stop_point.clone();
    let reader = Index::new(at.path(), IndexCheck::None).unwrap();
    let no_results = args.neighbours();
    let plotw = PlotWriter::new(args.reader_plot().unwrap());
    let query_it = RandomVectors::new(args.embedding_dim());
    let reader_handler =
        thread::spawn(move || reader::read_benchmark(stop, no_results, reader, plotw, query_it));

    writer_handler.join().unwrap();
    stop_point.store(true, Ordering::SeqCst);
    reader_handler.join().unwrap();
}
