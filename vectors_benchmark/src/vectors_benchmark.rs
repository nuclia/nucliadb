use nucliadb_vectors2::data_point_provider::Index;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use vectors_benchmark::cli_interface::*;
fn main() {
    let stop_point = Arc::new(AtomicBool::new(false));
    let at = tempfile::TempDir::new().unwrap();
    let args = Args::new();
    let writer = Index::writer(at.path()).unwrap();
    let batch_size = args.batch_size();
    let plotw = PlotWriter::new(args.writer_plot().unwrap());
    let vector_it = VectorIter::new(args.vectors().unwrap());
    let writer_handler =
        thread::spawn(move || writer::write_benchmark(batch_size, writer, plotw, vector_it));

    let stop = stop_point.clone();
    let reader = Index::reader(at.path()).unwrap();
    let no_results = args.neighbours();
    let plotw = PlotWriter::new(args.reader_plot().unwrap());
    let query_it = QueryIter::new(args.embedding_dim());
    let reader_handler = thread::spawn(move || {
        reader::read_benchmark::<Index>(stop, no_results, reader, plotw, query_it)
    });

    writer_handler.join().unwrap();
    stop_point.store(true, Ordering::SeqCst);
    reader_handler.join().unwrap();
}
