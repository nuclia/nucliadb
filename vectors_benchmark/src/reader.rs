use super::plot_writer::PlotWriter;
use super::query_iter::QueryIter;
use super::VectorEngine;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, Duration};

pub fn read_benchmark<Eng>(
    stop_point: Arc<AtomicBool>,
    no_results: usize,
    engine: Eng,
    mut plotw: PlotWriter,
    queries: QueryIter,
) where
    Eng: VectorEngine,
{
    let mut iter = queries.enumerate();
    while !stop_point.load(Ordering::SeqCst) {
        let (x, query) = iter.next().unwrap();
        let now = SystemTime::now();
        engine.search(no_results, &query);
        let tick = now.elapsed().unwrap().as_millis();
        plotw.add(x, tick).unwrap();
        std::thread::sleep(Duration::from_millis(100));
    }
}
