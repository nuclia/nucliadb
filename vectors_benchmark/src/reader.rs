use super::plot_writer::PlotWriter;
use super::vector_iter::VectorIter;
use super::VectorEngine;
use std::io::Read;
use std::time::SystemTime;

pub fn read_benchmark<Eng, Cnt>(
    no_results: usize,
    engine: Eng,
    mut plotw: PlotWriter,
    queries: VectorIter<Cnt>,
) where
    Eng: VectorEngine,
    Cnt: Read,
{
    for (x, query) in queries.enumerate() {
        let now = SystemTime::now();
        engine.search(no_results, &query);
        let tick = now.elapsed().unwrap().as_millis();
        plotw.add(x, tick).unwrap();
    }
}
