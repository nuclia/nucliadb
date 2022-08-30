use super::plot_writer::PlotWriter;
use super::vector_iter::VectorIter;
use super::VectorEngine;
use std::io::Read;
use std::time::SystemTime;

pub fn write_benchmark<Eng, Cnt>(
    batch_size: usize,
    mut engine: Eng,
    mut plotw: PlotWriter,
    vectors: VectorIter<Cnt>,
) where
    Eng: VectorEngine,
    Cnt: Read,
{
    let mut kbatch = vec![];
    let mut vbatch = vec![];
    let mut batch_num = 0;
    let mut batch_id = format!("Batch{batch_num}");
    for (x, vector) in vectors.enumerate() {
        kbatch.push(format!("{batch_id}/{x}"));
        vbatch.push(vector);
        if vbatch.len() == batch_size {
            let now = SystemTime::now();
            engine.add_batch(batch_id, kbatch, vbatch);
            let tick = now.elapsed().unwrap().as_millis();
            plotw.add(x, tick).unwrap();
            batch_num += 1;
            batch_id = format!("Batch{batch_num}");
            kbatch = vec![];
            vbatch = vec![];
        }
    }
}
