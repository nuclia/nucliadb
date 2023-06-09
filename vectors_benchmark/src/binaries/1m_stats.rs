use std::time::SystemTime;

use nucliadb_vectors::data_point::{DataPoint, Elem, LabelDictionary, Similarity};
use nucliadb_vectors::data_point_provider::*;
use nucliadb_vectors::formula::*;
use vectors_benchmark::random_vectors::RandomVectors;
use vectors_benchmark::stats::Stats;

const BATCH_SIZE: usize = 5000;
const NO_LABELS: usize = 5;
const NO_NEIGHBOURS: usize = 5;
const INDEX_SIZE: usize = 1000000;
const VECTOR_DIM: usize = 128;

struct Request {
    vector: Vec<f32>,
    filter: Formula,
}
impl SearchRequest for Request {
    fn with_duplicates(&self) -> bool {
        true
    }
    fn get_query(&self) -> &[f32] {
        &self.vector
    }

    fn get_filter(&self) -> &Formula {
        &self.filter
    }

    fn no_results(&self) -> usize {
        NO_NEIGHBOURS
    }
    fn min_score(&self) -> f32 {
        -1.0
    }
}

fn label_set(batch_id: usize) -> Vec<String> {
    (0..NO_LABELS).map(|l| format!("L{batch_id}_{l}")).collect()
}

fn add_batch(writer: &mut Writer, elems: Vec<(String, Vec<f32>)>, labels: Vec<String>) {
    let temporal_mark = TemporalMark::now();
    let location = writer.index().location();
    let similarity = Similarity::Cosine;
    let labels = LabelDictionary::new(labels);
    let elems = elems
        .into_iter()
        .map(|(key, vector)| Elem::new(key, vector, labels.clone(), None))
        .collect();
    let new_dp = DataPoint::new(location, elems, Some(temporal_mark), similarity).unwrap();
    writer.add(new_dp);
    writer.commit().unwrap();
}
fn main() {
    let _ = Merger::install_global().map(std::thread::spawn);
    let at = tempfile::TempDir::new().unwrap();
    let mut stats = Stats {
        writing_time: 0,
        read_time: 0,
        tagged_time: 0,
    };
    println!("Writing starts..");
    let mut possible_tag = vec![];
    let index = Index::new(at.path(), IndexMetadata::default()).unwrap();

    let mut writer = index.writer().unwrap();
    for i in 0..(INDEX_SIZE / BATCH_SIZE) {
        let labels = label_set(i);
        let elems = RandomVectors::new(VECTOR_DIM)
            .take(BATCH_SIZE)
            .enumerate()
            .map(|(i, q)| (i.to_string(), q))
            .collect();
        possible_tag.push(LabelClause::new(labels[0].clone()));
        let now = SystemTime::now();
        add_batch(&mut writer, elems, labels);
        stats.writing_time += now.elapsed().unwrap().as_millis();
        println!("{} vectors included", BATCH_SIZE * i);
    }
    possible_tag.truncate(1);

    let reader = index.reader().unwrap();
    let queries = possible_tag;

    println!("Unfiltered search..");
    let request = Request {
        filter: Formula::new(),
        vector: RandomVectors::new(VECTOR_DIM).next().unwrap(),
    };
    let now = SystemTime::now();
    reader.search(&request).unwrap();
    stats.read_time += now.elapsed().unwrap().as_millis();
    let formula = queries.into_iter().fold(Formula::new(), |mut acc, i| {
        acc.extend(i);
        acc
    });
    println!("Filtered search..");
    let request = Request {
        filter: formula,
        vector: RandomVectors::new(VECTOR_DIM).next().unwrap(),
    };
    let now = SystemTime::now();
    reader.search(&request).unwrap();
    stats.tagged_time += now.elapsed().unwrap().as_millis();
    println!("Cleaning garbage..");
    writer.collect_garbage().unwrap();
    println!("Garbage cleaned");
    println!("{stats}");
}
