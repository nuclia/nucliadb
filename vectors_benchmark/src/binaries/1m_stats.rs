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

fn add_batch(writer: &mut Index, elems: Vec<(String, Vec<f32>)>, labels: Vec<String>) {
    let temporal_mark = TemporalMark::now();
    let similarity = Similarity::Cosine;
    let labels = LabelDictionary::new(labels);
    let elems = elems
        .into_iter()
        .map(|(key, vector)| Elem::new(key, vector, labels.clone(), None))
        .collect();
    let new_dp = DataPoint::new(writer.location(), elems, Some(temporal_mark), similarity).unwrap();
    let lock = writer.get_elock().unwrap();
    writer.add(new_dp, &lock).unwrap();
    writer.commit(lock).unwrap();
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
    let mut writer = Index::new(at.path(), IndexMetadata::default()).unwrap();
    for i in 0..(INDEX_SIZE / BATCH_SIZE) {
        let labels = label_set(i);
        let elems = RandomVectors::new(VECTOR_DIM)
            .take(BATCH_SIZE)
            .enumerate()
            .map(|(i, q)| (i.to_string(), q))
            .collect();
        possible_tag.push(AtomClause::label(labels[0].clone()));
        let now = SystemTime::now();
        add_batch(&mut writer, elems, labels);
        stats.writing_time += now.elapsed().unwrap().as_millis();
        println!("{} vectors included", BATCH_SIZE * i);
    }
    possible_tag.truncate(1);

    let reader = Index::open(at.path(), IndexCheck::None).unwrap();
    let lock = reader.get_slock().unwrap();
    let queries = possible_tag;

    println!("Unfiltered search..");
    let request = Request {
        filter: Formula::new(),
        vector: RandomVectors::new(VECTOR_DIM).next().unwrap(),
    };
    let now = SystemTime::now();
    reader.search(&request, &lock).unwrap();
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
    reader.search(&request, &lock).unwrap();
    stats.tagged_time += now.elapsed().unwrap().as_millis();
    std::mem::drop(lock);
    println!("Cleaning garbage..");
    let exclusive = writer.get_elock().unwrap();
    writer.collect_garbage(&exclusive).unwrap();
    std::mem::drop(exclusive);
    println!("Garbage cleaned");
    println!("{stats}");
}
