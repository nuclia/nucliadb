use std::io::Write;
use std::path::Path;

use clap::Parser;
use nucliadb_core::Channel;
use nucliadb_vectors::data_point::{DataPoint, Elem, LabelDictionary, Similarity};
use nucliadb_vectors::data_point_provider::*;
use nucliadb_vectors::formula::*;
use rand::seq::SliceRandom;
use rand::Rng;
use serde_json::json;
use vectors_benchmark::json_writer::write_json;
use vectors_benchmark::random_vectors::RandomVectors;
use vectors_benchmark::stats::Stats;

const BATCH_SIZE: usize = 5000;
const NO_NEIGHBOURS: usize = 5;
const INDEX_SIZE: usize = 1_000_000;
const VECTOR_DIM: usize = 128;
const LABELS: [&str; 5] = [
    "nucliad_db_has_label_1",
    "nucliad_db_has_label_2",
    "nucliad_db_has_label_3",
    "nucliad_db_has_label_4",
    "nucliad_db_has_label_5",
];
const CYCLES: usize = 25;

macro_rules! measure_time {
    ( seconds $code:block ) => {{
        let start_time = std::time::Instant::now();
        let result = $code;
        let elapsed_time = start_time.elapsed().as_secs_f64();
        (result, elapsed_time)
    }};
    ( milliseconds $code:block ) => {{
        let start_time = std::time::Instant::now();
        let result = $code;
        let elapsed_time = start_time.elapsed().as_millis() as f64;
        (result, elapsed_time)
    }};
    ( microseconds $code:block ) => {{
        let start_time = std::time::Instant::now();
        let result = $code;
        let elapsed_time = start_time.elapsed().as_micros() as f64;
        (result, elapsed_time)
    }};
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Args {
    /// Size of index
    #[clap(short, long, default_value_t = INDEX_SIZE)]
    index_size: usize,
    /// Size of batch
    #[clap(short, long, default_value_t = BATCH_SIZE)]
    batch_size: usize,
    /// Number of search cycles
    #[clap(short, long, default_value_t = CYCLES)]
    cycles: usize,
    /// Path of the json output file
    #[clap(short, long, default_value_t = String::from("./benchmark.json"))]
    json_output: String,
    /// Merge results if json output file exists
    #[clap(short, long, action)]
    merge: bool,
}

impl Default for Args {
    fn default() -> Self {
        Args::new()
    }
}
impl Args {
    pub fn new() -> Args {
        Args::parse()
    }
}

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

fn random_labels(batch_no: usize, key: String, index_size: usize) -> LabelDictionary {
    let index = key.parse::<usize>().unwrap() + batch_no;

    let seventy_percent = (index_size as f64 * 0.7) as usize;
    let mut labels: Vec<String> = vec![];

    // the first 70% gets label 1, the remaining 30% label 2
    if index < seventy_percent {
        labels.push(LABELS[0].to_string());
    } else {
        labels.push(LABELS[1].to_string());
    }

    // and we randomly pick more labels
    let mut rng = rand::thread_rng();
    let num_elements = rng.gen_range(0..=3);

    for _ in 0..num_elements {
        let random_index = rng.gen_range(2..LABELS.len());
        labels.push(LABELS[random_index].to_string());
    }

    LabelDictionary::new(labels)
}

fn add_batch(
    batch_no: usize,
    writer: &mut Index,
    elems: Vec<(String, Vec<f32>)>,
    index_size: usize,
) {
    let temporal_mark = TemporalMark::now();
    let similarity = Similarity::Cosine;
    let elems = elems
        .into_iter()
        .map(|(key, vector)| {
            Elem::new(
                key.clone(),
                vector,
                random_labels(batch_no, key.clone(), index_size),
                None,
            )
        })
        .collect();

    let new_dp = DataPoint::new(
        writer.location(),
        elems,
        Some(temporal_mark),
        similarity,
        Channel::EXPERIMENTAL,
    )
    .unwrap();
    let mut tx = writer.transaction();
    tx.add_segment(new_dp.journal());
    writer.commit(tx).unwrap();
}

fn vector_random_subset<T: Clone>(vector: Vec<T>) -> Vec<T> {
    let mut rng = rand::thread_rng();
    let size = vector.len();
    let subset_size = rng.gen_range(1..=size);
    let mut random_indices: Vec<usize> = (0..size).collect();
    random_indices.shuffle(&mut rng);
    random_indices.truncate(subset_size);
    random_indices
        .iter()
        .map(|&index| vector[index].clone())
        .collect()
}

fn generate_vecs(count: usize) -> Vec<RandomVectors> {
    let mut res = vec![];
    for _ in 0..count {
        res.push(RandomVectors::new(VECTOR_DIM));
    }
    res
}

fn create_db(
    db_location: &Path,
    index_size: usize,
    batch_size: usize,
    vecs: &[RandomVectors],
) -> f64 {
    println!("Writing starts..");
    let mut writer = Index::new(db_location, IndexMetadata::default()).unwrap();
    let mut writing_time: f64 = 0.0;
    for (i, vec) in vecs.iter().enumerate().take(index_size / batch_size) {
        let elems = vec
            .take(batch_size)
            .enumerate()
            .map(|(i, q)| (i.to_string(), q))
            .collect();

        let (_, elapsed_time) = measure_time!(milliseconds {
            add_batch(i, &mut writer, elems, index_size);
        });

        writing_time += elapsed_time;
        print!("{} vectors included       \r", batch_size * i);
        let _ = std::io::stdout().flush();
    }
    println!("\nCleaning garbage..");
    writer.collect_garbage().unwrap();
    println!("Garbage cleaned");
    writing_time
}

fn create_filtered_request() -> Request {
    let random_subset = vector_random_subset(LABELS.to_vec().clone());

    let formula = random_subset
        .clone()
        .into_iter()
        .fold(Formula::new(), |mut acc, i| {
            acc.extend(AtomClause::label(i.to_string()));
            acc
        });

    Request {
        filter: formula,
        vector: RandomVectors::new(VECTOR_DIM).next().unwrap(),
    }
}

fn test_datapoint(
    index_size: usize,
    batch_size: usize,
    cycles: usize,
    unfiltered_request: &Request,
    filtered_requests: &[Request],
    vecs: &[RandomVectors],
) -> Stats {
    let _ = Merger::install_global().map(std::thread::spawn);
    let mut stats = Stats {
        writing_time: 0,
        read_time: 0,
        tagged_time: 0,
    };

    let at = tempfile::TempDir::new().unwrap();
    let db_location = at.path().join("vectors");

    stats.writing_time = create_db(&db_location, index_size, batch_size, vecs) as u128;

    let reader = Index::open(&db_location).unwrap();

    for cycle in 0..cycles {
        print!(
            "Unfiltered Search => cycle {} of {}      \r",
            (cycle + 1),
            cycles
        );
        let _ = std::io::stdout().flush();

        let (_, elapsed_time) = measure_time!(microseconds {
            reader.search(unfiltered_request).unwrap();
        });
        stats.read_time += elapsed_time as u128;
    }

    stats.read_time /= cycles as u128;
    println!();

    for (cycle, filtered_request) in filtered_requests.iter().enumerate().take(cycles) {
        print!(
            "Filtered Search => cycle {} of {}      \r",
            (cycle + 1),
            cycles
        );
        let _ = std::io::stdout().flush();

        let (_, elapsed_time) = measure_time!(microseconds {
            reader.search(filtered_request).unwrap();
        });
        stats.tagged_time += elapsed_time as u128;
    }

    println!();
    stats.tagged_time /= cycles as u128;
    stats
}

fn main() {
    let args = Args::new();
    let mut json_results = vec![];

    let unfiltered_request = Request {
        filter: Formula::new(),
        vector: RandomVectors::new(VECTOR_DIM).next().unwrap(),
    };

    let mut filtered_requests: Vec<Request> = vec![];
    for _ in 0..args.cycles {
        filtered_requests.push(create_filtered_request());
    }
    let vecs = generate_vecs(args.index_size / args.batch_size);

    let stats = test_datapoint(
        args.index_size,
        args.batch_size,
        args.cycles,
        &unfiltered_request,
        &filtered_requests,
        &vecs,
    );

    json_results.extend(vec![
        json!({
            "name": format!("Writing Time"),
            "unit": "ms",
            "value": stats.writing_time,
        }),
        json!({
        "name": format!("Reading Time"),
        "unit": "µs",
        "value": stats.read_time,

        }),
        json!({
        "name": format!("Reading Time with Labels"),
        "unit": "µs",
        "value": stats.tagged_time,

        }),
    ]);

    let pjson = serde_json::to_string_pretty(&json_results).unwrap();
    println!("{}", pjson);
    write_json(args.json_output, json_results, args.merge).unwrap();
}
