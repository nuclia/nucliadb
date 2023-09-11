use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use rand::seq::SliceRandom;
use rand::Rng;
use reqwest::blocking::Client;
use serde_json::json;
use tar::Archive;

use nucliadb_vectors::data_point_provider::*;
use nucliadb_vectors::formula::*;
use vectors_benchmark::json_writer::write_json;
use vectors_benchmark::random_vectors::RandomVectors;
use vectors_benchmark::stats::Stats;

const NO_NEIGHBOURS: usize = 5;
const LABELS: [&str; 5] = [
    "nucliad_db_has_label_1",
    "nucliad_db_has_label_2",
    "nucliad_db_has_label_3",
    "nucliad_db_has_label_4",
    "nucliad_db_has_label_5",
];
const CYCLES: usize = 25;
const CHUNK_SIZE: usize = 1024 * 1024;

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
    /// Path of the dataset definitions file
    #[clap(short, long)]
    datasets: String,
    /// Name of the dataset to use
    #[clap(short, long)]
    dataset_name: String,
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

fn create_filtered_request(dimension: usize) -> Request {
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
        vector: RandomVectors::new(dimension).next().unwrap(),
    }
}

fn download_and_decompress_tarball(
    url: &str,
    destination_dir: &str,
    dataset_name: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    // Create a progress bar
    let pb = ProgressBar::new(0);
    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})")
        .unwrap()
        .progress_chars("#>-"));

    let download_path = format!("{}/download.tar.gz", destination_dir);

    // Check if the partially downloaded file exists
    let mut downloaded_bytes = 0;
    if Path::new(&download_path).exists() {
        // Open the file for appending and get the current file size
        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(&download_path)?;

        // Seek to the end to continue the download
        downloaded_bytes = file.seek(SeekFrom::End(0))?;
    } else {
        // Create a new file for downloading
        let file = File::create(&download_path)?;
        file.set_len(0)?;
    }

    // Create a request with the Range header to resume the download
    let client = Client::new();
    let response = client
        .get(url)
        .header("Range", format!("bytes={}-", downloaded_bytes))
        .send()?;

    if response.status().is_success() {
        // Append the downloaded data to the file
        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(&download_path)?;

        let mut content = response.bytes()?;

        while !content.is_empty() {
            let chunk_size = std::cmp::min(CHUNK_SIZE, content.len());
            let chunk = content.split_to(chunk_size);
            file.write_all(&chunk)?;
            downloaded_bytes += chunk.len() as u64;
            pb.set_position(downloaded_bytes);
        }

        pb.finish_and_clear();
    }

    let tarball_file = File::open(&download_path)?;
    let tar = flate2::read::GzDecoder::new(tarball_file);
    let mut archive = Archive::new(tar);

    let mut destination_dir = PathBuf::from(destination_dir);
    destination_dir.push(dataset_name);
    fs::create_dir_all(destination_dir.as_path())?;

    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?;
        let entry_destination = format!("{}/{}", destination_dir.display(), path.display());

        if entry.header().entry_type().is_dir() {
            std::fs::create_dir_all(&entry_destination)?;
        } else {
            let mut file = File::create(&entry_destination)?;
            io::copy(&mut entry, &mut file)?;
        }
    }

    std::fs::remove_file(&download_path)?;
    Ok(destination_dir.into_os_string().into_string().unwrap())
}

fn test_datapoint(db_location: &Path, cycles: usize) -> Stats {
    println!("Opening Shared located at {:?}", db_location);
    let _ = Merger::install_global().map(std::thread::spawn);
    let reader = Index::open(db_location).unwrap();
    let vector_dims = reader.get_dimension().unwrap() as usize;

    let mut stats = Stats {
        writing_time: 0,
        read_time: 0,
        tagged_time: 0,
    };

    let mut filtered_requests: Vec<Request> = vec![];
    for _ in 0..cycles {
        filtered_requests.push(create_filtered_request(vector_dims));
    }

    let lock = reader.get_slock().unwrap();

    let unfiltered_request = Request {
        filter: Formula::new(),
        vector: RandomVectors::new(vector_dims).next().unwrap(),
    };

    for cycle in 0..cycles {
        print!(
            "Unfiltered Search => cycle {} of {}      \r",
            (cycle + 1),
            cycles
        );
        let _ = std::io::stdout().flush();

        let (_, elapsed_time) = measure_time!(microseconds {
            reader.search(&unfiltered_request, &lock).unwrap();
        });
        stats.read_time += elapsed_time as u128;
    }

    stats.read_time /= cycles as u128;
    println!();
    let mut filtered_requests: Vec<Request> = vec![];
    for _ in 0..cycles {
        filtered_requests.push(create_filtered_request(vector_dims));
    }

    for (cycle, filtered_request) in filtered_requests.iter().enumerate().take(cycles) {
        print!(
            "Filtered Search => cycle {} of {}      \r",
            (cycle + 1),
            cycles
        );
        let _ = std::io::stdout().flush();

        let (_, elapsed_time) = measure_time!(microseconds {
            reader.search(filtered_request, &lock).unwrap();
        });
        stats.tagged_time += elapsed_time as u128;
    }

    println!();
    stats.tagged_time /= cycles as u128;
    std::mem::drop(lock);
    stats
}

fn main() {
    let args = Args::new();

    // open the dataset definition
    let datasets = fs::read_to_string(args.datasets.clone()).expect("Unable to read file");
    let dataset_definition: serde_json::Value =
        serde_json::from_str(&datasets).expect("Unable to parse");

    let defs = dataset_definition["datasets"].as_array().unwrap();

    let mut found: bool = false;
    let mut target = PathBuf::from(args.datasets.clone());
    target.pop();
    let root_dir = target.clone();
    target = fs::canonicalize(&target).unwrap();
    target.push(args.dataset_name.clone());

    for dataset in defs {
        if dataset["name"] == args.dataset_name {
            let url = dataset["shards"]["url"].as_str().unwrap();
            println!("Shard located at : {}", url);
            println!("Target path is {:?}", target);
            if target.exists() {
                println!("Already exists");
            } else {
                println!("Downloading {:?} to {:?}", url, root_dir);
                download_and_decompress_tarball(
                    url,
                    root_dir.to_str().unwrap(),
                    &args.dataset_name,
                )
                .unwrap();
            }
            target.push(dataset["shards"]["root_path"].as_str().unwrap());
            // picking the first id for now
            target.push(dataset["shards"]["ids"][0].as_str().unwrap());
            found = true;
            break;
        }
    }

    if !found {
        println!("{:?} dataset not found", args.dataset_name);
        return;
    }

    let mut json_results = vec![];

    target.push("vectors");

    let stats = test_datapoint(&target, args.cycles);

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
