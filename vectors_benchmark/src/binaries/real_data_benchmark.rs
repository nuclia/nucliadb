use std::env::var;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use kv::*;
use reqwest::blocking::Client;
use serde_json::{json, Map};
use tar::Archive;

use nucliadb_vectors::data_point_provider::*;
use nucliadb_vectors::formula::*;
use vectors_benchmark::json_writer::write_json;

const NO_NEIGHBOURS: usize = 5;
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

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
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

#[derive(serde::Serialize, serde::Deserialize)]
struct PredictResults {
    data: Vec<f32>,
}

fn create_request(
    dataset_name: String,
    query_name: String,
    query: String,
    tags: Vec<String>,
    key_filters: Vec<String>,
    dimension: usize,
) -> Request {
    // check if we have it on cache already
    let query_key = format!("{dataset_name}-{query_name}");
    let cfg = Config::new("./requests.cache");
    let store = Store::new(cfg).unwrap();
    let bucket = store
        .bucket::<String, Json<Request>>(Some("requests"))
        .unwrap();

    let stored_request = bucket.get(&query_key).unwrap();
    if let Some(res) = stored_request {
        return res.into_inner();
    }

    // Calling the NUA service to convert the query as a vector
    let client = Client::new();
    let nua_key = var("NUA_KEY").unwrap();

    let response = client
        .get("https://europe-1.stashify.cloud/api/v1/predict/sentence")
        .query(&[("text", query), ("model", "multilingual".to_string())])
        .header("X-STF-NUAKEY", format!("Bearer {nua_key}"))
        .send()
        .unwrap();

    if response.status().as_u16() > 299 {
        panic!("[predict] Got a {} response from nua", response.status());
    }

    let json: PredictResults = serde_json::from_str(response.text().unwrap().as_str()).unwrap();

    if json.data.len() != dimension {
        panic!(
            "[predict] Got a vector of length {}, expected {}",
            json.data.len(),
            dimension
        );
    }
    // building the formula using tags and key filters
    let mut formula = Formula::new();
    let key_filters = key_filters.iter().cloned().map(AtomClause::key_prefix);

    tags.iter()
        .cloned()
        .map(AtomClause::label)
        .for_each(|c| formula.extend(c));

    if key_filters.len() > 0 {
        formula.extend(CompoundClause::new(1, key_filters.collect()));
    }

    let res = Request {
        filter: formula,
        vector: json.data,
    };

    // saving it in cache before we return it
    let val: Json<Request> = Json(res.clone());
    bucket.set(&query_key, &val).unwrap();
    res
}

fn download_and_decompress_tarball(
    url: &str,
    destination_dir: &str,
    dataset_name: &str,
) -> Result<String, Box<dyn std::error::Error>> {
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
        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(&download_path)?;

        let content_length = response.content_length().unwrap_or(100);
        let mut content = response.bytes()?;
        let pb = ProgressBar::new(content_length);

        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})")
            .unwrap()
            .progress_chars("#>-"));

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

fn get_num_dimensions(vectors_path: &Path) -> usize {
    let reader = Index::open(vectors_path).unwrap();
    reader.get_dimension().unwrap() as usize
}

fn test_search(dataset: &Dataset, cycles: usize) -> Vec<(String, Vec<u128>)> {
    println!("Opening vectors located at {:?}", dataset.vectors_path);
    let _ = Merger::install_global().map(std::thread::spawn);
    let reader = Index::open(dataset.vectors_path.as_path()).unwrap();
    let mut results: Vec<(String, Vec<u128>)> = vec![];

    let lock = reader.get_slock().unwrap();

    for (i, query) in dataset.queries.iter().enumerate() {
        let mut elapsed_times: Vec<u128> = vec![];

        for cycle in 0..cycles {
            print!(
                "Request {} => cycle {} of {}      \r",
                i,
                (cycle + 1),
                cycles
            );
            let _ = std::io::stdout().flush();

            let search_result;

            let (_, elapsed_time) = measure_time!(microseconds {
                search_result = reader.search(&query.request, &lock).unwrap();

            });

            if search_result.is_empty() {
                panic!("No results found for query {}", query.name);
            }
            elapsed_times.push(elapsed_time as u128);
        }

        results.push((query.name.clone(), elapsed_times));
    }
    println!();
    std::mem::drop(lock);
    results
}

struct Query {
    name: String,
    request: Request,
}

struct Dataset {
    name: String,
    shard_id: String,
    vectors_path: PathBuf,
    queries: Vec<Query>,
}

fn get_dataset(definition_file: String, dataset_name: String) -> Option<Dataset> {
    // open the dataset definition
    let datasets = fs::read_to_string(definition_file.clone()).expect("Unable to read file");
    let dataset_definition: serde_json::Value =
        serde_json::from_str(&datasets).expect("Unable to parse");

    let defs = dataset_definition["datasets"].as_array().unwrap();

    let mut queries = vec![];
    let mut found: bool = false;
    let mut shard_id = String::new();
    let mut target = PathBuf::from(definition_file);
    target.pop();
    let root_dir = target.clone();
    target = fs::canonicalize(&target).unwrap();
    target.push(dataset_name.clone());

    for dataset in defs {
        if dataset["name"] == dataset_name {
            let url = dataset["shards"]["url"].as_str().unwrap();
            if !target.exists() {
                println!("Downloading {:?} to {:?}", url, root_dir);
                download_and_decompress_tarball(url, root_dir.to_str().unwrap(), &dataset_name)
                    .unwrap();
            }
            target.push(dataset["shards"]["root_path"].as_str().unwrap());
            // picking the first id for now
            shard_id = dataset["shards"]["ids"][0].as_str().unwrap().to_string();
            target.push(shard_id.clone());
            target.push("vectors");
            let num_dimensions = get_num_dimensions(&target);
            for entry in dataset["queries"].as_array().unwrap() {
                let query: Map<String, _> = entry.as_object().unwrap().to_owned();

                let query_name = query["name"].to_string().trim_matches('"').to_string();

                let tags: Vec<String> = if query.contains_key("tags") {
                    query["tags"]
                        .as_array()
                        .unwrap()
                        .to_vec()
                        .iter()
                        .map(|v| v.to_string())
                        .collect()
                } else {
                    vec![]
                };

                let key_filters: Vec<String> = if query.contains_key("key_filters") {
                    query["key_filters"]
                        .as_array()
                        .unwrap()
                        .to_vec()
                        .iter()
                        .map(|v| v.to_string())
                        .collect()
                } else {
                    vec![]
                };

                let request = create_request(
                    dataset_name.clone(),
                    query_name.clone(),
                    query["query"].to_string().trim_matches('"').to_string(),
                    tags,
                    key_filters,
                    num_dimensions,
                );

                queries.push(Query {
                    name: query_name,
                    request,
                });
            }
            found = true;
            break;
        }
    }

    if !found {
        println!("{:?} dataset not found", dataset_name);
        return None;
    }

    Some(Dataset {
        name: dataset_name,
        shard_id,
        vectors_path: target,
        queries,
    })
}

fn main() {
    let args = Args::new();

    let dataset = get_dataset(args.datasets.clone(), args.dataset_name.clone())
        .unwrap_or_else(|| panic!("Dataset {} not found", args.dataset_name));

    println!(
        "Using dataset: {:?} shard: {:?}",
        dataset.name, dataset.shard_id
    );

    let mut json_results = vec![];
    let results = test_search(&dataset, args.cycles);

    for (name, values) in results {
        json_results.push(json!({
        "name": name,
        "unit": "µs",
        "values": values,
        "value":  values.iter().sum::<u128>() / values.len() as u128,
        }));
    }

    let pjson = serde_json::to_string_pretty(&json_results).unwrap();
    println!("{}", pjson);
    write_json(args.json_output, json_results, args.merge).unwrap();
}
