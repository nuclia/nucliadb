use std::env::var;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::str::FromStr;

use byte_unit::Byte;
use clap::Parser;
use indicatif::ProgressBar;
use kv::*;
use reqwest::blocking::Client;
use reqwest::blocking::Response;
use reqwest::header::{HeaderValue, CONTENT_LENGTH, RANGE};
use reqwest::StatusCode;
use serde_json::{json, Map};
use tar::Archive;

use nucliadb_vectors::data_point_provider::*;
use nucliadb_vectors::formula::*;
use vectors_benchmark::json_writer::write_json;

const NO_NEIGHBOURS: usize = 5;
const CYCLES: usize = 25;
const CHUNK_SIZE: u32 = 1024 * 1024;

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
        false
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
    key_prefixes: Vec<String>,
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
    let key_prefixes = key_prefixes.iter().cloned().map(AtomClause::key_prefix);

    tags.iter()
        .cloned()
        .map(AtomClause::label)
        .for_each(|c| formula.extend(c));

    if key_prefixes.len() > 0 {
        formula.extend(CompoundClause::new(1, key_prefixes.collect()));
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

struct PartialRangeIter {
    start: u64,
    end: u64,
    buffer_size: u32,
}

impl PartialRangeIter {
    pub fn new(start: u64, end: u64, buffer_size: u32) -> Result<Self, Box<dyn std::error::Error>> {
        if buffer_size == 0 {
            Err("invalid buffer_size, give a value greater than zero.")?;
        }
        Ok(PartialRangeIter {
            start,
            end,
            buffer_size,
        })
    }
}

impl Iterator for PartialRangeIter {
    type Item = HeaderValue;
    fn next(&mut self) -> Option<Self::Item> {
        if self.start > self.end {
            None
        } else {
            let prev_start = self.start;
            self.start += std::cmp::min(self.buffer_size as u64, self.end - self.start + 1);
            Some(
                HeaderValue::from_str(&format!("bytes={}-{}", prev_start, self.start - 1))
                    .expect("string provided by format!"),
            )
        }
    }
}

fn get_gcp_token() -> String {
    let output = Command::new("gcloud")
        .args(["auth", "application-default", "print-access-token"])
        .output()
        .expect("Failed to execute gcloud command.");

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!("gcloud error:\n{}", stderr);
    }

    let res = String::from_utf8_lossy(&output.stdout);
    res.trim_end_matches(|c| c == '\n' || c == '\r').to_string()
}

fn get_content_length(url: &str) -> u64 {
    let client = reqwest::blocking::Client::new();
    let mut request = client.head(url);

    if url.starts_with("https://storage.googleapis.com") {
        request = request.bearer_auth(get_gcp_token());
    } else if let Ok(dataset_auth) = var("DATASET_AUTH") {
        let (user_name, password) = dataset_auth.split_once(':').unwrap();
        request = request.basic_auth(user_name, Some(password));
    }
    let response = request.send().unwrap();
    let status = response.status();

    if !(status == StatusCode::OK || status == StatusCode::PARTIAL_CONTENT) {
        panic!("Unexpected server response: {}", status)
    }

    let length = response
        .headers()
        .get(CONTENT_LENGTH)
        .ok_or("response doesn't include the content length")
        .unwrap();

    u64::from_str(length.to_str().unwrap())
        .map_err(|_| "invalid Content-Length header")
        .unwrap()
}

fn range_request(url: &str, range: &HeaderValue) -> Response {
    let client = Client::new();
    let mut request = client.get(url).header(RANGE, range);

    if url.starts_with("https://storage.googleapis.com") {
        request = request.bearer_auth(get_gcp_token());
    } else if let Ok(dataset_auth) = var("DATASET_AUTH") {
        let (user_name, password) = dataset_auth.split_once(':').unwrap();
        request = request.basic_auth(user_name, Some(password));
    }

    request.send().unwrap()
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

    let content_length = get_content_length(url);

    if downloaded_bytes < content_length {
        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(&download_path)?;

        println!(
            "Downloading {}",
            Byte::from_bytes(content_length as u128).get_appropriate_unit(true),
        );

        let pb = ProgressBar::new(content_length);

        for range in PartialRangeIter::new(0, content_length - 1, CHUNK_SIZE)? {
            let mut response = range_request(url, &range);
            let status = response.status();

            if !(status == StatusCode::OK || status == StatusCode::PARTIAL_CONTENT) {
                panic!("Unexpected server response: {}", status)
            }
            std::io::copy(&mut response, &mut file).unwrap();

            // really???
            let length = u64::from_str(
                response
                    .headers()
                    .get("Content-Length")
                    .unwrap()
                    .to_str()
                    .unwrap(),
            )
            .unwrap();
            downloaded_bytes += length;
            pb.set_position(downloaded_bytes);
            pb.tick();
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

            //println!("{} {:?}", query.name, query.request.filter);
            let (_, elapsed_time) = measure_time!(microseconds {
                search_result = reader.search(&query.request, &lock).unwrap();

            });

            if let Some(min_results) = query.min_results {
                if search_result.len() < min_results {
                    panic!(
                        "Not enough results found for query {}. Found {}, Expected at least {}",
                        query.name,
                        search_result.len(),
                        min_results
                    );
                }
            };

            if let Some(num_results) = query.num_results {
                if search_result.len() != num_results {
                    panic!(
                        "Number of results found for query {} unexpected. Found {}, Expected {}",
                        query.name,
                        search_result.len(),
                        num_results
                    );
                }
            };

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
    min_results: Option<usize>,
    num_results: Option<usize>,
}

struct Dataset {
    name: String,
    shard_id: String,
    vectors_path: PathBuf,
    queries: Vec<Query>,
}

fn trim(data: String) -> String {
    data.trim_matches('"').to_string()
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

                let query_name = trim(query["name"].to_string());

                let tags: Vec<String> = if query.contains_key("tags") {
                    query["tags"]
                        .as_array()
                        .unwrap()
                        .to_vec()
                        .iter()
                        .map(|v| trim(v.to_string()))
                        .collect()
                } else {
                    vec![]
                };

                let key_prefixes: Vec<String> = if query.contains_key("key_prefixes") {
                    query["key_prefixes"]
                        .as_array()
                        .unwrap()
                        .to_vec()
                        .iter()
                        .map(|v| trim(v.to_string()))
                        .collect()
                } else {
                    vec![]
                };

                let request = create_request(
                    dataset_name.clone(),
                    query_name.clone(),
                    trim(query["query"].to_string()),
                    tags,
                    key_prefixes,
                    num_dimensions,
                );

                let min_results = if query.contains_key("min_results") {
                    Some(query["min_results"].as_u64().unwrap() as usize)
                } else {
                    None
                };
                let num_results = if query.contains_key("num_results") {
                    Some(query["num_results"].as_u64().unwrap() as usize)
                } else {
                    None
                };

                queries.push(Query {
                    name: query_name,
                    request,
                    min_results,
                    num_results,
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
