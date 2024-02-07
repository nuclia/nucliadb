// Copyright (C) 2021 Bosutech XXI S.L.
//
// nucliadb is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at info@nuclia.com.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.
//

use std::env::var;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{self, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::str::FromStr;

use byte_unit::Byte;
use indicatif::ProgressBar;
use reqwest::blocking::{Client, Response};
use reqwest::header::{HeaderValue, CONTENT_LENGTH, RANGE};
use reqwest::StatusCode;
use tar::Archive;

const CHUNK_SIZE: u32 = 1024 * 1024;

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

    let length = response.headers().get(CONTENT_LENGTH).ok_or("response doesn't include the content length").unwrap();

    u64::from_str(length.to_str().unwrap()).map_err(|_| "invalid Content-Length header").unwrap()
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

pub fn download_shard(
    url: &str,
    destination_dir: &str,
    dataset_name: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let download_path = format!("{}/download.tar.gz", destination_dir);

    // Check if the partially downloaded file exists
    let mut downloaded_bytes = 0;
    if Path::new(&download_path).exists() {
        // Open the file for appending and get the current file size
        let mut file = OpenOptions::new().write(true).append(true).open(&download_path)?;

        // Seek to the end to continue the download
        downloaded_bytes = file.seek(SeekFrom::End(0))?;
    } else {
        // Create a new file for downloading
        let file = File::create(&download_path)?;
        file.set_len(0)?;
    }

    let content_length = get_content_length(url);

    if downloaded_bytes < content_length {
        let mut file = OpenOptions::new().write(true).append(true).open(&download_path)?;

        println!("Downloading {}", Byte::from_bytes(content_length as u128).get_appropriate_unit(true),);

        let pb = ProgressBar::new(content_length);

        for range in PartialRangeIter::new(0, content_length - 1, CHUNK_SIZE)? {
            let mut response = range_request(url, &range);
            let status = response.status();

            if !(status == StatusCode::OK || status == StatusCode::PARTIAL_CONTENT) {
                panic!("Unexpected server response: {}", status)
            }
            std::io::copy(&mut response, &mut file).unwrap();

            // really???
            let length = u64::from_str(response.headers().get("Content-Length").unwrap().to_str().unwrap()).unwrap();
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
