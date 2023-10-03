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
use std::env;
use std::path::PathBuf;

use nucliadb_core::tracing::*;

use crate::utils::parse_log_levels;

/// Where data will be stored
pub fn data_path() -> PathBuf {
    match env::var("DATA_PATH") {
        Ok(var) => PathBuf::from(var),
        Err(_) => PathBuf::from("data"),
    }
}

/// Path for shards information inside data folder
pub fn shards_path() -> PathBuf {
    data_path().join("shards")
}

pub fn is_debug() -> bool {
    env::var("DEBUG").is_ok()
}

pub fn log_level() -> Vec<(String, Level)> {
    let default = "nucliadb_node=WARN".to_string();
    match env::var("RUST_LOG") {
        Ok(levels) => parse_log_levels(&levels),
        Err(_) => {
            error!("RUST_LOG not defined. Defaulting to {default}");
            parse_log_levels(&default)
        }
    }
}

pub fn metrics_http_port(default: u16) -> u16 {
    match env::var("METRICS_HTTP_PORT") {
        Ok(http_port) => {
            if let Ok(port) = http_port.parse() {
                port
            } else {
                error!("METRICS_HTTP_PORT defined incorrectly. Defaulting to {default}");
                default
            }
        }
        Err(_) => default,
    }
}

pub fn max_shards_per_node() -> usize {
    let default = 800;
    match env::var("MAX_NODE_REPLICAS") {
        Ok(max_shards) => {
            if let Ok(max_shards) = max_shards.parse() {
                max_shards
            } else {
                error!("MAX_NODE_REPLICAS defined incorrectly. Using default: {default}");
                default
            }
        }
        Err(_) => default,
    }
}

pub fn host_key_path() -> PathBuf {
    match env::var("HOST_KEY_PATH") {
        Ok(var) => PathBuf::from(var),
        Err(_) => PathBuf::from("host_key"),
    }
}

pub fn num_global_rayon_threads() -> usize {
    match env::var("NUM_GLOBAL_RAYON_THREADS") {
        Ok(threadstr) => {
            if let Ok(threads) = threadstr.parse() {
                threads
            } else {
                error!("NUM_GLOBAL_RAYON_THREADS defined incorrectly. Defaulting to num cpus.");
                10
            }
        }
        Err(_) => 10,
    }
}
