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
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.
use std::path;

use nucliadb_services::*;
use serde::{Deserialize, Serialize};
use tokio::fs;
#[derive(Serialize, Deserialize)]
pub struct ShardConfig {
    pub version_paragraphs: u32,
    pub version_vectors: u32,
    pub version_fields: u32,
}

impl ShardConfig {
    pub async fn new(path: &str) -> ShardConfig {
        fs::create_dir_all(path).await.unwrap();
        let json_file = path::Path::new(path).join("config.json");
        if !json_file.exists() {
            let config = ShardConfig {
                version_paragraphs: paragraphs::MAX_VERSION,
                version_fields: fields::MAX_VERSION,
                version_vectors: vectors::MAX_VERSION,
            };
            let serialized = serde_json::to_string(&config).unwrap();
            fs::File::create(&json_file).await.unwrap();
            fs::write(&json_file, &serialized).await.unwrap();
            config
        } else {
            let content = fs::read_to_string(&json_file).await.unwrap();
            serde_json::from_str(&content).unwrap()
        }
    }
}
