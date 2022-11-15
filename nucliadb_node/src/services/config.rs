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
use std::{fs, path};

use nucliadb_services::*;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Default)]
struct StoredConfig {
    #[serde(default)]
    pub version_paragraphs: Option<u32>,
    #[serde(default)]
    pub version_vectors: Option<u32>,
    #[serde(default)]
    pub version_fields: Option<u32>,
    #[serde(default)]
    pub version_relations: Option<u32>,
}
impl StoredConfig {
    fn fill_gaps(&mut self) -> bool {
        let mut modifed = false;
        if self.version_paragraphs.is_none() {
            self.version_paragraphs = Some(paragraphs::MAX_VERSION);
            modifed = true;
        }
        if self.version_vectors.is_none() {
            self.version_vectors = Some(vectors::MAX_VERSION);
            modifed = true;
        }
        if self.version_fields.is_none() {
            self.version_fields = Some(fields::MAX_VERSION);
            modifed = true;
        }
        if self.version_relations.is_none() {
            self.version_relations = Some(fields::MAX_VERSION);
            modifed = true;
        }
        modifed
    }
}

enum ConfigState {
    UpToDate(ShardConfig),
    Modified(ShardConfig),
}

#[derive(Serialize, Deserialize)]
pub struct ShardConfig {
    pub version_paragraphs: u32,
    pub version_vectors: u32,
    pub version_fields: u32,
    pub version_relations: u32,
}

impl Default for ShardConfig {
    fn default() -> Self {
        ShardConfig {
            version_paragraphs: paragraphs::MAX_VERSION,
            version_fields: fields::MAX_VERSION,
            version_vectors: vectors::MAX_VERSION,
            version_relations: relations::MAX_VERSION,
        }
    }
}

impl From<StoredConfig> for ShardConfig {
    fn from(raw: StoredConfig) -> Self {
        ShardConfig {
            version_paragraphs: raw.version_paragraphs.unwrap(),
            version_fields: raw.version_fields.unwrap(),
            version_vectors: raw.version_vectors.unwrap(),
            version_relations: raw.version_relations.unwrap(),
        }
    }
}
impl ShardConfig {
    pub fn new(path: &str) -> ShardConfig {
        fs::create_dir_all(path).unwrap();
        let json_file = path::Path::new(path).join("config.json");
        if !json_file.exists() {
            let config = ShardConfig::default();
            let serialized = serde_json::to_string(&config).unwrap();
            fs::File::create(&json_file).unwrap();
            fs::write(&json_file, &serialized).unwrap();
        }
        match ShardConfig::read_config(&json_file) {
            ConfigState::UpToDate(config) => config,
            ConfigState::Modified(config) => {
                let serialized = serde_json::to_string(&config).unwrap();
                fs::File::create(&json_file).unwrap();
                fs::write(&json_file, &serialized).unwrap();
                config
            }
        }
    }
    fn read_config(json_file: &path::Path) -> ConfigState {
        let content = fs::read_to_string(json_file).unwrap();
        let mut raw: StoredConfig = serde_json::from_str(&content).unwrap();
        if raw.fill_gaps() {
            ConfigState::Modified(ShardConfig::from(raw))
        } else {
            ConfigState::UpToDate(ShardConfig::from(raw))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn open_and_new() {
        let dir = tempfile::tempdir().unwrap();
        let config = ShardConfig::new(dir.path().to_str().unwrap());
        assert_eq!(config.version_relations, relations::MAX_VERSION);
        assert_eq!(config.version_fields, fields::MAX_VERSION);
        assert_eq!(config.version_paragraphs, paragraphs::MAX_VERSION);
        assert_eq!(config.version_vectors, vectors::MAX_VERSION);
    }
}
