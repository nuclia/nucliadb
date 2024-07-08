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

use nucliadb_core::node_error;
use nucliadb_core::NodeResult;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};
use uuid::Uuid;

use crate::disk_structure;

pub const DEFAULT_VECTORS_INDEX_NAME: &str = "__default__";
pub const MAX_ALLOWED_VECTORSETS: usize = 5;
pub const SHARD_INDEXES_FILENAME: &str = "indexes.json";
pub const TEMP_SHARD_INDEXES_FILENAME: &str = "indexes.temp.json";

#[derive(Debug)]
pub struct ShardIndexes {
    inner: ShardIndexesFile,
    shard_path: PathBuf,
}

impl ShardIndexes {
    pub fn new(shard_path: &Path) -> Self {
        Self {
            inner: ShardIndexesFile::default(),
            shard_path: shard_path.to_path_buf(),
        }
    }

    pub fn load(shard_path: &Path) -> NodeResult<Self> {
        Ok(Self {
            inner: ShardIndexesFile::load(shard_path)?,
            shard_path: shard_path.to_path_buf(),
        })
    }

    pub fn store(&self) -> NodeResult<()> {
        self.inner.store(&self.shard_path)
    }

    // Index path getters

    pub fn texts_path(&self) -> PathBuf {
        self.shard_path.join(&self.inner.texts)
    }

    pub fn paragraphs_path(&self) -> PathBuf {
        self.shard_path.join(&self.inner.paragraphs)
    }

    pub fn vectors_path(&self) -> PathBuf {
        self.vectorset_path(DEFAULT_VECTORS_INDEX_NAME).expect("Default vectors index should always be present")
    }

    pub fn vectorset_path(&self, name: &str) -> Option<PathBuf> {
        self.inner.vectorsets.get(name).map(|vectorset| self.shard_path.join(vectorset))
    }

    pub fn vectorset_relative_path(&self, name: &str) -> Option<PathBuf> {
        self.inner.vectorsets.get(name).map(PathBuf::from)
    }

    pub fn relations_path(&self) -> PathBuf {
        self.shard_path.join(&self.inner.relations)
    }

    // Vectorsets

    /// Add a new vectors index to the shard and returns it's path
    pub fn add_vectors_index(&mut self, name: String) -> NodeResult<PathBuf> {
        if self.inner.vectorsets.len() >= MAX_ALLOWED_VECTORSETS {
            return Err(node_error!(format!(
                "Max amount of allowed vectorsets reached: {}",
                self.inner.vectorsets.len()
            )));
        }
        if self.inner.vectorsets.contains_key(&name) {
            return Err(node_error!(format!("Vectorset id {name} is already in use")));
        }

        let uuid = if name == DEFAULT_VECTORS_INDEX_NAME {
            disk_structure::VECTORS_DIR.to_string()
        } else {
            format!("vectorset_{}", Uuid::new_v4())
        };
        let path = self.shard_path.join(uuid.clone());
        self.inner.vectorsets.insert(name, uuid);
        Ok(path)
    }

    /// Removes a vectors index from the shard and returns its path
    pub fn remove_vectors_index(&mut self, name: &str) -> NodeResult<Option<PathBuf>> {
        if name == DEFAULT_VECTORS_INDEX_NAME {
            return Err(node_error!(format!(
                "Vectorset id {DEFAULT_VECTORS_INDEX_NAME} is reserved and can't be removed"
            )));
        }
        let removed = self.inner.vectorsets.remove(name).map(|vectorset| self.shard_path.join(vectorset));
        Ok(removed)
    }

    pub fn iter_vectors_indexes(&self) -> impl Iterator<Item = (String, PathBuf)> + '_ {
        self.inner.vectorsets.iter().map(|(name, vectorset)| (name.to_owned(), self.shard_path.join(vectorset)))
    }

    pub fn count_vectors_indexes(&self) -> usize {
        self.inner.vectorsets.len()
    }
}

#[cfg_attr(test, derive(PartialEq))]
#[derive(Serialize, Deserialize, Debug)]
struct ShardIndexesFile {
    pub texts: String,
    pub paragraphs: String,
    pub vectorsets: HashMap<String, String>,
    pub relations: String,
}

impl ShardIndexesFile {
    pub fn load(shard_path: &Path) -> NodeResult<Self> {
        let mut reader = BufReader::new(File::open(shard_path.join(SHARD_INDEXES_FILENAME))?);
        let indexes: Self = serde_json::from_reader(&mut reader)?;
        Ok(indexes)
    }

    pub fn store(&self, shard_path: &Path) -> NodeResult<()> {
        let filename = shard_path.join(SHARD_INDEXES_FILENAME);
        let temp = shard_path.join(TEMP_SHARD_INDEXES_FILENAME);
        let mut writer = BufWriter::new(File::create(temp.clone())?);
        serde_json::to_writer(&mut writer, &self)?;
        writer.flush()?;
        std::fs::rename(temp, filename)?;
        Ok(())
    }
}

impl Default for ShardIndexesFile {
    fn default() -> Self {
        Self {
            texts: disk_structure::TEXTS_DIR.into(),
            paragraphs: disk_structure::PARAGRAPHS_DIR.into(),
            vectorsets: HashMap::new(),
            relations: disk_structure::RELATIONS_DIR.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use tempfile;

    use super::*;

    #[test]
    fn test_file_load_and_store() {
        let tempdir = tempfile::tempdir().unwrap();
        let shard_path = tempdir.path();
        let indexes_filename = shard_path.join(SHARD_INDEXES_FILENAME);

        assert!(!Path::exists(&indexes_filename));

        let indexes = ShardIndexesFile::default();
        indexes.store(shard_path).unwrap();
        assert!(Path::exists(&indexes_filename));

        let stored = ShardIndexesFile::load(shard_path).unwrap();
        assert_eq!(indexes, stored);
    }

    #[test]
    fn test_default_vectors_index() {
        let tempdir = tempfile::tempdir().unwrap();
        let shard_path = tempdir.path();

        let mut indexes = ShardIndexes::new(shard_path);

        let vectorsets = indexes.iter_vectors_indexes().collect::<Vec<(String, PathBuf)>>();
        assert_eq!(vectorsets.len(), 0);

        indexes.add_vectors_index(DEFAULT_VECTORS_INDEX_NAME.to_string()).unwrap();

        let vectorsets = indexes.iter_vectors_indexes().collect::<Vec<(String, PathBuf)>>();
        assert_eq!(vectorsets.len(), 1);
        assert_eq!(vectorsets[0].0, DEFAULT_VECTORS_INDEX_NAME.to_string());
        assert_eq!(vectorsets[0].1, shard_path.join(disk_structure::VECTORS_DIR));

        assert_eq!(
            indexes.vectorset_path(DEFAULT_VECTORS_INDEX_NAME),
            Some(shard_path.join(disk_structure::VECTORS_DIR))
        );

        // Default vectorset can't be removed
        assert!(indexes.remove_vectors_index(DEFAULT_VECTORS_INDEX_NAME).is_err());
    }

    #[test]
    fn test_indexes_path() {
        let tempdir = tempfile::tempdir().unwrap();
        let shard_path = tempdir.path();

        let mut indexes = ShardIndexes::new(shard_path);

        indexes.add_vectors_index("gecko".to_string()).unwrap();

        assert_eq!(indexes.texts_path(), shard_path.join(disk_structure::TEXTS_DIR));
        assert_eq!(indexes.paragraphs_path(), shard_path.join(disk_structure::PARAGRAPHS_DIR));
        assert_eq!(indexes.relations_path(), shard_path.join(disk_structure::RELATIONS_DIR));

        let vectorset_path_prefix = shard_path.join("vectorset_");
        let vectorset_path_prefix = vectorset_path_prefix.to_str().unwrap();
        let gecko_path = indexes.vectorset_path("gecko").unwrap();
        let gecko_path = gecko_path.to_str().unwrap();
        assert!(gecko_path.starts_with(vectorset_path_prefix));
        assert!(gecko_path.len() > vectorset_path_prefix.len());
    }

    #[test]
    fn test_iter_vectorsets() {
        let tempdir = tempfile::tempdir().unwrap();
        let shard_path = tempdir.path();

        let mut indexes = ShardIndexes::new(shard_path);

        indexes.add_vectors_index("gecko".to_string()).unwrap();
        indexes.add_vectors_index("openai".to_string()).unwrap();

        let vectorsets = indexes.iter_vectors_indexes().sorted().collect::<Vec<(String, PathBuf)>>();
        assert_eq!(vectorsets.len(), 2);

        assert_eq!(vectorsets[0].0, "gecko".to_string());
        assert_eq!(vectorsets[0].1, indexes.vectorset_path("gecko").unwrap());
        assert_eq!(vectorsets[1].0, "openai".to_string());
        assert_eq!(vectorsets[1].1, indexes.vectorset_path("openai").unwrap());
    }

    #[test]
    fn test_add_and_remove_vectorsets() {
        let tempdir = tempfile::tempdir().unwrap();
        let shard_path = tempdir.path();

        let mut indexes = ShardIndexes::new(shard_path);

        let added = indexes.add_vectors_index(DEFAULT_VECTORS_INDEX_NAME.to_string()).is_ok();
        assert!(added);

        // Add two vectorsets more

        let added = indexes.add_vectors_index("gecko".to_string()).is_ok();
        assert!(added);
        let added = indexes.add_vectors_index("openai".to_string()).is_ok();
        assert!(added);

        let vectorsets = indexes.iter_vectors_indexes().sorted().collect::<Vec<(String, PathBuf)>>();
        assert_eq!(vectorsets.len(), 3);

        assert_eq!(vectorsets[0].0, DEFAULT_VECTORS_INDEX_NAME.to_string());
        assert_eq!(vectorsets[1].0, "gecko".to_string());
        assert_eq!(vectorsets[1].1, indexes.vectorset_path("gecko").unwrap());
        assert_eq!(vectorsets[2].0, "openai".to_string());
        assert_eq!(vectorsets[2].1, indexes.vectorset_path("openai").unwrap());

        // Remove a regular vectorset

        assert!(indexes.remove_vectors_index("gecko").is_ok());

        let vectorsets = indexes.iter_vectors_indexes().sorted().collect::<Vec<(String, PathBuf)>>();
        assert_eq!(vectorsets.len(), 2);
        assert_eq!(vectorsets[0].0, DEFAULT_VECTORS_INDEX_NAME.to_string());
        assert_eq!(vectorsets[1].0, "openai".to_string());
        assert_eq!(vectorsets[1].1, indexes.vectorset_path("openai").unwrap());
    }

    #[test]
    fn test_add_vectorset_twice() {
        let tempdir = tempfile::tempdir().unwrap();
        let shard_path = tempdir.path();

        let mut indexes = ShardIndexes::new(shard_path);

        // Add two vectorsets more

        assert!(indexes.add_vectors_index("gecko".to_string()).is_ok());
        assert!(indexes.add_vectors_index("gecko".to_string()).is_err());
    }

    #[test]
    fn test_max_vectorsets_allowed() {
        let tempdir = tempfile::tempdir().unwrap();
        let shard_path = tempdir.path();

        let mut indexes = ShardIndexes::new(shard_path);

        for i in 0..MAX_ALLOWED_VECTORSETS {
            assert!(indexes.add_vectors_index(format!("vectorset-{i}")).is_ok());
        }
        assert!(indexes.add_vectors_index("too-many".to_string()).is_err());
    }
}
