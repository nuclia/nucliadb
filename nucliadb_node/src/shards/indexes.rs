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

use std::collections::HashMap;
use std::io::{BufReader, BufWriter};
use std::fs::File;
use std::path::{Path, PathBuf};
use nucliadb_core::NodeResult;
use serde::{Serialize, Deserialize};
use uuid::Uuid;
use nucliadb_core::node_error;

use crate::disk_structure;


pub const DEFAULT_VECTOR_INDEX_NAME: &str = "__default__";
pub const SHARD_INDEXES_FILENAME: &str = "indexes.json";

#[cfg_attr(test, derive(Debug))]
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

    // Vectorsets

    /// Add a new vectorset to the index and returns it's path
    pub fn add_vectorset(&mut self, name: String) -> NodeResult<PathBuf> {
        if name == DEFAULT_VECTOR_INDEX_NAME {
            return Err(
                node_error!(
                    format!(
                        "Vectorset id {DEFAULT_VECTOR_INDEX_NAME} is reserved for internal use"
                    )
                )
            );
        }

        let uuid = format!("vectorset_{}", Uuid::new_v4());
        let path = self.shard_path.join(uuid.clone());
        self.inner.vectorsets.insert(name, uuid);
        Ok(path)
    }

    /// Removes a vectorset from the shard and returns the index path
    pub fn remove_vectorset(&mut self, name: &str) -> NodeResult<Option<PathBuf>> {
        if name == DEFAULT_VECTOR_INDEX_NAME {
            return Err(
                node_error!(
                    format!(
                        "Vectorset id {DEFAULT_VECTOR_INDEX_NAME} is reserved and can't be removed"
                    )
                )
            );
        }
        let removed = self.inner.vectorsets.remove(name).map(|vectorset| self.shard_path.join(vectorset));
        Ok(removed)
    }

    pub fn vectorset_path(&self, name: &str) -> Option<PathBuf> {
        self.inner.vectorsets.get(name).map(|vectorset| self.shard_path.join(vectorset))
    }

    pub fn iter_vectorsets(&self) -> impl Iterator<Item = (String, PathBuf)> + '_ {
        self.inner.vectorsets.iter().map(|(name, vectorset)| (name.to_owned(), self.shard_path.join(vectorset)))
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
        let indexes: ShardIndexesFile = serde_json::from_reader(&mut reader)?;
        Ok(indexes)
    }

    pub fn store(&self, shard_path: &Path) -> NodeResult<()> {
        let mut writer = BufWriter::new(File::create(shard_path.join(SHARD_INDEXES_FILENAME))?);
        serde_json::to_writer(&mut writer, &self)?;
        Ok(())
    }
}

impl Default for ShardIndexesFile {
    fn default() -> Self {
        let mut vectorsets = HashMap::new();
        vectorsets.insert(DEFAULT_VECTOR_INDEX_NAME.to_string(), disk_structure::VECTORS_DIR.into());
        Self {
            texts: disk_structure::TEXTS_DIR.into(),
            paragraphs: disk_structure::PARAGRAPHS_DIR.into(),
            vectorsets,
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

        let vectorsets = indexes.iter_vectorsets().collect::<Vec<(String, PathBuf)>>();
        assert_eq!(vectorsets.len(), 1);
        assert_eq!(vectorsets[0].0, DEFAULT_VECTOR_INDEX_NAME.to_string());
        assert_eq!(vectorsets[0].1, shard_path.join(disk_structure::VECTORS_DIR));

        assert_eq!(indexes.vectorset_path(DEFAULT_VECTOR_INDEX_NAME), Some(shard_path.join(disk_structure::VECTORS_DIR)));

        // Default vectorset can't be removed
        assert!(indexes.remove_vectorset(DEFAULT_VECTOR_INDEX_NAME).is_err());

    }

    #[test]
    fn test_vectorset_path() {
        let tempdir = tempfile::tempdir().unwrap();
        let shard_path = tempdir.path();

        let mut indexes = ShardIndexes::new(shard_path);

        indexes.add_vectorset("gecko".to_string()).unwrap();

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

        indexes.add_vectorset("gecko".to_string()).unwrap();
        indexes.add_vectorset("openai".to_string()).unwrap();

        let vectorsets = indexes.iter_vectorsets().sorted().collect::<Vec<(String, PathBuf)>>();
        assert_eq!(vectorsets.len(), 3);

        assert_eq!(vectorsets[0].0, DEFAULT_VECTOR_INDEX_NAME.to_string());
        assert_eq!(vectorsets[1].0, "gecko".to_string());
        assert_eq!(vectorsets[1].1, indexes.vectorset_path("gecko").unwrap());
        assert_eq!(vectorsets[2].0, "openai".to_string());
        assert_eq!(vectorsets[2].1, indexes.vectorset_path("openai").unwrap());
    }

    #[test]
    fn test_add_and_remove_vectorsets() {
        let tempdir = tempfile::tempdir().unwrap();
        let shard_path = tempdir.path();

        let mut indexes = ShardIndexes::new(shard_path);

        // Add two vectorsets more

        let added = indexes.add_vectorset("gecko".to_string()).is_ok();
        assert!(added);
        let added = indexes.add_vectorset("openai".to_string()).is_ok();
        assert!(added);

        let vectorsets = indexes.iter_vectorsets().sorted().collect::<Vec<(String, PathBuf)>>();
        assert_eq!(vectorsets.len(), 3);

        assert_eq!(vectorsets[0].0, DEFAULT_VECTOR_INDEX_NAME.to_string());
        assert_eq!(vectorsets[1].0, "gecko".to_string());
        assert_eq!(vectorsets[1].1, indexes.vectorset_path("gecko").unwrap());
        assert_eq!(vectorsets[2].0, "openai".to_string());
        assert_eq!(vectorsets[2].1, indexes.vectorset_path("openai").unwrap());

        // Remove a regular vectorset

        assert!(indexes.remove_vectorset("gecko").is_ok());

        let vectorsets = indexes.iter_vectorsets().sorted().collect::<Vec<(String, PathBuf)>>();
        assert_eq!(vectorsets.len(), 2);
        assert_eq!(vectorsets[0].0, DEFAULT_VECTOR_INDEX_NAME.to_string());
        assert_eq!(vectorsets[1].0, "openai".to_string());
        assert_eq!(vectorsets[1].1, indexes.vectorset_path("openai").unwrap());
    }
}
