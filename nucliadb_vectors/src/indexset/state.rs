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

use std::borrow::Cow;
use std::collections::HashSet;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use super::IndexKeyCollector;
use crate::data_point::Similarity;
use crate::data_point_provider::{Index, IndexMetadata};
use crate::VectorR;
#[derive(Serialize, Deserialize)]
pub struct State {
    location: PathBuf,
    indexes: HashSet<String>,
}
impl State {
    pub fn new(at: PathBuf) -> State {
        State {
            location: at,
            indexes: HashSet::default(),
        }
    }
    pub fn index_keys<C: IndexKeyCollector>(&self, c: &mut C) {
        self.indexes.iter().cloned().for_each(|s| c.add_key(s));
    }
    pub fn remove_index(&mut self, index: &str) -> VectorR<()> {
        if self.indexes.remove(index) {
            let index_path = self.location.join(index);
            std::fs::remove_dir_all(index_path)?;
        }
        Ok(())
    }
    pub fn get(&self, index: &str) -> VectorR<Option<Index>> {
        if self.indexes.contains(index) {
            let location = self.location.join(index);
            Some(Index::open(&location)).transpose()
        } else {
            Ok(None)
        }
    }
    pub fn get_or_create<'a, S>(&mut self, index: S, similarity: Similarity) -> VectorR<Index>
    where S: Into<Cow<'a, str>> {
        let index: Cow<_> = index.into();
        if self.indexes.contains(index.as_ref()) {
            let index = index.as_ref();
            let location = self.location.join(index);
            Index::open(&location)
        } else {
            let index = index.to_string();
            let location = self.location.join(&index);
            self.indexes.insert(index);
            // TODO: for now we set the channel to STABLE here
            let channel = nucliadb_core::Channel::STABLE;
            Index::new(
                &location,
                IndexMetadata {
                    similarity,
                    channel,
                },
            )
        }
    }
}

#[cfg(test)]
mod test {
    use tempfile::TempDir;

    use super::*;
    #[test]
    fn basic_functionality_test() {
        let dir = TempDir::new().unwrap();
        let mut vectorset = State::new(dir.path().to_path_buf());
        let _index1 = vectorset
            .get_or_create("Index1".to_string(), Similarity::Cosine)
            .unwrap();
        let _index2 = vectorset
            .get_or_create("Index2".to_string(), Similarity::Cosine)
            .unwrap();
        let _index3 = vectorset
            .get_or_create("Index3".to_string(), Similarity::Cosine)
            .unwrap();
        assert!(vectorset.get("Index1").unwrap().is_some());
        assert!(vectorset.get("Index2").unwrap().is_some());
        assert!(vectorset.get("Index3").unwrap().is_some());
        assert!(vectorset.get("Index4").unwrap().is_none());
        vectorset.remove_index("Index1").unwrap();
        assert!(vectorset.get("Index1").unwrap().is_none());
        assert!(vectorset.get("Index2").unwrap().is_some());
        assert!(vectorset.get("Index3").unwrap().is_some());
        vectorset.remove_index("Index2").unwrap();
        assert!(vectorset.get("Index1").unwrap().is_none());
        assert!(vectorset.get("Index2").unwrap().is_none());
        assert!(vectorset.get("Index3").unwrap().is_some());
        vectorset.remove_index("Index3").unwrap();
        assert!(vectorset.get("Index1").unwrap().is_none());
        assert!(vectorset.get("Index2").unwrap().is_none());
        assert!(vectorset.get("Index3").unwrap().is_none());
    }
}
