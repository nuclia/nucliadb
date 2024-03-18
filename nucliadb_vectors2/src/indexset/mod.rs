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

mod state;
use std::path::{Path, PathBuf};
use std::sync::RwLock;

use nucliadb_core::fs_state::{self, Version};
use state::State;

use crate::data_point::Similarity;
use crate::data_point_provider::reader::Reader;
use crate::data_point_provider::writer::Writer;
use crate::data_point_provider::IndexMetadata;
use crate::VectorR;

pub trait IndexKeyCollector {
    fn add_key(&mut self, key: String);
}

pub struct WriterSet {
    state: State,
    location: PathBuf,
}
impl WriterSet {
    pub fn new(path: &Path) -> VectorR<WriterSet> {
        if !path.exists() {
            std::fs::create_dir(path)?;
        }
        fs_state::initialize_disk(path, State::default)?;
        let state = fs_state::load_state::<State>(path)?;
        Ok(WriterSet {
            state,
            location: path.to_path_buf(),
        })
    }
    pub fn remove_index(&mut self, index: &str) {
        self.state.indexes.remove(index);
    }
    pub fn create_index(&mut self, index: &str, similarity: Similarity) -> VectorR<Writer> {
        if self.state.indexes.contains(index) {
            let location = self.location.join(index);
            Writer::open(&location)
        } else {
            let metadata = IndexMetadata {
                similarity,
                ..Default::default()
            };
            let location = self.location.join(index);
            let index_writer = Writer::new(&location, metadata)?;
            self.state.indexes.insert(index.to_string());
            Ok(index_writer)
        }
    }
    pub fn index_keys<C: IndexKeyCollector>(&self, c: &mut C) {
        self.state.indexes.iter().cloned().for_each(|s| c.add_key(s))
    }
    pub fn get(&self, index: &str) -> VectorR<Option<Writer>> {
        if self.state.indexes.contains(index) {
            let location = self.location.join(index);
            Some(Writer::open(&location)).transpose()
        } else {
            Ok(None)
        }
    }
    pub fn get_location(&self) -> &Path {
        &self.location
    }
    pub fn commit(&self) -> VectorR<()> {
        fs_state::persist_state::<State>(&self.location, &self.state)?;
        Ok(())
    }
}

pub struct ReaderSet {
    state: RwLock<State>,
    date: RwLock<Version>,
    location: PathBuf,
}
impl ReaderSet {
    pub fn new(path: &Path) -> VectorR<ReaderSet> {
        if !path.exists() {
            std::fs::create_dir(path)?;
        }
        fs_state::initialize_disk(path, State::default)?;
        let state = fs_state::load_state::<State>(path)?;
        let date = fs_state::crnt_version(path)?;
        Ok(ReaderSet {
            state: RwLock::new(state),
            date: RwLock::new(date),
            location: path.to_path_buf(),
        })
    }
    pub fn update(&self) -> VectorR<()> {
        let disk_v = fs_state::crnt_version(&self.location)?;
        let current_v = *self.date.read().unwrap_or_else(|e| e.into_inner());
        if disk_v != current_v {
            let new_state = fs_state::load_state(&self.location)?;
            let mut state = self.state.write().unwrap();
            let mut date = self.date.write().unwrap();
            *state = new_state;
            *date = disk_v;
        }
        Ok(())
    }
    pub fn index_keys<C: IndexKeyCollector>(&self, c: &mut C) {
        let state = self.state.read().unwrap();
        state.indexes.iter().cloned().for_each(|s| c.add_key(s))
    }
    pub fn get(&self, index: &str) -> VectorR<Option<Reader>> {
        let state = self.state.read().unwrap();
        if state.indexes.contains(index) {
            let location = self.location.join(index);
            Some(Reader::open(&location)).transpose()
        } else {
            Ok(None)
        }
    }
    pub fn get_location(&self) -> &Path {
        &self.location
    }
}
