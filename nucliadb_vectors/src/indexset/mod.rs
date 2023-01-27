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

use nucliadb_core::fs_state::{self, ELock, Lock, SLock, Version};
use state::State;

use crate::data_point_provider::{Index, IndexCheck, VectorR};

pub trait IndexKeyCollector {
    fn add_key(&mut self, key: String);
}

pub struct IndexSet {
    state: RwLock<State>,
    date: RwLock<Version>,
    location: PathBuf,
}
impl IndexSet {
    pub fn new(path: &Path, with_check: IndexCheck) -> VectorR<IndexSet> {
        if !path.exists() {
            std::fs::create_dir_all(path)?;
        }
        fs_state::initialize_disk(path, || State::new(path.to_path_buf()))?;
        let lock = fs_state::shared_lock(path)?;
        let state = fs_state::load_state::<State>(&lock)?;
        let date = fs_state::crnt_version(&lock)?;
        if let IndexCheck::Sanity = with_check {
            state.do_sanity_checks()?;
        }
        let index = IndexSet {
            state: RwLock::new(state),
            date: RwLock::new(date),
            location: path.to_path_buf(),
        };
        Ok(index)
    }
    pub fn remove_index(&mut self, index: &str, _: &ELock) -> VectorR<()> {
        let mut write = self.state.write().unwrap();
        write.remove_index(index)
    }
    pub fn get_or_create<'a, S>(&'a mut self, index: S, _: &ELock) -> VectorR<Index>
    where S: Into<std::borrow::Cow<'a, str>> {
        let mut write = self.state.write().unwrap();
        write.get_or_create(index)
    }
    fn update(&self, lock: &fs_state::Lock) -> VectorR<()> {
        let disk_v = fs_state::crnt_version(lock)?;
        let date = *self.date.read().unwrap();
        if disk_v > date {
            let new_state = fs_state::load_state(lock)?;
            let mut state = self.state.write().unwrap();
            let mut date = self.date.write().unwrap();
            *state = new_state;
            *date = disk_v;
        }
        Ok(())
    }
    pub fn index_keys<C: IndexKeyCollector>(&self, c: &mut C, _: &Lock) {
        let read = self.state.read().unwrap();
        read.index_keys(c);
    }
    pub fn get(&self, index: &str, _: &Lock) -> VectorR<Option<Index>> {
        let read = self.state.read().unwrap();
        read.get(index)
    }
    pub fn get_elock(&self) -> VectorR<ELock> {
        let lock = fs_state::exclusive_lock(&self.location)?;
        self.update(&lock)?;
        Ok(lock)
    }
    pub fn get_slock(&self) -> VectorR<SLock> {
        let lock = fs_state::shared_lock(&self.location)?;
        self.update(&lock)?;
        Ok(lock)
    }
    pub fn get_location(&self) -> &Path {
        &self.location
    }
    pub fn commit(&self, lock: ELock) -> VectorR<()> {
        let state = self.state.read().unwrap();
        let mut date = self.date.write().unwrap();
        fs_state::persist_state::<State>(&lock, &state)?;
        *date = fs_state::crnt_version(&lock)?;
        Ok(())
    }
}
