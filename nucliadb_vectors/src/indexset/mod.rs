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
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use nucliadb_core::fs_state::{self, ELock, SLock, Version};
use state::State;

use crate::data_point::Similarity;
use crate::data_point_provider::{Index, IndexCheck};
use crate::VectorR;
pub trait IndexKeyCollector {
    fn add_key(&mut self, key: String);
}

#[derive(Clone)]
pub struct IndexSet {
    state: Arc<RwLock<State>>,
    date: Arc<RwLock<Version>>,
    location: PathBuf,
}
impl IndexSet {
    fn read_state(&self) -> RwLockReadGuard<'_, State> {
        self.state.read().unwrap_or_else(|e| e.into_inner())
    }
    fn write_state(&self) -> RwLockWriteGuard<'_, State> {
        self.state.write().unwrap_or_else(|e| e.into_inner())
    }
    fn read_date(&self) -> RwLockReadGuard<'_, Version> {
        self.date.read().unwrap_or_else(|e| e.into_inner())
    }
    fn write_date(&self) -> RwLockWriteGuard<'_, Version> {
        self.date.write().unwrap_or_else(|e| e.into_inner())
    }
    fn get_elock(&self) -> VectorR<ELock> {
        let lock = fs_state::exclusive_lock(&self.location)?;
        self.update(&lock)?;
        Ok(lock)
    }
    fn get_slock(&self) -> VectorR<SLock> {
        let lock = fs_state::shared_lock(&self.location)?;
        self.update(&lock)?;
        Ok(lock)
    }
    fn update(&self, lock: &fs_state::Lock) -> VectorR<()> {
        let disk_v = fs_state::crnt_version(lock)?;
        let date = *self.read_date();
        if disk_v > date {
            let new_state = fs_state::load_state(lock)?;
            let mut state = self.write_state();
            let mut date = self.write_date();
            *state = new_state;
            *date = disk_v;
        }
        Ok(())
    }
    fn commit(&self, lock: ELock) -> VectorR<()> {
        let state = self.read_state();
        let mut date = self.write_date();
        fs_state::persist_state::<State>(&lock, &state)?;
        *date = fs_state::crnt_version(&lock)?;
        Ok(())
    }
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
        Ok(IndexSet {
            state: Arc::new(RwLock::new(state)),
            date: Arc::new(RwLock::new(date)),
            location: path.to_path_buf(),
        })
    }
    pub fn remove_index(&mut self, index: &str) -> VectorR<()> {
        let lock = self.get_elock()?;
        let mut write = self.write_state();
        write.remove_index(index)?;
        std::mem::drop(write);
        self.commit(lock)
    }
    pub fn create(&mut self, index: &str, similarity: Similarity) -> VectorR<Index> {
        let lock = self.get_elock()?;
        let mut write = self.write_state();
        let index = write.create(index, similarity)?;
        std::mem::drop(write);
        self.commit(lock)?;
        Ok(index)
    }
    pub fn keys<C: IndexKeyCollector>(&self, c: &mut C) -> VectorR<()> {
        let _lock = self.get_slock()?;
        let read = self.read_state();
        read.index_keys(c);
        Ok(())
    }
    pub fn get(&self, index: &str) -> VectorR<Option<Index>> {
        let _lock = self.get_slock()?;
        let reader = self.read_state();
        reader.get(index)
    }
    pub fn get_location(&self) -> &Path {
        &self.location
    }
}
