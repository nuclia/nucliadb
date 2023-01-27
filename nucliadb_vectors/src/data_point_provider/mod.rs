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

mod merge_worker;
mod merger;
mod state;
use std::path::{Path, PathBuf};
use std::sync::RwLock;
use std::time::SystemTime;
use std::{io, mem};

use nucliadb_core::fs_state::{self, ELock, FsError, Lock, SLock, Version};
use state::*;
use thiserror::Error;

use crate::data_point::{DPError, DataPoint};

pub type TemporalMark = SystemTime;
pub trait SearchRequest {
    fn get_query(&self) -> &[f32];
    fn get_labels(&self) -> &[String];
    fn no_results(&self) -> usize;
    fn with_duplicates(&self) -> bool;
}

#[derive(Debug, Error)]
pub enum VectorErr {
    #[error("Serialization error: {0}")]
    SerErr(#[from] bincode::Error),
    #[error("IO error: {0}")]
    IoErr(#[from] io::Error),
    #[error("Error in data point: {0}")]
    Dp(#[from] DPError),
    #[error("Error in fs: {0}")]
    FsError(#[from] FsError),
}

pub type VectorR<O> = Result<O, VectorErr>;

#[derive(Clone, Copy, Debug)]
pub enum IndexCheck {
    None,
    Sanity,
}

pub struct Index {
    state: RwLock<State>,
    date: RwLock<Version>,
    location: PathBuf,
}
impl Index {
    fn update(&self, lock: &Lock) -> VectorR<()> {
        let disk_v = fs_state::crnt_version(lock)?;
        let date = self.date.read().unwrap();
        if disk_v > *date {
            mem::drop(date);
            let new_state = fs_state::load_state(lock)?;
            let mut state = self.state.write().unwrap();
            let mut date = self.date.write().unwrap();
            *state = new_state;
            *date = disk_v;
            mem::drop(date);
            mem::drop(state);
        }
        Ok(())
    }
    pub fn new(path: &Path, with_check: IndexCheck) -> VectorR<Index> {
        if !path.exists() {
            std::fs::create_dir_all(path)?;
        }
        fs_state::initialize_disk(path, || State::new(path.to_path_buf()))?;
        let lock = fs_state::shared_lock(path)?;
        let state = fs_state::load_state::<State>(&lock)?;
        let date = fs_state::crnt_version(&lock)?;
        if let IndexCheck::Sanity = with_check {
            state.work_sanity_check();
        }
        let index = Index {
            state: RwLock::new(state),
            date: RwLock::new(date),
            location: path.to_path_buf(),
        };
        Ok(index)
    }
    pub fn delete(&mut self, prefix: impl AsRef<str>, temporal_mark: SystemTime, _: &ELock) {
        let mut state = self.state.write().unwrap();
        state.remove(prefix.as_ref(), temporal_mark);
    }
    pub fn add(&mut self, dp: DataPoint, _: &ELock) {
        let mut state = self.state.write().unwrap();
        state.add(dp);
    }
    pub fn get_keys(&self, _: &Lock) -> VectorR<Vec<String>> {
        self.state.read().unwrap().keys()
    }
    pub fn search(&self, request: &dyn SearchRequest, _: &Lock) -> VectorR<Vec<(String, f32)>> {
        let state = self.state.read().unwrap();
        state.search(request)
    }
    pub fn no_nodes(&self, _: &Lock) -> usize {
        let state = self.state.read().unwrap();
        state.no_nodes()
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
