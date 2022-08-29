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

mod disk_handler;
mod merger;
mod state;
use std::path::{Path, PathBuf};
use std::sync::RwLock;
use std::{io, mem};

use disk_handler::{ELock, Lock, SLock, Version};
use state::*;
use thiserror::Error;

use crate::data_point::{DPError, DataPoint};

pub trait SearchRequest {
    fn get_query(&self) -> &[f32];
    fn get_labels(&self) -> &[String];
    fn no_results(&self) -> usize;
}

#[derive(Debug, Error)]
pub enum VectorErr {
    #[error("Serialization error: {0}")]
    SerErr(#[from] bincode::Error),
    #[error("IO error: {0}")]
    IoErr(#[from] io::Error),
    #[error("Error in data point: {0}")]
    Dp(#[from] DPError),
}

pub type VectorR<O> = Result<O, VectorErr>;

pub struct Index {
    state: RwLock<State>,
    date: RwLock<Version>,
    location: PathBuf,
}
impl Index {
    fn update(&self, lock: &disk_handler::Lock) -> VectorR<()> {
        let disk_v = disk_handler::crnt_version(lock)?;
        let date = self.date.read().unwrap();
        if disk_v > *date {
            mem::drop(date);
            let new_state = disk_handler::load_state(lock)?;
            let mut state = self.state.write().unwrap();
            let mut date = self.date.write().unwrap();
            *state = new_state;
            *date = disk_v;
            mem::drop(date);
            mem::drop(state);
        }
        Ok(())
    }
    pub fn reader(at: &Path) -> VectorR<Index> {
        let lock = disk_handler::shared_lock(at)?;
        let state = disk_handler::load_state(&lock)?;
        let date = disk_handler::crnt_version(&lock)?;
        mem::drop(lock);
        Ok(Index {
            state: RwLock::new(state),
            date: RwLock::new(date),
            location: at.to_path_buf(),
        })
    }
    pub fn writer(at: &Path) -> VectorR<Index> {
        let lock = disk_handler::shared_lock(at)?;
        let state = disk_handler::load_state(&lock)?;
        let date = disk_handler::crnt_version(&lock)?;
        state.work_sanity_check();
        mem::drop(lock);
        Ok(Index {
            state: RwLock::new(state),
            date: RwLock::new(date),
            location: at.to_path_buf(),
        })
    }
    pub fn has_resource(&self, resource: impl AsRef<str>, _: &ELock) -> bool {
        let state = self.state.read().unwrap();
        state.has_resource(resource.as_ref())
    }
    pub fn delete(&mut self, resource: impl AsRef<str>, _: &ELock) {
        let mut state = self.state.write().unwrap();
        state.remove_rosource(resource.as_ref());
    }
    pub fn add(&mut self, resource: String, dp: DataPoint, _lock: &ELock) {
        let mut state = self.state.write().unwrap();
        state.add_resource(resource, dp);
    }
    pub fn get_keys(&self, _: &Lock) -> Vec<String> {
        self.state
            .read()
            .unwrap()
            .get_keys()
            .map(|k| k.to_string())
            .collect()
    }
    pub fn search(&self, request: &dyn SearchRequest, _: &Lock) -> VectorR<Vec<(String, f32)>> {
        let state = self.state.read().unwrap();
        state.search(request)
    }
    pub fn no_nodes(&self, _: &Lock) -> usize {
        let state = self.state.read().unwrap();
        state.get_no_nodes()
    }
    pub fn get_elock(&self) -> VectorR<ELock> {
        let lock = disk_handler::exclusive_lock(&self.location)?;
        self.update(&lock)?;
        Ok(lock)
    }
    pub fn get_slock(&self) -> VectorR<SLock> {
        let lock = disk_handler::shared_lock(&self.location)?;
        self.update(&lock)?;
        Ok(lock)
    }
    pub fn get_location(&self) -> &Path {
        &self.location
    }
    pub fn commit(&self, lock: ELock) -> VectorR<()> {
        let state = self.state.read().unwrap();
        let mut date = self.date.write().unwrap();
        disk_handler::persist_state(&lock, &state)?;
        *date = disk_handler::crnt_version(&lock)?;
        Ok(())
    }
}
