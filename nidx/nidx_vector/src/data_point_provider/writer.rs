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

use crate::config::VectorConfig;
use crate::data_point::{self, DataPointPin, Journal};
use crate::data_point_provider::state::*;
use crate::data_point_provider::state::{read_state, write_state};
use crate::data_point_provider::{IndexMetadata, OPENING_FLAG, STATE, TEMP_STATE, WRITING_FLAG};
use crate::data_types::dtrie_ram::DTrie;
use crate::{VectorErr, VectorR};
use fs2::FileExt;
use std::fs::{File, OpenOptions};
use std::mem;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

fn persist_state(path: &Path, state: &State) -> VectorR<()> {
    let temporal_path = path.join(TEMP_STATE);
    let state_path = path.join(STATE);

    let mut temporal_options = OpenOptions::new();
    temporal_options.write(true);
    temporal_options.create(true);
    temporal_options.truncate(true);
    let mut temporal_file = temporal_options.open(&temporal_path)?;

    write_state(&mut temporal_file, state)?;
    std::fs::rename(&temporal_path, state_path)?;

    Ok(())
}

struct OnlineDataPoint {
    pin: DataPointPin,
    journal: Journal,
}

pub struct Writer {
    has_uncommitted_changes: bool,
    config: VectorConfig,
    path: PathBuf,
    added_data_points: Vec<DataPointPin>,
    added_to_delete_log: Vec<(Vec<u8>, SystemTime)>,
    online_data_points: Vec<OnlineDataPoint>,
    delete_log: DTrie,
    /// Only set when not specified by the vector config. TODO: To be removed
    dimension: Option<usize>,
    number_of_embeddings: usize,
    #[allow(unused)]
    writing: File,
}

impl Writer {
    pub fn add_data_point(&mut self, data_point_pin: DataPointPin) -> VectorR<()> {
        let data_point = data_point::open(&data_point_pin)?;
        let data_point_len = data_point.stored_len();

        if self.dimension.is_some() && (self.dimension != data_point_len) {
            return Err(VectorErr::InconsistentDimensions);
        }

        self.added_data_points.push(data_point_pin);
        self.has_uncommitted_changes = true;
        Ok(())
    }

    pub fn record_delete(&mut self, prefix: &[u8], temporal_mark: SystemTime) {
        self.added_to_delete_log.push((prefix.to_vec(), temporal_mark));
        self.has_uncommitted_changes = true;
    }

    pub fn abort(&mut self) {
        self.added_to_delete_log.clear();
        self.added_data_points.clear();
    }

    pub fn commit(&mut self) -> VectorR<()> {
        if !self.has_uncommitted_changes {
            return Ok(());
        }

        let added_to_delete_log = mem::take(&mut self.added_to_delete_log);
        let added_data_points = mem::take(&mut self.added_data_points);
        let current_data_points = mem::take(&mut self.online_data_points);

        let mut state = State::new();
        state.delete_log = self.delete_log.clone();

        for pin in current_data_points.iter().map(|i| &i.pin) {
            state.data_point_list.push(pin.id());
        }

        for pin in added_data_points.iter() {
            state.data_point_list.push(pin.id());
        }

        for (entry, time) in &added_to_delete_log {
            state.delete_log.insert(entry, *time);
        }

        if let Err(err) = persist_state(&self.path, &state) {
            self.online_data_points = current_data_points;
            return Err(err);
        };

        let updated_delete_log = state.delete_log;
        let mut alive_data_points = Vec::new();
        let mut number_of_embeddings = 0;

        for pin in added_data_points {
            let Ok(journal) = pin.read_journal() else {
                continue;
            };
            let online_data_point = OnlineDataPoint {
                pin,
                journal,
            };

            number_of_embeddings += online_data_point.journal.no_nodes();
            alive_data_points.push(online_data_point);
        }

        for online_data_point in current_data_points {
            number_of_embeddings += online_data_point.journal.no_nodes();
            alive_data_points.push(online_data_point);
        }

        self.online_data_points = alive_data_points;
        self.delete_log = updated_delete_log;
        self.has_uncommitted_changes = false;
        self.number_of_embeddings = number_of_embeddings;

        Ok(())
    }

    pub fn new(path: &Path, config: VectorConfig) -> VectorR<Writer> {
        std::fs::create_dir(path)?;
        File::create(path.join(OPENING_FLAG))?;

        let writing_path = path.join(WRITING_FLAG);
        let writing_file = File::create(writing_path)?;

        if writing_file.try_lock_exclusive().is_err() {
            return Err(VectorErr::MultipleWritersError);
        }

        IndexMetadata::write(&config, path)?;
        persist_state(path, &State::default())?;

        Ok(Writer {
            config,
            path: path.to_path_buf(),
            added_data_points: Vec::new(),
            added_to_delete_log: Vec::new(),
            online_data_points: Vec::new(),
            delete_log: DTrie::new(),
            has_uncommitted_changes: false,
            dimension: None,
            number_of_embeddings: 0,
            writing: writing_file,
        })
    }

    pub fn open(path: &Path) -> VectorR<Writer> {
        let writing_path = path.join(WRITING_FLAG);
        let writing_file = File::create(writing_path)?;

        if writing_file.try_lock_exclusive().is_err() {
            return Err(VectorErr::MultipleWritersError);
        }

        let lock_path = path.join(OPENING_FLAG);
        let lock_file = File::create(lock_path)?;
        lock_file.lock_shared()?;

        let config = IndexMetadata::open(path)?.map(Ok).unwrap_or_else(|| {
            // Old indexes may not have this file so in that case the
            // metadata file they should have is created.
            let config = VectorConfig::default();
            IndexMetadata::write(&config, path).map(|_| config)
        })?;

        let state_path = path.join(STATE);
        let state_file = File::open(state_path)?;
        let state = read_state(&state_file)?;
        let data_point_list = state.data_point_list;
        let delete_log = state.delete_log;
        let mut dimension = None;
        let mut number_of_embeddings = 0;
        let mut online_data_points = Vec::new();

        for data_point_id in data_point_list {
            let data_point_pin = DataPointPin::open_pin(path, data_point_id)?;
            let data_point_journal = data_point_pin.read_journal()?;
            let online_data_point = OnlineDataPoint {
                pin: data_point_pin,
                journal: data_point_journal,
            };

            number_of_embeddings += online_data_point.journal.no_nodes();
            online_data_points.push(online_data_point);
        }

        if !config.known_dimensions() {
            if let Some(online_data_point) = online_data_points.first() {
                let open_data_point = data_point::open(&online_data_point.pin)?;
                dimension = open_data_point.stored_len();
            }
        }

        Ok(Writer {
            config,
            online_data_points,
            delete_log,
            dimension,
            number_of_embeddings,
            added_data_points: Vec::new(),
            added_to_delete_log: Vec::new(),
            path: path.to_path_buf(),
            has_uncommitted_changes: false,
            writing: writing_file,
        })
    }

    /// This must be used only by replication and should be
    /// deleted as soon as possible.
    pub fn reload(&mut self) -> VectorR<()> {
        if self.has_uncommitted_changes {
            return Err(VectorErr::UncommittedChangesError);
        }

        let state_path = self.path.join(STATE);
        let state_file = File::open(state_path)?;
        let state = read_state(&state_file)?;
        let data_point_list = state.data_point_list;
        let new_delete_log = state.delete_log;
        let mut new_dimension = self.dimension;
        let mut new_number_of_embeddings = 0;
        let mut new_data_points = Vec::new();

        for data_point_id in data_point_list {
            let data_point_pin = DataPointPin::open_pin(&self.path, data_point_id)?;
            let data_point_journal = data_point_pin.read_journal()?;

            if !self.config.known_dimensions() && new_dimension.is_none() {
                let data_point = data_point::open(&data_point_pin)?;
                new_dimension = data_point.stored_len();
            }

            new_number_of_embeddings += data_point_journal.no_nodes();
            let online_data_point = OnlineDataPoint {
                pin: data_point_pin,
                journal: data_point_journal,
            };

            new_data_points.push(online_data_point);
        }

        self.delete_log = new_delete_log;
        self.online_data_points = new_data_points;
        self.dimension = new_dimension;
        self.number_of_embeddings = new_number_of_embeddings;

        Ok(())
    }

    pub fn location(&self) -> &Path {
        &self.path
    }

    pub fn config(&self) -> &VectorConfig {
        &self.config
    }

    pub fn size(&self) -> usize {
        self.number_of_embeddings
    }
}
