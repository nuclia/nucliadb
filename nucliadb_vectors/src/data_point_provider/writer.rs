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

use crate::data_point::{self, DataPointPin, DpId};
use crate::data_point_provider::state::*;
use crate::data_point_provider::TimeSensitiveDLog;
use crate::data_point_provider::{IndexMetadata, OPENING_FLAG, STATE, TEMP_STATE, WRITING_FLAG};
use crate::data_types::dtrie_ram::DTrie;
use crate::{VectorErr, VectorR};
use fs2::FileExt;
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Write};
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
    let temporal_file = temporal_options.open(&temporal_path)?;

    let mut temporal_buffer = BufWriter::new(temporal_file);
    bincode::serialize_into(&mut temporal_buffer, state)?;
    temporal_buffer.flush()?;
    std::fs::rename(&temporal_path, state_path)?;

    Ok(())
}

#[derive(Debug, Clone, Copy)]
pub struct MergeMetrics {
    pub merged: usize,
    pub segments_left: usize,
}

pub struct Writer {
    has_uncommitted_changes: bool,
    metadata: IndexMetadata,
    path: PathBuf,
    added_data_points: Vec<DataPointPin>,
    removed_data_points: HashSet<DpId>,
    added_to_delete_log: Vec<(Vec<u8>, SystemTime)>,
    data_points: Vec<DataPointPin>,
    delete_log: DTrie,
    dimension: Option<u64>,
    #[allow(unused)]
    writing: File,
}

impl Writer {
    pub fn add_data_point(&mut self, pin: DataPointPin) -> VectorR<()> {
        let data_point = pin.open_data_point()?;
        let data_point_len = data_point.stored_len();

        if self.dimension != data_point_len {
            return Err(VectorErr::InconsistentDimensions);
        }

        self.added_data_points.push(pin);
        self.has_uncommitted_changes = true;
        Ok(())
    }

    pub fn record_delete(&mut self, prefix: &[u8], temporal_mark: SystemTime) {
        self.added_to_delete_log.push((prefix.to_vec(), temporal_mark));
        self.has_uncommitted_changes = true;
    }

    pub fn merge(&mut self) -> VectorR<MergeMetrics> {
        if self.has_uncommitted_changes {
            return Err(VectorErr::UncommittedChangesError);
        }

        let channel = self.metadata.channel;
        let similarity = self.metadata.similarity;
        let max_nodes_per_segment = 50_000;
        let force_merge_capacity = 100;
        let mut blocked_segments = vec![];
        let mut live_segments = mem::take(&mut self.data_points);
        let mut buffer = Vec::with_capacity(force_merge_capacity);

        while buffer.len() < force_merge_capacity {
            let Some(data_point_pin) = live_segments.pop() else {
                break;
            };
            let Ok(data_point_journal) = data_point_pin.read_journal() else {
                continue;
            };
            if data_point_journal.no_nodes() >= max_nodes_per_segment {
                blocked_segments.push(data_point_pin);
            } else {
                let delete_log = TimeSensitiveDLog {
                    time: data_point_journal.time(),
                    dlog: &self.delete_log,
                };
                let data_point_id = data_point_pin.id();
                buffer.push((delete_log, data_point_id));
            }
        }

        let merged_pin = DataPointPin::create_pin(self.location())?;
        data_point::merge_data_points(&merged_pin, &buffer, similarity, channel)?;
        blocked_segments.push(merged_pin);
        blocked_segments.extend(live_segments);
        self.data_points = blocked_segments;

        let metrics = MergeMetrics {
            merged: buffer.len(),
            segments_left: self.data_points.len(),
        };

        let mut state = State::new();
        state.delete_log = self.delete_log.clone();
        state.available_data_points = self.data_points.iter().map(|i| i.id()).collect();
        persist_state(self.location(), &state)?;

        Ok(metrics)
    }

    pub fn abort(&mut self) {
        self.added_to_delete_log.clear();
        self.removed_data_points.clear();
        self.added_data_points.clear();
    }

    pub fn commit(&mut self) -> VectorR<()> {
        if !self.has_uncommitted_changes {
            return Ok(());
        }

        let added_to_delete_log = mem::take(&mut self.added_to_delete_log);
        let removed_data_points = mem::take(&mut self.removed_data_points);
        let added_data_points = mem::take(&mut self.added_data_points);
        let current_data_points = mem::take(&mut self.data_points);

        let mut state = State::default();
        state.delete_log = self.delete_log.clone();

        for pin in &current_data_points {
            if removed_data_points.contains(&pin.id()) {
                continue;
            }
            state.available_data_points.push(pin.id());
        }

        for pin in &added_data_points {
            if removed_data_points.contains(&pin.id()) {
                continue;
            }
            state.available_data_points.push(pin.id());
        }

        for (entry, time) in &added_to_delete_log {
            state.delete_log.insert(entry, *time);
        }

        if let Err(err) = persist_state(&self.path, &state) {
            self.data_points = current_data_points;
            return Err(err);
        };

        let updated_delete_log = state.delete_log;
        let mut alive_data_points = Vec::new();

        for pin in added_data_points {
            if removed_data_points.contains(&pin.id()) {
                continue;
            }
            alive_data_points.push(pin);
        }

        for pin in current_data_points {
            if removed_data_points.contains(&pin.id()) {
                continue;
            }
            alive_data_points.push(pin);
        }

        self.data_points = alive_data_points;
        self.delete_log = updated_delete_log;
        self.has_uncommitted_changes = false;
        Ok(())
    }

    pub fn new(path: &Path, metadata: IndexMetadata) -> VectorR<Writer> {
        std::fs::create_dir(path)?;
        File::create(path.join(OPENING_FLAG))?;

        let writing_path = path.join(WRITING_FLAG);
        let writing_file = File::create(writing_path)?;

        if writing_file.try_lock_exclusive().is_err() {
            return Err(VectorErr::MultipleWritersError);
        }

        metadata.write(path)?;
        persist_state(path, &State::default())?;

        Ok(Writer {
            metadata,
            path: path.to_path_buf(),
            added_data_points: Vec::new(),
            removed_data_points: HashSet::new(),
            added_to_delete_log: Vec::new(),
            data_points: Vec::new(),
            delete_log: DTrie::new(),
            has_uncommitted_changes: false,
            dimension: None,
            writing: writing_file,
        })
    }

    pub fn open(path: &Path) -> VectorR<Writer> {
        let writing_path = path.join(WRITING_FLAG);
        let writing_file = File::open(writing_path)?;

        if writing_file.try_lock_exclusive().is_err() {
            return Err(VectorErr::MultipleWritersError);
        }

        let lock_path = path.join(OPENING_FLAG);
        let lock_file = File::open(lock_path)?;
        lock_file.lock_shared()?;

        let metadata = IndexMetadata::open(path)?.map(Ok).unwrap_or_else(|| {
            // Old indexes may not have this file so in that case the
            // metadata file they should have is created.
            let metadata = IndexMetadata::default();
            metadata.write(path).map(|_| metadata)
        })?;

        let state_path = path.join(STATE);
        let state_file = File::open(state_path)?;
        let mut state: State = bincode::deserialize_from(BufReader::new(state_file))?;

        let delete_log = mem::take(&mut state.delete_log);
        let mut dimension = None;
        let mut data_points = Vec::new();
        for data_point_id in state.dpid_iter() {
            let data_point_pin = DataPointPin::open_pin(path, data_point_id)?;

            if dimension.is_none() {
                let data_point = data_point_pin.open_data_point()?;
                dimension = data_point.stored_len();
            }

            data_points.push(data_point_pin);
        }

        Ok(Writer {
            metadata,
            data_points,
            delete_log,
            dimension,
            added_data_points: Vec::new(),
            removed_data_points: HashSet::new(),
            added_to_delete_log: Vec::new(),
            path: path.to_path_buf(),
            has_uncommitted_changes: false,
            writing: writing_file,
        })
    }

    pub fn location(&self) -> &Path {
        &self.path
    }

    pub fn metadata(&self) -> &IndexMetadata {
        &self.metadata
    }
}
