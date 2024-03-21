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

use crate::data_point::{self, DataPointPin, Journal};
use crate::data_point_provider::state::*;
use crate::data_point_provider::state::{read_state, write_state};
use crate::data_point_provider::TimeSensitiveDLog;
use crate::data_point_provider::{IndexMetadata, OPENING_FLAG, STATE, TEMP_STATE, WRITING_FLAG};
use crate::data_types::dtrie_ram::DTrie;
use crate::{VectorErr, VectorR};
use fs2::FileExt;
use nucliadb_core::tracing;
use std::fs::{File, OpenOptions};
use std::mem;
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime};

const MAX_NODES_IN_MERGE: &str = "MAX_NODES_IN_MERGE";
const SEGMENTS_BEFORE_MERGE: &str = "SEGMENTS_BEFORE_MERGE";

#[derive(Debug, Clone, Copy)]
pub struct MergeParameters {
    pub max_nodes_in_merge: usize,
    pub segments_before_merge: usize,
}

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

#[derive(Debug, Clone, Default)]
pub struct MergeMetrics {
    pub seconds_elapsed: f64,
    pub merged: usize,
    pub segments_left: usize,
    pub input_segment_sizes: Vec<usize>,
    pub output_segment_size: usize,
}

struct OnlineDataPoint {
    pin: DataPointPin,
    journal: Journal,
}

pub struct Writer {
    on_commit_merge_parameters: MergeParameters,
    has_uncommitted_changes: bool,
    metadata: IndexMetadata,
    path: PathBuf,
    added_data_points: Vec<DataPointPin>,
    added_to_delete_log: Vec<(Vec<u8>, SystemTime)>,
    online_data_points: Vec<OnlineDataPoint>,
    delete_log: DTrie,
    dimension: Option<u64>,
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

    pub fn merge(&mut self, parameters: MergeParameters) -> VectorR<MergeMetrics> {
        if self.has_uncommitted_changes {
            return Err(VectorErr::UncommittedChangesError);
        }

        if self.online_data_points.len() < parameters.segments_before_merge {
            return Ok(MergeMetrics {
                segments_left: self.online_data_points.len(),
                ..Default::default()
            });
        }

        let start = Instant::now();
        let similarity = self.metadata.similarity;
        let mut blocked_segments = Vec::new();
        let mut being_merged = Vec::new();
        let mut live_segments = mem::take(&mut self.online_data_points);
        let mut nodes_in_merge = 0;
        let mut buffer = Vec::new();

        // Order smallest segments last (first to be pop()), so they are merged first
        live_segments.sort_unstable_by_key(|i| std::cmp::Reverse(i.journal.no_nodes()));

        let mut input_segment_sizes = vec![];
        while nodes_in_merge < parameters.max_nodes_in_merge {
            let Some(online_data_point) = live_segments.pop() else {
                break;
            };
            let data_point_size = online_data_point.journal.no_nodes();

            if data_point_size + nodes_in_merge > parameters.max_nodes_in_merge {
                blocked_segments.push(online_data_point);
                break;
            } else {
                let delete_log = TimeSensitiveDLog {
                    time: online_data_point.journal.time(),
                    dlog: &self.delete_log,
                };
                let open_data_point = data_point::open(&online_data_point.pin)?;
                being_merged.push(online_data_point);
                buffer.push((delete_log, open_data_point));
                nodes_in_merge += data_point_size;
                input_segment_sizes.push(data_point_size);
            }
        }

        if buffer.len() < 2 {
            self.online_data_points.extend(blocked_segments);
            self.online_data_points.extend(being_merged);
            self.online_data_points.extend(live_segments);
            return Ok(MergeMetrics {
                segments_left: self.online_data_points.len(),
                ..Default::default()
            });
        }

        let merged_pin = DataPointPin::create_pin(self.location())?;

        if let Err(err) = data_point::merge(&merged_pin, &buffer, similarity) {
            self.online_data_points.extend(blocked_segments);
            self.online_data_points.extend(being_merged);
            self.online_data_points.extend(live_segments);
            return Err(err);
        }

        let new_online_data_point = OnlineDataPoint {
            journal: merged_pin.read_journal()?,
            pin: merged_pin,
        };
        let output_segment_size = new_online_data_point.journal.no_nodes();

        blocked_segments.extend(live_segments);
        blocked_segments.push(new_online_data_point);
        let online_data_points = blocked_segments;

        let metrics = MergeMetrics {
            merged: buffer.len(),
            segments_left: online_data_points.len(),
            input_segment_sizes,
            output_segment_size,
            seconds_elapsed: start.elapsed().as_secs_f64(),
        };

        let mut oldest_age = SystemTime::now();
        let mut persisted_data_points = Vec::new();

        for data_point in online_data_points.iter() {
            let data_point_id = data_point.pin.id();
            let data_point_time = data_point.journal.time();
            persisted_data_points.push(data_point_id);
            oldest_age = std::cmp::min(oldest_age, data_point_time);
        }

        let mut state = State::new();
        state.data_point_list = persisted_data_points;
        state.delete_log = self.delete_log.clone();
        persist_state(self.location(), &state)?;

        self.delete_log.prune(oldest_age);
        self.online_data_points = online_data_points;

        Ok(metrics)
    }

    pub fn abort(&mut self) {
        self.added_to_delete_log.clear();
        self.added_data_points.clear();
    }

    pub fn commit(&mut self) -> VectorR<MergeMetrics> {
        if !self.has_uncommitted_changes {
            return Ok(MergeMetrics::default());
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

        let merge_result = self.merge(self.on_commit_merge_parameters);
        if let Err(merge_error) = &merge_result {
            tracing::error!("Merge error: {merge_error:?}")
        }

        merge_result
    }

    pub fn new(path: &Path, metadata: IndexMetadata) -> VectorR<Writer> {
        std::fs::create_dir(path)?;
        File::create(path.join(OPENING_FLAG))?;

        let writing_path = path.join(WRITING_FLAG);
        let writing_file = File::create(writing_path)?;
        let max_nodes_in_merge: usize = match std::env::var(MAX_NODES_IN_MERGE) {
            Ok(v) => v.parse().unwrap_or(50_000),
            Err(_) => 50_000,
        };
        let segments_before_merge: usize = match std::env::var(SEGMENTS_BEFORE_MERGE) {
            Ok(v) => v.parse().unwrap_or(100),
            Err(_) => 100,
        };
        let on_commit_merge_parameters = MergeParameters {
            max_nodes_in_merge,
            segments_before_merge,
        };

        if writing_file.try_lock_exclusive().is_err() {
            return Err(VectorErr::MultipleWritersError);
        }

        metadata.write(path)?;
        persist_state(path, &State::default())?;

        Ok(Writer {
            metadata,
            on_commit_merge_parameters,
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

        let metadata = IndexMetadata::open(path)?.map(Ok).unwrap_or_else(|| {
            // Old indexes may not have this file so in that case the
            // metadata file they should have is created.
            let metadata = IndexMetadata::default();
            metadata.write(path).map(|_| metadata)
        })?;
        let max_nodes_in_merge: usize = match std::env::var(MAX_NODES_IN_MERGE) {
            Ok(v) => v.parse().unwrap_or(50_000),
            Err(_) => 50_000,
        };
        let segments_before_merge: usize = match std::env::var(SEGMENTS_BEFORE_MERGE) {
            Ok(v) => v.parse().unwrap_or(100),
            Err(_) => 100,
        };
        let on_commit_merge_parameters = MergeParameters {
            max_nodes_in_merge,
            segments_before_merge,
        };

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

        if dimension.is_none() {
            if let Some(online_data_point) = online_data_points.first() {
                let open_data_point = data_point::open(&online_data_point.pin)?;
                dimension = open_data_point.stored_len();
            }
        }

        Ok(Writer {
            metadata,
            online_data_points,
            delete_log,
            dimension,
            number_of_embeddings,
            on_commit_merge_parameters,
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

            if new_dimension.is_none() {
                let data_point = data_point::open(&data_point_pin)?;
                new_dimension = data_point.stored_len();
            }

            let online_data_point = OnlineDataPoint {
                pin: data_point_pin,
                journal: data_point_journal,
            };

            new_number_of_embeddings += data_point_journal.no_nodes();
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

    pub fn metadata(&self) -> &IndexMetadata {
        &self.metadata
    }

    pub fn size(&self) -> usize {
        self.number_of_embeddings
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use crate::data_point::Similarity;
    use data_point::create;

    #[test]
    fn force_merge_more_than_limit() {
        let dir = tempfile::tempdir().unwrap();
        let vectors_path = dir.path().join("vectors");

        let mut writer = Writer::new(&vectors_path, IndexMetadata::default()).unwrap();
        let mut data_points = vec![];
        let merge_parameters = MergeParameters {
            segments_before_merge: 10,
            ..writer.on_commit_merge_parameters
        };

        for _ in 0..100 {
            let similarity = Similarity::Cosine;
            let embeddings = vec![];
            let time = Some(SystemTime::now());
            let data_point_pin = DataPointPin::create_pin(&vectors_path).unwrap();
            create(&data_point_pin, embeddings, time, similarity).unwrap();

            let online_data_point = OnlineDataPoint {
                journal: data_point_pin.read_journal().unwrap(),
                pin: data_point_pin,
            };
            data_points.push(online_data_point);
        }

        writer.online_data_points = data_points;

        let metrics = writer.merge(merge_parameters).unwrap();
        assert_eq!(metrics.merged, 100);
        assert_eq!(metrics.segments_left, 1);
    }

    #[test]
    fn force_merge_less_than_limit() {
        let dir = tempfile::tempdir().unwrap();
        let vectors_path = dir.path().join("vectors");

        let mut writer = Writer::new(&vectors_path, IndexMetadata::default()).unwrap();
        let mut data_points = vec![];
        let merge_parameters = MergeParameters {
            segments_before_merge: 1000,
            ..writer.on_commit_merge_parameters
        };

        for _ in 0..50 {
            let similarity = Similarity::Cosine;
            let embeddings = vec![];
            let time = Some(SystemTime::now());
            let data_point_pin = DataPointPin::create_pin(&vectors_path).unwrap();
            create(&data_point_pin, embeddings, time, similarity).unwrap();

            let online_data_point = OnlineDataPoint {
                journal: data_point_pin.read_journal().unwrap(),
                pin: data_point_pin,
            };
            data_points.push(online_data_point);
        }

        writer.online_data_points = data_points;

        let metrics = writer.merge(merge_parameters).unwrap();
        assert_eq!(metrics.merged, 0);
        assert_eq!(metrics.segments_left, 50);
    }

    #[test]
    fn force_merge_all() {
        let dir = tempfile::tempdir().unwrap();
        let vectors_path = dir.path().join("vectors");

        let mut writer = Writer::new(&vectors_path, IndexMetadata::default()).unwrap();
        let mut data_points = vec![];
        let merge_parameters = writer.on_commit_merge_parameters;

        for _ in 0..100 {
            let similarity = Similarity::Cosine;
            let embeddings = vec![];
            let time = Some(SystemTime::now());
            let data_point_pin = DataPointPin::create_pin(&vectors_path).unwrap();
            create(&data_point_pin, embeddings, time, similarity).unwrap();

            let online_data_point = OnlineDataPoint {
                journal: data_point_pin.read_journal().unwrap(),
                pin: data_point_pin,
            };
            data_points.push(online_data_point);
        }

        writer.online_data_points = data_points;

        let metrics = writer.merge(merge_parameters).unwrap();
        assert_eq!(metrics.merged, 100);
        assert_eq!(metrics.segments_left, 1);

        let metrics = writer.merge(merge_parameters).unwrap();
        assert_eq!(metrics.merged, 0);
        assert_eq!(metrics.segments_left, 1);
    }
}
