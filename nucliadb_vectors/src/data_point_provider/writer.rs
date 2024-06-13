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
use crate::data_point::{self, DataPointPin, DpId, Journal, OpenDataPoint};
use crate::data_point_provider::state::*;
use crate::data_point_provider::state::{read_state, write_state};
use crate::data_point_provider::TimeSensitiveDLog;
use crate::data_point_provider::{IndexMetadata, OPENING_FLAG, STATE, TEMP_STATE, WRITING_FLAG};
use crate::data_types::dtrie_ram::DTrie;
use crate::{VectorErr, VectorR};
use fs2::FileExt;
use nucliadb_core::merge::{send_merge_request, MergePriority, MergeRequest};
use nucliadb_core::metrics::get_metrics;
use nucliadb_core::vectors::{MergeParameters, MergeResults, MergeRunner};
use nucliadb_core::{tracing, NodeResult};
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::mem;
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime};

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

pub struct PreparedMergeResults {
    inputs: HashSet<DpId>,
    destination: DataPointPin,
    metrics: MergeMetrics,
}

impl MergeResults for PreparedMergeResults {
    fn inputs(&self) -> &HashSet<DpId> {
        &self.inputs
    }

    fn output(&self) -> DpId {
        self.destination.id()
    }

    fn record_metrics(&self, source: nucliadb_core::metrics::vectors::MergeSource) {
        if self.metrics.merged == 0 {
            return;
        }

        let metrics = &get_metrics().vectors_metrics;
        metrics.record_time(source, self.metrics.seconds_elapsed);
        for input in &self.metrics.input_segment_sizes {
            metrics.record_input_segment(source, *input);
        }
        metrics.record_output_segment(source, self.metrics.output_segment_size);
    }

    fn get_metrics(&self) -> nucliadb_core::vectors::MergeMetrics {
        nucliadb_core::vectors::MergeMetrics {
            merged: self.metrics.merged,
            left: self.metrics.segments_left,
        }
    }
}

pub struct PreparedMerge {
    dtrie_copy: DTrie,
    destination: Option<DataPointPin>,
    inputs: Vec<OpenDataPoint>,
    config: VectorConfig,
    segments_left: usize,
    merge_time: SystemTime,
}

impl MergeRunner for PreparedMerge {
    fn run(&mut self) -> NodeResult<Box<dyn nucliadb_core::vectors::MergeResults>> {
        let start = Instant::now();
        let merged = data_point::merge(
            self.destination.as_ref().unwrap(),
            self.inputs
                .iter()
                .map(|dp| {
                    (
                        TimeSensitiveDLog {
                            time: dp.journal().time(),
                            dlog: &self.dtrie_copy,
                        },
                        dp,
                    )
                })
                .collect::<Vec<_>>()
                .as_slice(),
            &self.config,
            self.merge_time,
        )?;
        let metrics = MergeMetrics {
            seconds_elapsed: start.elapsed().as_secs_f64(),
            merged: self.inputs.len(),
            segments_left: self.segments_left,
            input_segment_sizes: self.inputs.iter().map(|dp| dp.journal().no_nodes()).collect(),
            output_segment_size: merged.journal().no_nodes(),
        };
        Ok(Box::new(PreparedMergeResults {
            inputs: self.inputs.iter().map(|d| d.journal().id()).collect(),
            destination: self.destination.take().unwrap(),
            metrics,
        }))
    }
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
    shard_id: String,
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

    pub fn prepare_merge(&self, parameters: MergeParameters) -> VectorR<Option<Box<dyn MergeRunner>>> {
        if self.has_uncommitted_changes {
            return Err(VectorErr::UncommittedChangesError);
        }

        if self.online_data_points.len() < parameters.segments_before_merge {
            return Ok(None);
        }

        let mut live_segments: Vec<_> = self.online_data_points.iter().collect();
        let mut nodes_in_merge = 0;
        let mut inputs = Vec::new();

        let pruning_deletions = self.delete_log.size() > parameters.maximum_deleted_entries;
        if pruning_deletions {
            // Too many deletins, order oldest segments last (first to pop()) so we can prune the delete log
            live_segments.sort_unstable_by_key(|i| std::cmp::Reverse(i.journal.time()));
        } else {
            // Order smallest segments last (first to be pop()), so they are merged first
            live_segments.sort_unstable_by_key(|i| std::cmp::Reverse(i.journal.no_nodes()));
        }

        let dtrie_copy = self.delete_log.clone();
        while nodes_in_merge < parameters.max_nodes_in_merge {
            let Some(online_data_point) = live_segments.pop() else {
                break;
            };
            let data_point_size = online_data_point.journal.no_nodes();

            if data_point_size + nodes_in_merge > parameters.max_nodes_in_merge && nodes_in_merge > 0 {
                break;
            }

            let open_data_point = data_point::open(&online_data_point.pin)?;
            inputs.push(open_data_point);
            nodes_in_merge += data_point_size;
        }

        // We need at least one segment for pruning deletes and 2 for an actual merge
        if inputs.is_empty() || (inputs.len() < 2 && !pruning_deletions) {
            return Ok(None);
        }

        let destination = DataPointPin::create_pin(self.location())?;
        Ok(Some(Box::new(PreparedMerge {
            dtrie_copy,
            destination: Some(destination),
            inputs,
            config: self.config.clone(),
            segments_left: live_segments.len() + 1,
            merge_time: SystemTime::now(),
        })))
    }

    pub fn record_merge(&mut self, merge: &dyn MergeResults) -> VectorR<()> {
        let online_data_points = mem::take(&mut self.online_data_points);
        let (merged, mut keep): (Vec<_>, Vec<_>) =
            online_data_points.into_iter().partition(|dp| merge.inputs().contains(&dp.journal.id()));

        if merged.len() != merge.inputs().len() {
            tracing::error!("Failed to merge {:?}, some merged segments are not present", self.location());
            keep.extend(merged);
            self.online_data_points = keep;
            return Err(VectorErr::MissingMergedSegments);
        }

        let merged_pin = DataPointPin::open_pin(self.location(), merge.output())?;
        let new_online_data_point = OnlineDataPoint {
            journal: merged_pin.read_journal()?,
            pin: merged_pin,
        };
        keep.push(new_online_data_point);
        self.online_data_points = keep;

        let mut new_delete_log = self.delete_log.clone();
        if let Some(oldest_age) = self.online_data_points.iter().map(|dp| dp.journal.time()).reduce(std::cmp::min) {
            new_delete_log.prune(oldest_age);
        }

        let state = State {
            delete_log: new_delete_log,
            data_point_list: self.online_data_points.iter().map(|dp| dp.journal.id()).collect(),
        };

        if let Err(err) = persist_state(&self.path, &state) {
            // Restore state
            self.online_data_points.pop().unwrap(); // Remove merged segment
            self.online_data_points.extend(merged); // Add input segments
            return Err(err);
        } else {
            // Apply state
            self.delete_log = state.delete_log;
        };

        Ok(())
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

        let _ = send_merge_request(MergeRequest {
            shard_id: self.shard_id.clone(),
            priority: MergePriority::Low,
            waiter: nucliadb_core::merge::MergeWaiter::None,
        });

        Ok(())
    }

    pub fn new(path: &Path, config: VectorConfig, shard_id: String) -> VectorR<Writer> {
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
            shard_id,
        })
    }

    pub fn open(path: &Path, shard_id: String) -> VectorR<Writer> {
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
            shard_id,
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

    pub fn config(&self) -> &VectorConfig {
        &self.config
    }

    pub fn size(&self) -> usize {
        self.number_of_embeddings
    }
}

#[cfg(test)]
mod test {

    use std::time::Duration;

    use super::*;
    use data_point::create;

    #[test]
    fn force_merge_more_than_limit() {
        let dir = tempfile::tempdir().unwrap();
        let vectors_path = dir.path().join("vectors");

        let mut writer = Writer::new(&vectors_path, VectorConfig::default(), "abc".into()).unwrap();
        let mut data_points = vec![];
        let merge_parameters = MergeParameters {
            segments_before_merge: 10,
            max_nodes_in_merge: 50_000,
            maximum_deleted_entries: 25_000,
        };

        for _ in 0..100 {
            let embeddings = vec![];
            let time = Some(SystemTime::now());
            let data_point_pin = DataPointPin::create_pin(&vectors_path).unwrap();
            create(&data_point_pin, embeddings, time, &writer.config).unwrap();

            let online_data_point = OnlineDataPoint {
                journal: data_point_pin.read_journal().unwrap(),
                pin: data_point_pin,
            };
            data_points.push(online_data_point);
        }

        writer.online_data_points = data_points;

        let metrics = writer.prepare_merge(merge_parameters).unwrap().unwrap().run().unwrap().get_metrics();

        assert_eq!(metrics.merged, 100);
        assert_eq!(metrics.left, 1);
    }

    #[test]
    fn force_merge_less_than_limit() {
        let dir = tempfile::tempdir().unwrap();
        let vectors_path = dir.path().join("vectors");

        let mut writer = Writer::new(&vectors_path, VectorConfig::default(), "abc".into()).unwrap();
        let mut data_points = vec![];
        let merge_parameters = MergeParameters {
            segments_before_merge: 1000,
            max_nodes_in_merge: 50_000,
            maximum_deleted_entries: 25_000,
        };

        for _ in 0..50 {
            let embeddings = vec![];
            let time = Some(SystemTime::now());
            let data_point_pin = DataPointPin::create_pin(&vectors_path).unwrap();
            create(&data_point_pin, embeddings, time, &writer.config).unwrap();

            let online_data_point = OnlineDataPoint {
                journal: data_point_pin.read_journal().unwrap(),
                pin: data_point_pin,
            };
            data_points.push(online_data_point);
        }

        writer.online_data_points = data_points;

        let merge_proposal = writer.prepare_merge(merge_parameters).unwrap();
        assert!(merge_proposal.is_none());
    }

    #[test]
    fn force_merge_all() {
        let dir = tempfile::tempdir().unwrap();
        let vectors_path = dir.path().join("vectors");

        let mut writer = Writer::new(&vectors_path, VectorConfig::default(), "abc".into()).unwrap();
        let mut data_points = vec![];
        let merge_parameters = MergeParameters {
            segments_before_merge: 100,
            max_nodes_in_merge: 50_000,
            maximum_deleted_entries: 25_000,
        };

        for _ in 0..100 {
            let embeddings = vec![];
            let time = Some(SystemTime::now());
            let data_point_pin = DataPointPin::create_pin(&vectors_path).unwrap();
            create(&data_point_pin, embeddings, time, &writer.config).unwrap();

            let online_data_point = OnlineDataPoint {
                journal: data_point_pin.read_journal().unwrap(),
                pin: data_point_pin,
            };
            data_points.push(online_data_point);
        }

        writer.online_data_points = data_points;

        let result = writer.prepare_merge(merge_parameters).unwrap().unwrap().run().unwrap();
        writer.record_merge(result.as_ref()).unwrap();
        let metrics = result.get_metrics();
        assert_eq!(metrics.merged, 100);
        assert_eq!(metrics.left, 1);

        let merge_proposal = writer.prepare_merge(merge_parameters).unwrap();
        assert!(merge_proposal.is_none());
    }

    #[test]
    fn merge_old_segments() {
        let dir = tempfile::tempdir().unwrap();
        let vectors_path = dir.path().join("vectors");

        // Writer with a single empty vector
        let mut writer = Writer::new(&vectors_path, VectorConfig::default(), "abc".into()).unwrap();
        let merge_parameters = MergeParameters {
            segments_before_merge: 1,
            max_nodes_in_merge: 50_000,
            maximum_deleted_entries: 10,
        };
        let embeddings = vec![];
        let time = Some(SystemTime::now());
        let data_point_pin = DataPointPin::create_pin(&vectors_path).unwrap();
        create(&data_point_pin, embeddings, time, &writer.config).unwrap();

        let online_data_point = OnlineDataPoint {
            journal: data_point_pin.read_journal().unwrap(),
            pin: data_point_pin,
        };
        writer.online_data_points = vec![online_data_point];

        // Should not merge, there is no reason to with a single segment
        assert!(writer.prepare_merge(merge_parameters).unwrap().is_none());

        // Create a bunch of deletions
        let past = SystemTime::now().checked_sub(Duration::from_secs(5)).unwrap();
        for i in 0..20 {
            writer.record_delete(format!("delete{i:02}").as_bytes(), past);
        }
        writer.commit().unwrap();
        assert_eq!(writer.delete_log.size(), 20);

        // Should merge, just to prune the deletion tree
        let result = writer.prepare_merge(merge_parameters).unwrap().unwrap().run().unwrap();
        writer.record_merge(result.as_ref()).unwrap();

        assert_eq!(writer.delete_log.size(), 0);

        // Nothing else to merge
        let merge_proposal = writer.prepare_merge(merge_parameters).unwrap();
        assert!(merge_proposal.is_none());
    }
}
