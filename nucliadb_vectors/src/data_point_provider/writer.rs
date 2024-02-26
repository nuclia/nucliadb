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

use crate::data_point::{DataPoint, DataPointPin, DpId};
use crate::data_point_provider::state::*;
use crate::data_point_provider::{IndexMetadata, OPEN_LOCK, STATE, TEMP_STATE};
use crate::data_types::dtrie_ram::DTrie;
use crate::{VectorErr, VectorR};
use fs2::FileExt;
use nucliadb_core::tracing::*;
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

#[derive(Default)]
pub struct GarbageCollectionMetrics {
    pub unknown_items: usize,
    pub partial_data_points: usize,
    pub garbage_not_deleted: usize,
    pub garbage_deleted: usize,
    pub total: usize,
}

pub fn collect_garbage(path: &Path) -> VectorR<GarbageCollectionMetrics> {
    let lock_path = path.join(OPEN_LOCK);
    let lock_file = File::open(lock_path)?;
    lock_file.lock_exclusive()?;

    let mut metrics = GarbageCollectionMetrics::default();
    for dir_entry in std::fs::read_dir(path)? {
        let entry = dir_entry?;
        let dir_path = entry.path();
        let name = entry.file_name().to_string_lossy().to_string();
        if dir_path.is_file() {
            continue;
        }

        let Ok(data_point_id) = DpId::parse_str(&name) else {
            info!("Unknown item {dir_path:?} found");
            metrics.unknown_items += 1;
            continue;
        };

        metrics.total += 1;

        let Ok(is_pinned) = DataPointPin::is_pinned(path, data_point_id) else {
            warn!("Error checking {data_point_id}");
            metrics.partial_data_points += 1;
            continue;
        };

        if is_pinned {
            continue;
        }

        match DataPoint::delete(path, data_point_id) {
            Ok(_) => metrics.garbage_deleted += 1,
            Err(err) => {
                warn!("{name} is garbage not deleted: {err}");
                metrics.garbage_not_deleted += 1;
            }
        }
    }

    Ok(metrics)
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
            if !removed_data_points.contains(&pin.id()) {
                state.available_data_points.push(pin.id());
            }
        }

        for pin in &added_data_points {
            if !removed_data_points.contains(&pin.id()) {
                state.available_data_points.push(pin.id());
            }
        }

        for (entry, time) in &added_to_delete_log {
            state.delete_log.insert(entry, *time);
        }

        if let Err(err) = persist_state(&self.path, &state) {
            self.added_to_delete_log = added_to_delete_log;
            self.removed_data_points = removed_data_points;
            self.added_data_points = added_data_points;
            self.data_points = current_data_points;
            return Err(err);
        };

        let mut alive_data_points = Vec::new();

        for pin in added_data_points {
            if removed_data_points.contains(&pin.id()) {
                alive_data_points.push(pin);
            }
        }

        for pin in current_data_points {
            if removed_data_points.contains(&pin.id()) {
                alive_data_points.push(pin);
            }
        }

        self.data_points = alive_data_points;
        self.delete_log = state.delete_log;
        self.has_uncommitted_changes = false;
        Ok(())
    }

    pub fn new(path: &Path, metadata: IndexMetadata) -> VectorR<Writer> {
        std::fs::create_dir(path)?;
        File::create(path.join(OPEN_LOCK))?;

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
        })
    }

    pub fn open(path: &Path) -> VectorR<Writer> {
        let lock_path = path.join(OPEN_LOCK);
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
        })
    }

    pub fn location(&self) -> &Path {
        &self.path
    }

    pub fn metadata(&self) -> &IndexMetadata {
        &self.metadata
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn gc_with_some_garbage() {
        let workspace = tempfile::tempdir().unwrap();
        let index_path = workspace.path();
        let lock_path = index_path.join(OPEN_LOCK);
        File::create(&lock_path).unwrap();

        DataPointPin::create_pin(index_path).unwrap();
        DataPointPin::create_pin(index_path).unwrap();
        let pin = DataPointPin::create_pin(index_path).unwrap();

        let metrics = collect_garbage(index_path).unwrap();

        assert!(pin.path().is_dir());
        assert_eq!(metrics.total, 3);
        assert_eq!(metrics.garbage_deleted, 2);
        assert_eq!(metrics.garbage_not_deleted, 0);
    }

    #[test]
    fn gc_all_garbage() {
        let workspace = tempfile::tempdir().unwrap();
        let index_path = workspace.path();
        let lock_path = index_path.join(OPEN_LOCK);
        File::create(&lock_path).unwrap();

        DataPointPin::create_pin(index_path).unwrap();
        DataPointPin::create_pin(index_path).unwrap();
        DataPointPin::create_pin(index_path).unwrap();

        let metrics = collect_garbage(index_path).unwrap();

        assert_eq!(metrics.total, 3);
        assert_eq!(metrics.garbage_deleted, 3);
        assert_eq!(metrics.garbage_not_deleted, 0);
    }

    #[test]
    fn gc_every_data_point_pinned() {
        let workspace = tempfile::tempdir().unwrap();
        let index_path = workspace.path();
        let lock_path = index_path.join(OPEN_LOCK);
        File::create(&lock_path).unwrap();

        let pin_0 = DataPointPin::create_pin(index_path).unwrap();
        let pin_1 = DataPointPin::create_pin(index_path).unwrap();
        let pin_2 = DataPointPin::create_pin(index_path).unwrap();

        let metrics = collect_garbage(index_path).unwrap();

        assert!(pin_0.path().is_dir());
        assert!(pin_1.path().is_dir());
        assert!(pin_2.path().is_dir());
        assert_eq!(metrics.total, 3);
        assert_eq!(metrics.garbage_deleted, 0);
        assert_eq!(metrics.garbage_not_deleted, 0);
    }

    #[test]
    fn gc_no_data_points() {
        let workspace = tempfile::tempdir().unwrap();
        let index_path = workspace.path();
        let lock_path = index_path.join(OPEN_LOCK);
        File::create(&lock_path).unwrap();

        let metrics = collect_garbage(index_path).unwrap();

        assert_eq!(metrics.total, 0);
        assert_eq!(metrics.garbage_deleted, 0);
        assert_eq!(metrics.garbage_not_deleted, 0);
    }
}
