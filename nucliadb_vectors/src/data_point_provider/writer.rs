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

use nucliadb_core::tracing::*;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Write};
use std::mem;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use crate::data_point::{DataPoint, DataPointPin, DpId};
use crate::data_point_provider::state::*;
use crate::data_point_provider::{IndexMetadata, OPEN_LOCK, STATE, TEMP_STATE};
use crate::data_types::dtrie_ram::DTrie;
use crate::{VectorErr, VectorR};

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
pub struct GarbageCollectorMetrics {
    pub unknown_items: usize,
    pub partial_data_points: usize,
    pub garbage_not_deleted: usize,
    pub garbage_deleted: usize,
    pub total: usize,
}

pub struct Writer {
    has_uncommitted_changes: bool,
    metadata: IndexMetadata,
    path: PathBuf,
    data_points: HashMap<DpId, DataPointPin>,
    delete_log: DTrie,
    number_of_embeddings: usize,
    dimension: Option<u64>,
}

impl Writer {
    pub fn collect_garbage(&self) -> VectorR<GarbageCollectorMetrics> {
        let mut metrics = GarbageCollectorMetrics::default();

        for dir_entry in std::fs::read_dir(&self.path)? {
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

            let Ok(is_pinned) = DataPointPin::is_pinned(&self.path, data_point_id) else {
                warn!("Error checking {data_point_id}");
                metrics.partial_data_points += 1;
                continue;
            };

            if is_pinned {
                continue;
            }

            match DataPoint::delete(&self.path, data_point_id) {
                Ok(_) => metrics.garbage_deleted += 1,
                Err(err) => {
                    warn!("{name} is garbage not deleted: {err}");
                    metrics.garbage_not_deleted += 1;
                }
            }
        }

        Ok(metrics)
    }

    pub fn add_data_point(&mut self, pin: DataPointPin) -> VectorR<()> {
        let data_point = pin.open_data_point()?;
        let data_point_len = data_point.stored_len();

        if self.dimension != data_point_len {
            return Err(VectorErr::InconsistentDimensions);
        }

        let journal = pin.read_journal()?;
        self.data_points.insert(journal.id(), pin);
        self.number_of_embeddings += journal.no_nodes();
        self.has_uncommitted_changes = true;
        Ok(())
    }

    pub fn record_delete(&mut self, prefix: &[u8], temporal_mark: SystemTime) {
        self.delete_log.insert(prefix, temporal_mark);
        self.has_uncommitted_changes = true;
    }

    pub fn commit(&mut self) -> VectorR<()> {
        if !self.has_uncommitted_changes {
            return Ok(());
        }

        let mut state = State::default();
        state.delete_log = self.delete_log.clone();
        state.available_data_points = self.data_points.keys().copied().collect();
        persist_state(&self.path, &state)?;

        self.has_uncommitted_changes = false;
        Ok(())
    }

    pub fn new(path: &Path, metadata: IndexMetadata) -> VectorR<Writer> {
        std::fs::create_dir(path)?;

        let lock_path = path.join(OPEN_LOCK);
        File::create(lock_path)?;

        metadata.write(path)?;

        persist_state(path, &State::default())?;

        Ok(Writer {
            metadata,
            path: path.to_path_buf(),
            data_points: HashMap::new(),
            delete_log: DTrie::new(),
            number_of_embeddings: 0,
            has_uncommitted_changes: false,
            dimension: None,
        })
    }

    pub fn open(path: &Path) -> VectorR<Writer> {
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
        let mut number_of_embeddings = 0;
        let mut data_points = HashMap::new();
        for data_point_id in state.dpid_iter() {
            let data_point_pin = DataPointPin::open_pin(path, data_point_id)?;
            let data_point_journal = data_point_pin.read_journal()?;

            if dimension.is_none() {
                let data_point = data_point_pin.open_data_point()?;
                dimension = data_point.stored_len();
            }

            data_points.insert(data_point_id, data_point_pin);
            number_of_embeddings += data_point_journal.no_nodes();
        }

        Ok(Writer {
            metadata,
            data_points,
            delete_log,
            number_of_embeddings,
            dimension,
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
    use crate::data_point_provider::METADATA;

    #[test]
    fn gc_with_some_garbage() {
        let workspace = tempfile::tempdir().unwrap();
        let index_path = workspace.path().join("vectors");
        let metadata = IndexMetadata::default();
        let writer = Writer::new(&index_path, metadata).unwrap();

        DataPointPin::create_pin(&index_path).unwrap();
        DataPointPin::create_pin(&index_path).unwrap();
        let pin = DataPointPin::create_pin(&index_path).unwrap();

        let metrics = writer.collect_garbage().unwrap();

        assert!(pin.path().is_dir());
        assert_eq!(metrics.total, 3);
        assert_eq!(metrics.garbage_deleted, 2);
        assert_eq!(metrics.garbage_not_deleted, 0);
    }

    #[test]
    fn gc_all_garbage() {
        let workspace = tempfile::tempdir().unwrap();
        let index_path = workspace.path().join("vectors");
        let metadata = IndexMetadata::default();
        let writer = Writer::new(&index_path, metadata).unwrap();

        DataPointPin::create_pin(&index_path).unwrap();
        DataPointPin::create_pin(&index_path).unwrap();
        DataPointPin::create_pin(&index_path).unwrap();

        let metrics = writer.collect_garbage().unwrap();

        assert_eq!(metrics.total, 3);
        assert_eq!(metrics.garbage_deleted, 3);
        assert_eq!(metrics.garbage_not_deleted, 0);
    }

    #[test]
    fn gc_every_data_point_pinned() {
        let workspace = tempfile::tempdir().unwrap();
        let index_path = workspace.path().join("vectors");
        let metadata = IndexMetadata::default();
        let writer = Writer::new(&index_path, metadata).unwrap();

        let pin_0 = DataPointPin::create_pin(&index_path).unwrap();
        let pin_1 = DataPointPin::create_pin(&index_path).unwrap();
        let pin_2 = DataPointPin::create_pin(&index_path).unwrap();

        let metrics = writer.collect_garbage().unwrap();

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
        let index_path = workspace.path().join("vectors");
        let metadata = IndexMetadata::default();
        let writer = Writer::new(&index_path, metadata).unwrap();

        let metrics = writer.collect_garbage().unwrap();

        assert_eq!(metrics.total, 0);
        assert_eq!(metrics.garbage_deleted, 0);
        assert_eq!(metrics.garbage_not_deleted, 0);
        assert!(index_path.join(STATE).is_file());
        assert!(index_path.join(METADATA).is_file());
        assert!(index_path.join(OPEN_LOCK).is_file());
    }
}
