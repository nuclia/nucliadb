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

use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::Arc,
};

use nidx_types::Seq;
use tokio::sync::{mpsc::Sender, OwnedRwLockReadGuard, RwLock, RwLockReadGuard};

use crate::metadata::{Index, IndexId, SegmentId};

pub struct SegmentDiff {
    pub added_segments: HashSet<SegmentId>,
    pub removed_segments: HashSet<SegmentId>,
}

pub struct SeqMetadata {
    pub seq: Seq,
    pub segment_ids: Vec<SegmentId>,
    pub deleted_keys: Vec<String>,
}

pub struct Operations(pub Vec<SeqMetadata>);

pub struct IndexMetadata {
    pub index: Index,
    pub operations: Operations,
}

impl Operations {
    pub fn segments(&self) -> impl Iterator<Item = SegmentId> + '_ {
        self.0.iter().flat_map(|o| o.segment_ids.iter().cloned())
    }
}

pub struct SearchMetadata {
    work_dir: PathBuf,
    synced_metadata: Arc<RwLock<HashMap<IndexId, RwLock<IndexMetadata>>>>,
    changes: Sender<IndexId>,
}

impl SearchMetadata {
    pub fn new(work_dir: PathBuf, changes: Sender<IndexId>) -> Self {
        SearchMetadata {
            work_dir,
            synced_metadata: Arc::new(RwLock::new(HashMap::new())),
            changes,
        }
    }

    pub fn segment_location(&self, index_id: &IndexId, segment_id: &SegmentId) -> PathBuf {
        self.work_dir.join(segment_id.local_path(index_id))
    }

    pub async fn diff(&self, index_id: &IndexId, new: &Operations) -> SegmentDiff {
        let current_segments = match self.synced_metadata.read().await.get(index_id) {
            Some(meta) => meta.read().await.operations.segments().collect(),
            None => HashSet::new(),
        };
        let new_segments: HashSet<_> = new.segments().collect();

        SegmentDiff {
            added_segments: new_segments.difference(&current_segments).cloned().collect(),
            removed_segments: current_segments.difference(&new_segments).cloned().collect(),
        }
    }

    pub async fn set(&self, index: Index, operations: Operations) {
        let index_id = index.id;
        let read_meta = self.synced_metadata.read().await;
        let existing_meta = read_meta.get(&index.id);
        if let Some(existing_meta) = existing_meta {
            existing_meta.write().await.operations = operations;
        } else {
            drop(read_meta);
            self.synced_metadata.write().await.insert(
                index.id,
                RwLock::new(IndexMetadata {
                    index,
                    operations,
                }),
            );
        }
        self.changes.send(index_id).await.unwrap();
    }

    pub async fn get<'a>(&self, index_id: &IndexId) -> GuardedIndexMetadata {
        GuardedIndexMetadata::new(self.synced_metadata.clone().read_owned().await, *index_id)
    }
}

pub struct GuardedIndexMetadata {
    guard: OwnedRwLockReadGuard<HashMap<IndexId, RwLock<IndexMetadata>>>,
    index_id: IndexId,
}

impl GuardedIndexMetadata {
    fn new(guard: OwnedRwLockReadGuard<HashMap<IndexId, RwLock<IndexMetadata>>>, index_id: IndexId) -> Self {
        Self {
            guard,
            index_id,
        }
    }

    pub async fn get(&self) -> Option<RwLockReadGuard<IndexMetadata>> {
        let Some(m) = self.guard.get(&self.index_id) else {
            return None;
        };
        Some(m.read().await)
    }
}
