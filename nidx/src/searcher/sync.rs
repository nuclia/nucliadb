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

use crate::metadata::{Index, IndexId, SegmentId};
use crate::{segment_store::download_segment, NidxMetadata};
use nidx_types::Seq;
use object_store::DynObjectStore;
use sqlx::types::time::PrimitiveDateTime;
use sqlx::{Executor, Postgres};
use std::{cmp::max, sync::Arc, time::Duration};
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
};
use tokio::sync::{mpsc::Sender, OwnedRwLockReadGuard, RwLock, RwLockReadGuard};

pub async fn run_sync(
    meta: NidxMetadata,
    storage: Arc<DynObjectStore>,
    index_metadata: Arc<SyncMetadata>,
    notifier: Sender<IndexId>,
) -> anyhow::Result<()> {
    let mut last_updated_at = PrimitiveDateTime::MIN.replace_year(2000)?;
    loop {
        let indexes = Index::recently_updated(&meta.pool, last_updated_at).await?;
        for index in indexes {
            // TODO: Handle errors
            last_updated_at = max(last_updated_at, index.updated_at);
            sync_index(&meta, storage.clone(), index_metadata.clone(), index, &notifier).await?;
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn sync_index(
    meta: &NidxMetadata,
    storage: Arc<DynObjectStore>,
    sync_metadata: Arc<SyncMetadata>,
    index: Index,
    notifier: &Sender<IndexId>,
) -> anyhow::Result<()> {
    let operations = Operations::load_for_index(&meta.pool, &index.id).await?;
    // Empty index, just created, no need to sync
    if operations.0.is_empty() {
        return Ok(());
    }

    let diff = sync_metadata.diff(&index.id, &operations).await;

    // Download new segments
    for segment_id in diff.added_segments {
        download_segment(storage.clone(), segment_id, sync_metadata.segment_location(&index.id, &segment_id)).await?;
    }

    // Switch meta
    let index_id = index.id;
    sync_metadata.set(index, operations).await;
    notifier.send(index_id).await?;

    // Delete unneeded segments
    for segment_id in diff.removed_segments {
        std::fs::remove_dir_all(sync_metadata.segment_location(&index_id, &segment_id))?;
    }

    Ok(())
}

pub struct SegmentDiff {
    pub added_segments: HashSet<SegmentId>,
    pub removed_segments: HashSet<SegmentId>,
}

#[derive(Clone)]
pub struct SeqMetadata {
    pub seq: Seq,
    pub segment_ids: Vec<SegmentId>,
    pub deleted_keys: Vec<String>,
}

#[derive(Clone)]
pub struct Operations(pub Vec<SeqMetadata>);

impl Operations {
    async fn load_for_index(meta: impl Executor<'_, Database = Postgres>, index_id: &IndexId) -> anyhow::Result<Self> {
        let loaded = sqlx::query_as!(
            SeqMetadata,
            r#"WITH ready_segments AS (
                SELECT index_id, seq, array_agg(id) AS segment_ids
                    FROM segments
                    WHERE delete_at IS NULL
                    GROUP BY index_id, seq
                )
                SELECT
                COALESCE(ready_segments.seq, deletions.seq) AS "seq!",
                COALESCE(segment_ids, '{}') AS "segment_ids!: Vec<SegmentId>",
                COALESCE(deletions.keys, '{}') AS "deleted_keys!"
                FROM ready_segments
                NATURAL FULL OUTER JOIN deletions
                WHERE index_id = $1
                ORDER BY seq;"#,
            index_id as &IndexId
        )
        .fetch_all(meta)
        .await?;

        Ok(Operations(loaded))
    }

    pub fn segments(&self) -> impl Iterator<Item = SegmentId> + '_ {
        self.0.iter().flat_map(|o| o.segment_ids.iter().cloned())
    }
}

pub struct IndexMetadata {
    pub index: Index,
    pub operations: Operations,
}

pub struct SyncMetadata {
    work_dir: PathBuf,
    synced_metadata: Arc<RwLock<HashMap<IndexId, RwLock<IndexMetadata>>>>,
}

impl SyncMetadata {
    pub fn new(work_dir: PathBuf) -> Self {
        SyncMetadata {
            work_dir,
            synced_metadata: Arc::new(RwLock::new(HashMap::new())),
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

#[cfg(test)]
mod tests {
    use std::{io::BufWriter, path::Path, sync::Arc};

    use object_store::{ObjectStore, PutPayload};
    use tempfile::tempdir;

    use crate::{
        maintenance::scheduler::purge_deletions,
        metadata::{Deletion, Index, Segment, SegmentId, Shard},
        searcher::sync::{sync_index, Operations, SyncMetadata},
        NidxMetadata,
    };

    #[sqlx::test]
    async fn test_load_index_metadata(pool: sqlx::PgPool) -> anyhow::Result<()> {
        let index = Index::create(
            &pool,
            Shard::create(&pool, uuid::Uuid::new_v4()).await?.id,
            crate::metadata::IndexKind::Vector,
            "english",
        )
        .await?;

        // Seq 1: A segment was created and unmerged
        let s1 = Segment::create(&pool, index.id, 1i64.into(), 4, serde_json::Value::Null).await?;
        s1.mark_ready(&pool, 122).await?;
        Deletion::create(&pool, index.id, 1i64.into(), &["k1".to_string()]).await?;

        // Seq 2: A segment was created and later merged (deletions remain)
        Deletion::create(&pool, index.id, 2i64.into(), &["k2a".to_string(), "k2b".to_string()]).await?;

        // Seq 3: A segment was created and also the result of a merge
        let s2 = Segment::create(&pool, index.id, 3i64.into(), 4, serde_json::Value::Null).await?;
        s2.mark_ready(&pool, 122).await?;
        Deletion::create(&pool, index.id, 3i64.into(), &["k3".to_string()]).await?;
        let s3 = Segment::create(&pool, index.id, 3i64.into(), 40, serde_json::Value::Null).await?;
        s3.mark_ready(&pool, 1220).await?;

        // Seq 4: A segment was created without deletions
        let s4 = Segment::create(&pool, index.id, 4i64.into(), 4, serde_json::Value::Null).await?;
        s4.mark_ready(&pool, 122).await?;

        let op = Operations::load_for_index(&pool, &index.id).await?;
        let sm = &op.0[0];
        assert_eq!(sm.seq, 1i64.into());
        assert_eq!(sm.segment_ids, vec![s1.id]);
        assert_eq!(sm.deleted_keys, vec!["k1"]);

        let sm = &op.0[1];
        assert_eq!(sm.seq, 2i64.into());
        assert_eq!(sm.segment_ids, vec![]);
        assert_eq!(sm.deleted_keys, vec!["k2a", "k2b"]);

        let sm = &op.0[2];
        assert_eq!(sm.seq, 3i64.into());
        assert_eq!(sm.segment_ids, vec![s2.id, s3.id]);
        assert_eq!(sm.deleted_keys, vec!["k3"]);

        let sm = &op.0[3];
        assert_eq!(sm.seq, 4i64.into());
        assert_eq!(sm.segment_ids, vec![s4.id]);
        assert!(sm.deleted_keys.is_empty());

        Ok(())
    }

    fn downloaded_segments(work_dir: &Path) -> anyhow::Result<Vec<SegmentId>> {
        let mut segment_ids: Vec<_> = std::fs::read_dir(work_dir)?
            .map(|entry| entry.unwrap().file_name().to_str().unwrap().parse::<i64>().unwrap())
            .collect();

        segment_ids.sort();
        Ok(segment_ids.into_iter().map(SegmentId::from).collect())
    }

    #[sqlx::test]
    async fn test_sync_flow(pool: sqlx::PgPool) -> anyhow::Result<()> {
        let mut dummy_data = Vec::new();
        tar::Builder::new(BufWriter::new(&mut dummy_data)).finish()?;

        let meta = NidxMetadata::new_with_pool(pool).await?;
        let work_dir = tempdir()?;
        let (tx, mut rx) = tokio::sync::mpsc::channel(100);
        let storage = Arc::new(object_store::memory::InMemory::new());
        let sync_metadata = Arc::new(SyncMetadata::new(work_dir.path().to_path_buf()));
        let index = Index::create(
            &meta.pool,
            Shard::create(&meta.pool, uuid::Uuid::new_v4()).await?.id,
            crate::metadata::IndexKind::Vector,
            "english",
        )
        .await?;
        // Assumes we get index_id=1
        let index_path = work_dir.path().join("1");

        // Initial sync with empty data
        sync_index(&meta, storage.clone(), sync_metadata.clone(), Index::get(&meta.pool, index.id).await?, &tx).await?;
        // No notification sent for empty index
        assert!(rx.try_recv().is_err());
        // No data yet
        assert!(downloaded_segments(&index_path).is_err());

        // Adds a first segment
        let s1 = Segment::create(&meta.pool, index.id, 1i64.into(), 4, serde_json::Value::Null).await?;
        storage.put(&s1.id.storage_key(), PutPayload::from_iter(dummy_data.iter().cloned())).await?;
        s1.mark_ready(&meta.pool, 122).await?;
        Deletion::create(&meta.pool, index.id, 1i64.into(), &["k1".to_string()]).await?;

        sync_index(&meta, storage.clone(), sync_metadata.clone(), Index::get(&meta.pool, index.id).await?, &tx).await?;
        assert_eq!(rx.try_recv()?, index.id);
        assert_eq!(downloaded_segments(&index_path)?, vec![s1.id]);
        {
            let index_meta_guard = sync_metadata.get(&index.id).await;
            let index_meta = index_meta_guard.get().await.unwrap();
            assert_eq!(index_meta.operations.0[0].deleted_keys, &["k1".to_string()]);
        }

        // Adds another segment
        let s2 = Segment::create(&meta.pool, index.id, 2i64.into(), 4, serde_json::Value::Null).await?;
        storage.put(&s2.id.storage_key(), PutPayload::from_iter(dummy_data.iter().cloned())).await?;
        s2.mark_ready(&meta.pool, 122).await?;
        Deletion::create(&meta.pool, index.id, 2i64.into(), &["k2".to_string()]).await?;

        sync_index(&meta, storage.clone(), sync_metadata.clone(), Index::get(&meta.pool, index.id).await?, &tx).await?;
        assert_eq!(rx.try_recv()?, index.id);
        assert_eq!(downloaded_segments(&index_path)?, vec![s1.id, s2.id]);
        {
            let index_meta_guard = sync_metadata.get(&index.id).await;
            let index_meta = index_meta_guard.get().await.unwrap();
            assert_eq!(index_meta.operations.0[0].deleted_keys, &["k1".to_string()]);
            assert_eq!(index_meta.operations.0[1].deleted_keys, &["k2".to_string()]);
        }

        // Merge (new segment, deletes old one)
        Segment::delete_many(&meta.pool, &[s1.id, s2.id]).await?;
        storage.delete(&s1.id.storage_key()).await?;
        storage.delete(&s2.id.storage_key()).await?;
        let s3 = Segment::create(&meta.pool, index.id, 3i64.into(), 4, serde_json::Value::Null).await?;
        storage.put(&s3.id.storage_key(), PutPayload::from_iter(dummy_data.iter().cloned())).await?;
        s3.mark_ready(&meta.pool, 122).await?;

        sync_index(&meta, storage.clone(), sync_metadata.clone(), Index::get(&meta.pool, index.id).await?, &tx).await?;
        assert_eq!(rx.try_recv()?, index.id);
        assert_eq!(downloaded_segments(&index_path)?, vec![s3.id]);
        {
            let index_meta_guard = sync_metadata.get(&index.id).await;
            let index_meta = index_meta_guard.get().await.unwrap();
            assert_eq!(index_meta.operations.0.len(), 3);
            assert_eq!(index_meta.operations.0[0].deleted_keys, &["k1".to_string()]);
            assert_eq!(index_meta.operations.0[1].deleted_keys, &["k2".to_string()]);
            assert!(index_meta.operations.0[2].deleted_keys.is_empty());
        }

        // Purge old deletions
        purge_deletions(&meta, 10).await?;

        sync_index(&meta, storage.clone(), sync_metadata.clone(), Index::get(&meta.pool, index.id).await?, &tx).await?;
        assert_eq!(rx.try_recv()?, index.id);
        assert_eq!(downloaded_segments(&index_path)?, vec![s3.id]);
        {
            let index_meta_guard = sync_metadata.get(&index.id).await;
            let index_meta = index_meta_guard.get().await.unwrap();
            assert_eq!(index_meta.operations.0.len(), 1);
            assert!(index_meta.operations.0[0].deleted_keys.is_empty());
        }

        Ok(())
    }
}
