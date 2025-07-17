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

use crate::metadata::{Index, IndexId, IndexKind, SegmentId, Shard};
use crate::metrics;
use crate::metrics::searcher::{ACTIVE_SHARDS, DESIRED_SHARDS, EVICTED_SHARDS};
use crate::settings::SearcherSettings;
use crate::{NidxMetadata, segment_store::download_segment};
use anyhow::anyhow;
use nidx_types::Seq;
use object_store::DynObjectStore;
use sqlx::postgres::types::PgInterval;
use sqlx::types::time::PrimitiveDateTime;
use sqlx::{Executor, Postgres};
use std::time::Instant;
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
};
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc::Receiver;
use tokio::sync::{OwnedRwLockReadGuard, RwLock, RwLockReadGuard, mpsc::Sender};
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::*;
use uuid::Uuid;

use super::shard_selector::ShardSelector;

const SHARD_EVICTION_TIME: Duration = Duration::from_secs(120);

pub enum SyncStatus {
    Syncing,
    Synced,
}

pub fn interval_to_duration(interval: PgInterval) -> Duration {
    let micros = interval.days as u64 * 24 * 3600 * 1_000_000 + interval.microseconds as u64;
    Duration::from_micros(micros)
}

#[allow(clippy::too_many_arguments)]
pub async fn run_sync(
    meta: NidxMetadata,
    storage: Arc<DynObjectStore>,
    index_metadata: Arc<SyncMetadata>,
    settings: SearcherSettings,
    shutdown: CancellationToken,
    notifier: Sender<IndexId>,
    sync_status: Option<watch::Sender<SyncStatus>>,
    mut request_sync: Option<Receiver<()>>,
    shard_selector: ShardSelector,
) -> anyhow::Result<()> {
    // Keeps track of the `updated_at` date of the most recent synced index, in order
    // to only sync indexes with changes newer than that
    let mut last_updated_at = PrimitiveDateTime::MIN.replace_year(2000)?;

    // Keeps track of indexes that failed to sync in order to retry them
    let mut failed_indexes: HashMap<IndexId, usize> = HashMap::new();

    // We only retry once every few sync rounds to avoid failing indexes to block other syncs
    let mut retry_interval = 0;

    let mut initial_sync = true;

    while !shutdown.is_cancelled() {
        let sync_result: anyhow::Result<()> = async {
            let t = Instant::now();
            let all_shards = Shard::list_ids(&meta.pool).await?;
            let selected_shards: Vec<_> = shard_selector.select_shards(all_shards);
            let (shard_ids_to_sync, indexes_to_delete) = index_metadata.set_synced_shards(&selected_shards).await;
            let indexes_to_sync = if !shard_ids_to_sync.is_empty() {
                Index::for_shards(&meta.pool, &shard_ids_to_sync).await?
            } else {
                Vec::new()
            };
            metrics::searcher::SHARD_SELECTOR_TIME.observe(t.elapsed().as_secs_f64());

            let delay = sqlx::query_scalar!(
                "SELECT NOW() - MIN(updated_at) FROM indexes WHERE shard_id = ANY($1) AND updated_at > $2 AND deleted_at IS NULL",
                &selected_shards,
                last_updated_at
            )
            .fetch_one(&meta.pool)
            .await?;

            // NULL = no indexes to update = 0 delay
            let delay = delay.map(interval_to_duration).unwrap_or(Duration::ZERO);
            metrics::searcher::SYNC_DELAY.set(delay.as_secs_f64());

            if let Some(ref sync_status) = sync_status {
                let _ = sync_status.send(SyncStatus::Syncing);
            }

            // Remove deleted indexes
            let deleted = Index::marked_to_delete(&meta.pool).await?;
            for (shard_id, index_id) in deleted.into_iter().map(|i| (i.shard_id, i.id)).chain(indexes_to_delete.into_iter()) {
                if shutdown.is_cancelled() {
                    break;
                }

                if let Err(e) = delete_index(shard_id, index_id, Arc::clone(&index_metadata), &notifier).await {
                    warn!(?index_id, "Could not delete index, some files will be left behind: {e:?}");
                }
            }

            // Update indexes
            let indexes = Index::recently_updated(&meta.pool, &selected_shards, last_updated_at).await?;
            let last_index_updated_at = indexes.last().map(|x| x.updated_at);
            let no_updates = indexes.is_empty();

            retry_interval = (retry_interval + 1) % 10;
            let retry_indexes = if retry_interval == 0 {
                let failed_ids: Vec<_> =
                    failed_indexes.keys().filter(|failed| indexes.iter().all(|i| &&i.id != failed)).collect();
                Index::get_many(&meta.pool, failed_ids.as_slice()).await?
            } else {
                vec![]
            };

            let sync_semaphore = Arc::new(Semaphore::new(settings.parallel_index_syncs));
            let mut tasks = JoinSet::new();
            for index in indexes.into_iter().chain(retry_indexes.into_iter()).chain(indexes_to_sync.into_iter()) {
                let index_id = index.id;
                let meta2 = meta.clone();
                let index_metadata2 = Arc::clone(&index_metadata);
                let notifier2 = notifier.clone();
                let storage2 = Arc::clone(&storage);
                let sync_semaphore = Arc::clone(&sync_semaphore);

                tasks.spawn(async move {
                    let Ok(_permit) = sync_semaphore.acquire().await else {
                        // Semaphore was closed, just exit
                        return (index_id, Err(anyhow!("Cancelled")));
                    };
                    info!(?index_id, "Syncing index");
                    (index_id, sync_index(&meta2, storage2, index_metadata2, index, &notifier2).await)
                });
            }

            while let Some(result) = tasks.join_next().await {
                if shutdown.is_cancelled() {
                    sync_semaphore.close();
                }
                let (index_id, result) = result.expect("Join error");
                if let Err(e) = result {
                    // If shutted down, we are just joining the tasks before exiting
                    if shutdown.is_cancelled() {
                        continue;
                    }
                    let retries = failed_indexes.entry(index_id).or_default();
                    if *retries > 2 {
                        error!(?index_id, "Index failed to update multiple times, will keep retrying forever: {e:?}")
                    } else {
                        warn!(?index_id, "Index failed to update, will retry: {e:?}")
                    }
                    *retries += 1;
                } else {
                    info!(?index_id, "Index synced");
                    failed_indexes.remove(&index_id);
                }
                metrics::searcher::SYNC_FAILED_INDEXES.set(failed_indexes.len() as i64);
            }

            if let Some(updated_at) = last_index_updated_at {
                last_updated_at = updated_at;
            }

            // Initial sync only finished when we complete a sync without errors
            if initial_sync && failed_indexes.is_empty() {
                initial_sync = false;
            }

            // We do not marked as synced until initial sync has fully finished without error
            if !initial_sync {
                if let Some(ref sync_status) = sync_status {
                    let _ = sync_status.send(SyncStatus::Synced);
                }
            }

            // If we didn't sync anything, wait for a bit
            if no_updates {
                if let Some(ref mut rx) = request_sync {
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_secs_f32(settings.metadata_refresh_interval)) => {},
                        _ = rx.recv() => {}
                    };
                } else {
                    tokio::time::sleep(Duration::from_secs_f32(settings.metadata_refresh_interval)).await;
                }
            }

            Ok(())
        }
        .await;
        if let Err(e) = sync_result {
            error!("Unexpected error while syncing: {e:?}");
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }

    Ok(())
}

async fn sync_index(
    meta: &NidxMetadata,
    storage: Arc<DynObjectStore>,
    sync_metadata: Arc<SyncMetadata>,
    index: Index,
    notifier: &Sender<IndexId>,
) -> anyhow::Result<()> {
    let operations = Operations::load_for_index(&meta.pool, &index.id).await?;
    let diff = sync_metadata.diff(&index.id, &operations).await;

    // Download new segments
    let mut tasks = JoinSet::new();
    for segment_id in diff.added_segments {
        let storage2 = Arc::clone(&storage);
        let location = sync_metadata.segment_location(&index.id, &segment_id);

        // Segment already downloaded (from a previous try)
        if let Ok(true) = tokio::fs::try_exists(&location).await {
            continue;
        }

        tasks.spawn(async move {
            // Download segment has some built-in retries (in object_store crate)
            // but failing here is expensive, so we do some extra retries
            let mut retries = 0;
            loop {
                let result = download_segment(storage2.clone(), segment_id, location.clone()).await;
                if let Err(e) = result {
                    let _ = tokio::fs::remove_dir_all(&location).await;
                    if retries > 3 {
                        return Err(e);
                    } else {
                        warn!(?segment_id, "Failure to download a segment, will retry: {e:?}");
                    }
                    retries += 1;
                } else {
                    break;
                }
            }
            Ok(())
        });
    }

    let results = tasks.join_all().await;
    if let Some(failure) = results.into_iter().find_map(Result::err) {
        return Err(failure);
    }

    // Switch meta
    let index_id = index.id;
    sync_metadata.set(index, operations).await;
    notifier.send(index_id).await?;
    let pending_refreshes = notifier.max_capacity() - notifier.capacity();
    metrics::searcher::REFRESH_QUEUE_LEN.set(pending_refreshes as i64);

    // Delete unneeded segments
    for segment_id in diff.removed_segments {
        std::fs::remove_dir_all(sync_metadata.segment_location(&index_id, &segment_id))?;
    }

    Ok(())
}

async fn delete_index(
    shard_id: Uuid,
    index_id: IndexId,
    sync_metadata: Arc<SyncMetadata>,
    notifier: &Sender<IndexId>,
) -> anyhow::Result<()> {
    if sync_metadata.delete(&shard_id, &index_id).await {
        notifier.send(index_id).await?;
        // remove directory for the index, effectively deleting all segment data
        // stored locally
        let index_location = sync_metadata.index_location(&index_id);
        match tokio::fs::remove_dir_all(index_location).await {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(e.into()),
            _ => (),
        }
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

#[derive(Clone, Debug)]
pub struct ShardIndex {
    id: IndexId,
    kind: IndexKind,
    name: String,
}

impl ShardIndex {
    fn new(from: &Index) -> Self {
        Self {
            id: from.id,
            kind: from.kind,
            name: from.name.clone(),
        }
    }
}

#[derive(Clone, Default)]
pub struct ShardIndexes(Vec<ShardIndex>);

impl ShardIndexes {
    fn push(&mut self, index: ShardIndex) {
        if !self.0.iter().any(|i| index.id == i.id) {
            self.0.push(index);
        }
    }

    fn remove(&mut self, index_id: &IndexId) {
        if let Some(pos) = self.0.iter().position(|i| i.id == *index_id) {
            self.0.swap_remove(pos);
        }
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn single_index_by_kind(&self, kind: IndexKind) -> Option<IndexId> {
        let indexes: Vec<_> = self.0.iter().filter(|i| i.kind == kind).collect();
        if indexes.len() > 2 {
            error!(
                ?indexes,
                "Unexpected multiple indexes of the same kind for the same shard"
            );
            None
        } else {
            indexes.first().map(|i| i.id)
        }
    }

    pub fn paragraph_index(&self) -> Option<IndexId> {
        self.single_index_by_kind(IndexKind::Paragraph)
    }

    pub fn text_index(&self) -> Option<IndexId> {
        self.single_index_by_kind(IndexKind::Text)
    }

    pub fn relation_index(&self) -> Option<IndexId> {
        self.single_index_by_kind(IndexKind::Relation)
    }

    pub fn vector_index(&self, name: &str) -> Option<IndexId> {
        self.0
            .iter()
            .filter(|i| i.kind == IndexKind::Vector && i.name == name)
            .map(|i| i.id)
            .next()
    }
}

pub struct SyncMetadata {
    work_dir: PathBuf,
    synced_metadata: Arc<RwLock<HashMap<IndexId, RwLock<IndexMetadata>>>>,
    shard_metadata: RwLock<HashMap<Uuid, ShardIndexes>>,
    evicted_shards: RwLock<HashMap<Uuid, (Instant, ShardIndexes)>>,
}

impl SyncMetadata {
    pub fn new(work_dir: PathBuf) -> Self {
        SyncMetadata {
            work_dir,
            synced_metadata: Arc::new(RwLock::new(HashMap::new())),
            shard_metadata: RwLock::new(HashMap::new()),
            evicted_shards: RwLock::new(HashMap::new()),
        }
    }

    pub fn index_location(&self, index_id: &IndexId) -> PathBuf {
        self.work_dir.join(index_id.local_path())
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
            let shard_id = index.shard_id;
            let shard_index = ShardIndex::new(&index);
            self.synced_metadata
                .write()
                .await
                .insert(index.id, RwLock::new(IndexMetadata { index, operations }));
            self.shard_metadata
                .write()
                .await
                .entry(shard_id)
                .or_default()
                .push(shard_index);
        }
    }

    pub async fn get(&self, index_id: &IndexId) -> GuardedIndexMetadata {
        GuardedIndexMetadata::new(self.synced_metadata.clone().read_owned().await, *index_id)
    }

    pub async fn delete(&self, shard_id: &Uuid, index_id: &IndexId) -> bool {
        let removed = self.synced_metadata.write().await.remove(index_id).is_some();

        let mut write_shard_metadata = self.shard_metadata.write().await;
        let shard_entry = write_shard_metadata.get_mut(shard_id);
        if let Some(shard) = shard_entry {
            shard.remove(index_id);
            if shard.is_empty() {
                write_shard_metadata.remove(shard_id);
            }
        }

        removed
    }

    pub async fn get_shard_indexes(&self, shard_id: &Uuid) -> Option<ShardIndexes> {
        self.shard_metadata.read().await.get(shard_id).cloned()
    }

    pub async fn set_synced_shards(&self, shards: &[Uuid]) -> (Vec<Uuid>, Vec<(Uuid, IndexId)>) {
        let shards: HashSet<Uuid> = HashSet::from_iter(shards.iter().copied());

        let mut shard_metadata = self.shard_metadata.write().await;
        let mut evicted_shards = self.evicted_shards.write().await;
        let synced_shards = shard_metadata.keys().copied().collect();

        // New shards to sync
        let mut shards_to_sync = Vec::new();
        let mut count_new_shards = 0;
        let mut count_recovered_shards = 0;
        for new_shard in shards.difference(&synced_shards) {
            if let Some((_, evicted_indexes)) = evicted_shards.remove(new_shard) {
                // If the shard was being evicted, recover it but still force sync its indexes
                shard_metadata.insert(*new_shard, evicted_indexes);
                count_recovered_shards += 1;
            } else {
                // If the shard is new add it to the list to be forcibly synced (it may not have been recently updated)
                shard_metadata.insert(*new_shard, ShardIndexes(Vec::new()));
                count_new_shards += 1;
            }
            shards_to_sync.push(*new_shard);
        }
        if count_new_shards > 0 || count_recovered_shards > 0 {
            info!(
                new = count_new_shards,
                recovered = count_recovered_shards,
                "New shards added to sync"
            );
        }

        // Completely purge evicted and expired shards
        let mut indexes_to_delete = Vec::new();
        let mut shards_to_delete = Vec::new();
        let mut count_removed_shards = 0;
        for (shard_id, (evicted_at, indexes)) in evicted_shards.iter() {
            if evicted_at.elapsed() > SHARD_EVICTION_TIME {
                shards_to_delete.push(*shard_id);
                for index in &indexes.0 {
                    indexes_to_delete.push((*shard_id, index.id));
                }
                count_removed_shards += 1;
            }
        }
        for shard in shards_to_delete {
            evicted_shards.remove(&shard);
        }
        if count_removed_shards > 0 {
            info!(count = count_removed_shards, "Shards removed after eviction");
        }

        // Shards that have just been removed, move to the eviction zone
        let mut count_evicted_shards = 0;
        for shard_to_evict in synced_shards.difference(&shards) {
            if let Some(indexes) = shard_metadata.remove(shard_to_evict) {
                evicted_shards.insert(*shard_to_evict, (Instant::now(), indexes));
            }
            count_evicted_shards += 1;
        }
        if count_evicted_shards > 0 {
            info!(count = count_evicted_shards, "Shards marked for eviction");
        }

        DESIRED_SHARDS.set(shards.len() as i64);
        ACTIVE_SHARDS.set(shard_metadata.values().filter(|v| !v.is_empty()).count() as i64);
        EVICTED_SHARDS.set(evicted_shards.len() as i64);

        (shards_to_sync, indexes_to_delete)
    }
}

pub struct GuardedIndexMetadata {
    guard: OwnedRwLockReadGuard<HashMap<IndexId, RwLock<IndexMetadata>>>,
    index_id: IndexId,
}

impl GuardedIndexMetadata {
    fn new(guard: OwnedRwLockReadGuard<HashMap<IndexId, RwLock<IndexMetadata>>>, index_id: IndexId) -> Self {
        Self { guard, index_id }
    }

    pub async fn get(&self) -> Option<RwLockReadGuard<IndexMetadata>> {
        let m = self.guard.get(&self.index_id)?;
        Some(m.read().await)
    }
}

#[cfg(test)]
mod tests {
    use std::{io::BufWriter, path::Path, sync::Arc};

    use nidx_vector::config::{VectorCardinality, VectorConfig};
    use object_store::{ObjectStore, PutPayload};
    use tempfile::tempdir;

    use crate::{
        NidxMetadata,
        metadata::{Deletion, Index, Segment, SegmentId, Shard},
        scheduler::purge_deletions,
        searcher::sync::{Operations, SyncMetadata, sync_index},
    };

    const VECTOR_CONFIG: VectorConfig = VectorConfig {
        similarity: nidx_vector::config::Similarity::Cosine,
        normalize_vectors: false,
        vector_type: nidx_vector::config::VectorType::DenseF32 { dimension: 3 },
        flags: vec![],
        vector_cardinality: VectorCardinality::Single,
    };

    #[sqlx::test]
    async fn test_load_index_metadata(pool: sqlx::PgPool) -> anyhow::Result<()> {
        let index = Index::create(
            &pool,
            Shard::create(&pool, uuid::Uuid::new_v4()).await?.id,
            "english",
            VECTOR_CONFIG.into(),
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
            "english",
            VECTOR_CONFIG.into(),
        )
        .await?;
        // Assumes we get index_id=1
        let index_path = work_dir.path().join("1");

        // Initial sync with empty data
        sync_index(
            &meta,
            storage.clone(),
            sync_metadata.clone(),
            Index::get(&meta.pool, index.id).await?,
            &tx,
        )
        .await?;
        // We get a notification even for an empty index (so we don't return errors for empty indexes)
        assert!(rx.try_recv().is_ok());
        // No data yet
        assert!(downloaded_segments(&index_path).is_err());

        // Adds a first segment
        let s1 = Segment::create(&meta.pool, index.id, 1i64.into(), 4, serde_json::Value::Null).await?;
        storage
            .put(&s1.id.storage_key(), PutPayload::from_iter(dummy_data.iter().cloned()))
            .await?;
        s1.mark_ready(&meta.pool, 122).await?;
        Deletion::create(&meta.pool, index.id, 1i64.into(), &["k1".to_string()]).await?;

        sync_index(
            &meta,
            storage.clone(),
            sync_metadata.clone(),
            Index::get(&meta.pool, index.id).await?,
            &tx,
        )
        .await?;
        assert_eq!(rx.try_recv()?, index.id);
        assert_eq!(downloaded_segments(&index_path)?, vec![s1.id]);
        {
            let index_meta_guard = sync_metadata.get(&index.id).await;
            let index_meta = index_meta_guard.get().await.unwrap();
            assert_eq!(index_meta.operations.0[0].deleted_keys, &["k1".to_string()]);
        }

        // Adds another segment
        let s2 = Segment::create(&meta.pool, index.id, 2i64.into(), 4, serde_json::Value::Null).await?;
        storage
            .put(&s2.id.storage_key(), PutPayload::from_iter(dummy_data.iter().cloned()))
            .await?;
        s2.mark_ready(&meta.pool, 122).await?;
        Deletion::create(&meta.pool, index.id, 2i64.into(), &["k2".to_string()]).await?;

        sync_index(
            &meta,
            storage.clone(),
            sync_metadata.clone(),
            Index::get(&meta.pool, index.id).await?,
            &tx,
        )
        .await?;
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
        storage
            .put(&s3.id.storage_key(), PutPayload::from_iter(dummy_data.iter().cloned()))
            .await?;
        s3.mark_ready(&meta.pool, 122).await?;

        sync_index(
            &meta,
            storage.clone(),
            sync_metadata.clone(),
            Index::get(&meta.pool, index.id).await?,
            &tx,
        )
        .await?;
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

        sync_index(
            &meta,
            storage.clone(),
            sync_metadata.clone(),
            Index::get(&meta.pool, index.id).await?,
            &tx,
        )
        .await?;
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
