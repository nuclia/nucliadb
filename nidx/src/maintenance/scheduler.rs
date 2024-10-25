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

use std::{sync::Arc, time::Duration};

use async_nats::jetstream::consumer::PullConsumer;
use futures::StreamExt;
use nidx_types::Seq;
use object_store::DynObjectStore;
use tokio::{task::JoinSet, time::sleep};
use tracing::*;

use crate::{
    metadata::{MergeJob, Segment, SegmentId},
    NidxMetadata, Settings,
};

pub async fn run() -> anyhow::Result<()> {
    let settings = Settings::from_env();
    let indexer_settings = settings.indexer.unwrap();
    let meta = NidxMetadata::new(&settings.metadata.database_url).await?;

    let client = async_nats::connect(indexer_settings.nats_server).await?;
    let jetstream = async_nats::jetstream::new(client);
    let mut consumer: PullConsumer = jetstream.get_consumer_from_stream("nidx", "nidx").await?;

    let mut tasks = JoinSet::new();

    let meta2 = meta.clone();
    tasks.spawn(async move {
        loop {
            if let Err(e) = retry_jobs(&meta2).await {
                warn!(?e, "Error in retry_jobs task");
            }
            sleep(Duration::from_secs(30)).await;
        }
    });

    let meta2 = meta.clone();
    let storage = indexer_settings.object_store.client();
    tasks.spawn(async move {
        loop {
            if let Err(e) = purge_segments(&meta2, &storage).await {
                warn!(?e, "Error in purge_segments task");
            }
            sleep(Duration::from_secs(60)).await;
        }
    });

    let meta2 = meta.clone();
    let mut consumer2 = consumer.clone();
    tasks.spawn(async move {
        loop {
            match consumer2.info().await {
                Ok(consumer_info) => {
                    let oldest_confirmed_seq = consumer_info.ack_floor.stream_sequence;
                    let oldest_pending_seq = oldest_confirmed_seq + 1;
                    if let Err(e) = purge_deletions(&meta2, oldest_pending_seq).await {
                        warn!(?e, "Error in purge_deletions task");
                    }
                }
                Err(e) => {
                    warn!(?e, "Error while getting consumer information");
                }
            }
            sleep(Duration::from_secs(15)).await;
        }
    });

    tasks.spawn(async move {
        loop {
            match consumer.info().await {
                Ok(consumer_info) => {
                    let oldest_confirmed_seq = consumer_info.ack_floor.stream_sequence;
                    if let Err(e) = schedule_merges(&meta, Seq::from(oldest_confirmed_seq)).await {
                        warn!(?e, "Error in schedule_merges task");
                    }
                }
                Err(e) => {
                    warn!(?e, "Error while getting consumer information");
                }
            }
            sleep(Duration::from_secs(15)).await;
        }
    });

    tasks.join_next().await;
    println!("A task finished, exiting");

    Ok(())
}

/// Re-enqueues jobs that have been stuck for a while without ack'ing
pub async fn retry_jobs(meta: &NidxMetadata) -> anyhow::Result<()> {
    // Requeue failed jobs (with retries left)
    let retry_jobs = sqlx::query_as!(MergeJob, "UPDATE merge_jobs SET started_at = NULL, running_at = NULL, retries = retries + 1 WHERE running_at < NOW() - INTERVAL '1 minute' AND retries < 4 RETURNING *").fetch_all(&meta.pool).await?;
    for j in retry_jobs {
        debug!(j.id, j.retries, "Retrying job");
    }

    // Delete failed jobs (no retries left)
    let failed_jobs = sqlx::query_as!(
        MergeJob,
        "DELETE FROM merge_jobs WHERE running_at < NOW() - INTERVAL '1 minute' AND retries >= 4 RETURNING *"
    )
    .fetch_all(&meta.pool)
    .await?;
    for j in failed_jobs {
        error!(j.id, "Failed job");
    }

    Ok(())
}

/// Purge segments that have not been ready for a while:
/// - Uploads that failed
/// - Recent deletions
pub async fn purge_segments(meta: &NidxMetadata, storage: &Arc<DynObjectStore>) -> anyhow::Result<()> {
    let deleted_segments = sqlx::query_scalar!(r#"SELECT id AS "id: SegmentId" FROM segments WHERE delete_at < NOW()"#)
        .fetch_all(&meta.pool)
        .await?;
    let paths = deleted_segments.iter().map(|sid| Ok(sid.storage_key()));
    let results = storage.delete_stream(futures::stream::iter(paths).boxed()).collect::<Vec<_>>().await;

    let mut deleted = Vec::new();
    for (segment_id, result) in deleted_segments.into_iter().zip(results.iter()) {
        match result {
            Ok(_)
            | Err(object_store::Error::NotFound {
                ..
            }) => deleted.push(segment_id),
            Err(e) => warn!(?e, "Error deleting segment from storage"),
        }
    }
    Segment::delete_many(&meta.pool, &deleted).await?;

    Ok(())
}

pub async fn purge_deletions(meta: &NidxMetadata, oldest_pending_seq: u64) -> anyhow::Result<()> {
    // Purge deletions that don't apply to any segment and won't apply to any segment pending to process
    sqlx::query!(
        "WITH oldest_segments AS (
            SELECT index_id, MIN(seq) AS seq FROM segments
            GROUP BY index_id
        )
        DELETE FROM deletions USING oldest_segments
        WHERE deletions.index_id = oldest_segments.index_id
        AND deletions.seq <= oldest_segments.seq
        AND deletions.seq <= $1",
        oldest_pending_seq as i64
    )
    .execute(&meta.pool)
    .await?;

    Ok(())
}

/// Enqueue merge jobs for segments older than `last_indexed_seq` that aren't
/// already scheduled for merge or marked to delete.
///
/// Merging involves creation of a single segment from multiple ones, combining
/// their data and applying deletions. Merge jobs are executed in parallel (in
/// multiple workers) and while other segments are being indexed. This restricts
/// us to only merge segments whose sequences are less than the smaller sequence
/// being indexed.
///
/// As an example, if sequences 100 and 102 are indexed but 101 is still being
/// indexed, we can only merge segments with sequence <= 100. Otherwise, if we
/// merge 100 and 102 (generating a new 102 segment) and segment 101 included
/// deletions for 100, we'll never apply them and we'll end in an inconsistent
/// state.
///
/// Right now, we schedule merges with everything we can
async fn schedule_merges(meta: &NidxMetadata, last_indexed_seq: Seq) -> anyhow::Result<()> {
    // TODO: better merge algorithm
    let merges = sqlx::query!(
        r#"SELECT index_id, array_agg(id) AS "segment_ids!: Vec<SegmentId>" FROM segments WHERE delete_at IS NULL AND merge_job_id IS NULL AND seq <= $1 GROUP BY index_id"#,
        i64::from(last_indexed_seq),
    )
    .fetch_all(&meta.pool)
    .await?;
    for m in merges {
        if m.segment_ids.len() > 3 {
            MergeJob::create(meta, m.index_id.into(), &m.segment_ids, last_indexed_seq).await?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    mod merge_scheduling {
        use std::collections::{HashMap, HashSet};

        use nidx_types::Seq;
        use uuid::Uuid;

        use super::*;

        use crate::metadata::{Index, IndexId, IndexKind, NidxMetadata, Shard};

        #[sqlx::test]
        async fn test_schedule_merges_for_shard_with_single_index(pool: sqlx::PgPool) -> anyhow::Result<()> {
            let meta = NidxMetadata::new_with_pool(pool).await?;
            let kbid = Uuid::new_v4();
            let shard = Shard::create(&meta.pool, kbid).await?;
            let index = Index::create(&meta.pool, shard.id, IndexKind::Vector, "multilingual").await?;
            let mut seq: i64 = 0;

            for _ in 0..10 {
                let segment = Segment::create(&meta.pool, index.id, Seq::from(seq)).await?;
                segment.mark_ready(&meta.pool, 50, 1000).await?;
                seq += 1;
            }

            // creation of shards/indexes/segments don't trigger any merge job
            assert!(MergeJob::take(&meta.pool).await?.is_none());

            schedule_merges(&meta, Seq::from(seq)).await?;

            // one job has been scheduled for the index
            let jobs = get_all_merge_jobs(&meta).await?;
            assert_eq!(jobs.len(), 1);
            assert_eq!(jobs[0].index_id, index.id);
            assert_eq!(jobs[0].seq, Seq::from(seq));

            for segment in index.segments(&meta.pool).await? {
                assert!(segment.merge_job_id.is_some());
                assert_eq!(segment.merge_job_id.unwrap(), jobs[0].id);
            }

            Ok(())
        }

        #[sqlx::test]
        async fn test_schedule_merges_for_shard_with_multiple_indexes(pool: sqlx::PgPool) -> anyhow::Result<()> {
            let meta = NidxMetadata::new_with_pool(pool).await?;
            let kbid = Uuid::new_v4();
            let shard = Shard::create(&meta.pool, kbid).await?;

            let indexes = vec![
                Index::create(&meta.pool, shard.id, IndexKind::Vector, "multilingual").await?,
                Index::create(&meta.pool, shard.id, IndexKind::Vector, "english").await?,
                Index::create(&meta.pool, shard.id, IndexKind::Text, "fulltext").await?,
                Index::create(&meta.pool, shard.id, IndexKind::Paragraph, "keyword").await?,
                Index::create(&meta.pool, shard.id, IndexKind::Relation, "relation").await?,
            ];
            let mut seq: i64 = 0;

            for _ in 0..10 {
                for index in &indexes {
                    let segment = Segment::create(&meta.pool, index.id, Seq::from(seq)).await?;
                    segment.mark_ready(&meta.pool, 50, 1000).await?;
                    seq += 1;
                }
            }

            schedule_merges(&meta, Seq::from(seq)).await?;

            // scheduled a job per index
            let jobs = get_all_merge_jobs(&meta).await?;
            assert_eq!(jobs.len(), indexes.len());
            assert_eq!(
                indexes.iter().map(|i| i.id).collect::<HashSet<_>>(),
                jobs.iter().map(|j| j.index_id).collect::<HashSet<_>>(),
            );

            for job in &jobs {
                assert_eq!(job.seq, Seq::from(seq));
            }

            // validate segments are marked with merge job id
            let jobs_by_index: HashMap<IndexId, _> = jobs.iter().map(|job| (job.index_id, job)).collect();
            for index in &indexes {
                let (_, merge_job) = jobs_by_index.get_key_value(&index.id).unwrap();

                let segments = index.segments(&meta.pool).await?;
                assert_eq!(segments.len(), 10);

                for segment in &segments {
                    assert!(segment.merge_job_id.is_some());
                    assert_eq!(segment.merge_job_id.unwrap(), merge_job.id);
                }
            }

            Ok(())
        }

        async fn ongoing_indexing_scenario(pool: sqlx::PgPool) -> anyhow::Result<NidxMetadata> {
            let meta = NidxMetadata::new_with_pool(pool).await?;
            let kbid = Uuid::new_v4();
            let shard = Shard::create(&meta.pool, kbid).await?;
            let index = Index::create(&meta.pool, shard.id, IndexKind::Vector, "multilingual").await?;

            for seq in [95, 98, 99, 100, 102i64] {
                let segment = Segment::create(&meta.pool, index.id, Seq::from(seq)).await?;
                segment.mark_ready(&meta.pool, 50, 1000).await?;
            }

            // 101 is still indexing
            Segment::create(&meta.pool, index.id, Seq::from(101i64)).await?;

            Ok(meta)
        }

        #[sqlx::test]
        async fn scheduling_with_smaller_than_existing_sequences(pool: sqlx::PgPool) -> anyhow::Result<()> {
            let meta = ongoing_indexing_scenario(pool).await?;

            schedule_merges(&meta, Seq::from(50i64)).await?;
            let jobs = get_all_merge_jobs(&meta).await?;
            assert!(jobs.is_empty());

            Ok(())
        }

        #[sqlx::test]
        async fn scheduling_with_ack_floor(pool: sqlx::PgPool) -> anyhow::Result<()> {
            let meta = ongoing_indexing_scenario(pool).await?;
            let ack_floor = Seq::from(100i64);

            schedule_merges(&meta, ack_floor).await?;
            let jobs = get_all_merge_jobs(&meta).await?;
            assert_eq!(jobs.len(), 1);

            let index = Index::get(&meta.pool, jobs[0].index_id).await?;
            let segments = index.segments(&meta.pool).await?;

            let mut segment_sequences = HashSet::new();
            for segment in &segments {
                if segment.seq <= ack_floor {
                    // should be scheduled to merge
                    assert!(segment.merge_job_id.is_some());
                    assert_eq!(segment.merge_job_id.unwrap(), jobs[0].id);
                    segment_sequences.insert(segment.seq);
                } else {
                    assert!(segment.merge_job_id.is_none());
                }
            }
            let expected = [95, 98, 99, 100i64].into_iter().map(Seq::from).collect();
            assert_eq!(segment_sequences, expected);

            Ok(())
        }

        async fn get_all_merge_jobs(meta: &NidxMetadata) -> anyhow::Result<Vec<MergeJob>> {
            let mut jobs = vec![];
            while let Some(job) = MergeJob::take(&meta.pool).await? {
                jobs.push(job);
            }
            Ok(jobs)
        }
    }
}
