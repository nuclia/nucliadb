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
            if let Err(e) = purge_deletions(&meta2, &mut consumer2).await {
                warn!(?e, "Error in purge_deletions task");
            }
            sleep(Duration::from_secs(15)).await;
        }
    });

    tasks.spawn(async move {
        loop {
            match consumer.info().await {
                Ok(consumer_info) => {
                    let oldest_confirmed_seq = consumer_info.ack_floor.stream_sequence;
                    let oldest_pending_seq = oldest_confirmed_seq + 1;

                    if let Err(e) = schedule_merges(&meta, Seq::from(oldest_pending_seq)).await {
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

pub async fn purge_deletions(meta: &NidxMetadata, consumer: &mut PullConsumer) -> anyhow::Result<()> {
    let oldest_confirmed_seq = consumer.info().await?.ack_floor.stream_sequence;
    let oldest_pending_seq = oldest_confirmed_seq + 1;

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

/// Enqueue merge jobs for segments older than `before_seq` that aren't already
/// scheduled or marked to delete. Right now we merge everything that we can
async fn schedule_merges(meta: &NidxMetadata, before_seq: Seq) -> anyhow::Result<()> {
    // TODO: better merge algorithm
    let merges = sqlx::query!(
        r#"SELECT index_id, array_agg(id) AS "segment_ids!: Vec<SegmentId>" FROM segments WHERE delete_at IS NULL AND merge_job_id IS NULL AND seq < $1 GROUP BY index_id"#,
        i64::from(before_seq),
    )
    .fetch_all(&meta.pool)
    .await?;
    for m in merges {
        if m.segment_ids.len() > 3 {
            MergeJob::create(meta, m.index_id.into(), &m.segment_ids, before_seq).await?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    mod merge_scheduling {
        use std::collections::HashSet;

        use nidx_types::Seq;
        use uuid::Uuid;

        use super::*;

        use crate::metadata::{Index, IndexId, IndexKind, NidxMetadata, Shard};

        #[sqlx::test]
        async fn test_schedule_merges_for_shard_with_single_index(pool: sqlx::PgPool) -> anyhow::Result<()> {
            let meta = NidxMetadata::new_with_pool(pool).await?;
            let kbid = Uuid::new_v4();
            let shard = Shard::create(&meta.pool, kbid).await?;
            let index = Index::create(&meta.pool, shard.id, IndexKind::Vector, Some("multilingual")).await?;
            let mut seq: i64 = 0;

            for _ in 0..10 {
                let segment = Segment::create(&meta.pool, index.id, Seq::from(seq)).await?;
                segment.mark_ready(&meta.pool, 50, 1000).await?;
                seq += 1;
            }

            // creation of shards/indexes/segments don't trigger any merge job
            assert!(MergeJob::take(&meta.pool).await?.is_none());

            schedule_merges(&meta, 100i64.into()).await?;

            // one job has been scheduled for the index
            let mut jobs = vec![];
            while let Some(job) = MergeJob::take(&meta.pool).await? {
                jobs.push(job)
            }
            assert_eq!(jobs.len(), 1);
            assert_eq!(IndexId::from(jobs[0].index_id), index.id);

            Ok(())
        }

        #[sqlx::test]
        async fn test_schedule_merges_for_shard_with_multiple_indexes(pool: sqlx::PgPool) -> anyhow::Result<()> {
            let meta = NidxMetadata::new_with_pool(pool).await?;
            let kbid = Uuid::new_v4();
            let shard = Shard::create(&meta.pool, kbid).await?;

            let indexes = vec![
                Index::create(&meta.pool, shard.id, IndexKind::Vector, Some("multilingual")).await?,
                Index::create(&meta.pool, shard.id, IndexKind::Vector, Some("english")).await?,
                Index::create(&meta.pool, shard.id, IndexKind::Text, Some("fulltext")).await?,
                Index::create(&meta.pool, shard.id, IndexKind::Paragraph, Some("keyword")).await?,
                Index::create(&meta.pool, shard.id, IndexKind::Relation, Some("relation")).await?,
            ];
            let mut seq: i64 = 0;

            for _ in 0..10 {
                for index in indexes.iter() {
                    let segment = Segment::create(&meta.pool, index.id, Seq::from(seq)).await?;
                    segment.mark_ready(&meta.pool, 50, 1000).await?;
                    seq += 1;
                }
            }

            schedule_merges(&meta, 100i64.into()).await?;

            let mut jobs = vec![];
            while let Some(job) = MergeJob::take(&meta.pool).await? {
                jobs.push(job);
            }

            // scheduled a job per index

            assert_eq!(jobs.len(), indexes.len());

            let indexes_ids: HashSet<_> = indexes.iter().map(|i| i.id).collect();
            let job_indexes: HashSet<_> = jobs.iter().map(|j| j.index_id).collect();

            assert_eq!(indexes_ids, job_indexes);

            Ok(())
        }
    }
}
