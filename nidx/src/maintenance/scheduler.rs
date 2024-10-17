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
            if let Err(e) = schedule_merges(&meta, &mut consumer).await {
                warn!(?e, "Error in schedule_merges task");
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

pub async fn schedule_merges(meta: &NidxMetadata, consumer: &mut PullConsumer) -> anyhow::Result<()> {
    let oldest_confirmed_seq = consumer.info().await?.ack_floor.stream_sequence;
    let oldest_pending_seq = oldest_confirmed_seq + 1;

    // Enqueue merges. Right now, merge everything that we can
    // TODO: better merge algorithm
    let merges = sqlx::query_scalar!(
        r#"SELECT array_agg(id) AS "segment_ids!" FROM segments WHERE delete_at IS NULL AND merge_job_id IS NULL AND seq < $1 GROUP BY index_id"#,
        oldest_pending_seq as i64
    )
    .fetch_all(&meta.pool)
    .await?;
    for m in merges {
        if m.len() > 3 {
            MergeJob::create(&meta, &m, oldest_confirmed_seq as i64).await?;
        }
    }

    Ok(())
}
