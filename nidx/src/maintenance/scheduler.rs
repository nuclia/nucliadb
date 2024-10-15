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

use std::time::Duration;

use async_nats::jetstream::consumer::PullConsumer;
use futures::StreamExt;
use object_store::path::Path;
use tokio::time::sleep;

use crate::{metadata::MergeJob, NidxMetadata, Settings};

pub async fn run() -> anyhow::Result<()> {
    let settings = Settings::from_env();
    let indexer_settings = settings.indexer.unwrap();
    let meta = NidxMetadata::new(&settings.metadata.database_url).await?;
    let storage = indexer_settings.object_store.client();

    let client = async_nats::connect(indexer_settings.nats_server).await?;
    let jetstream = async_nats::jetstream::new(client);
    let mut consumer: PullConsumer = jetstream.get_consumer_from_stream("nidx", "nidx").await?;

    // TODO: Do this in different tasks with different frequencies?
    loop {
        // Requeue failed jobs (with retries left)
        let retry_jobs = sqlx::query_as!(MergeJob, "UPDATE merge_jobs SET started_at = NULL, running_at = NULL, retries = retries + 1 WHERE running_at < NOW() - INTERVAL '5 seconds' AND retries < 2 RETURNING *").fetch_all(&meta.pool).await?;
        for j in retry_jobs {
            println!("Retrying {} for {} time", j.id, j.retries);
        }

        // Delete failed jobs (no retries left)
        let failed_jobs = sqlx::query_as!(
            MergeJob,
            "DELETE FROM merge_jobs WHERE running_at < NOW() - INTERVAL '5 seconds' AND retries >= 2 RETURNING *"
        )
        .fetch_all(&meta.pool)
        .await?;
        for j in failed_jobs {
            println!("Failed job {}", j.id);
        }

        // Warn of long jobs
        let long_jobs =
            sqlx::query_as!(MergeJob, "SELECT * FROM merge_jobs WHERE running_at - started_at > INTERVAL '1 minute'")
                .fetch_all(&meta.pool)
                .await?;
        for j in long_jobs {
            println!("Job has been running for a while {}", j.id);
        }

        // Delete non-ready segments
        let deleted_segments =
            sqlx::query_scalar!("SELECT id FROM segments WHERE delete_at < NOW()").fetch_all(&meta.pool).await?;
        let paths = deleted_segments.iter().map(|sid| Ok(Path::parse(format!("segment/{sid}")).unwrap()));
        let results = storage.delete_stream(futures::stream::iter(paths).boxed()).collect::<Vec<_>>().await;
        for r in results {
            r?;
        }
        sqlx::query!("DELETE FROM segments WHERE id = ANY($1)", &deleted_segments).execute(&meta.pool).await?;

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

        // Enqueue merges. Right now, merge everything that we can
        // TODO: better merge algorithm
        let merges = sqlx::query_scalar!(
            r#"SELECT array_agg(id) AS "segment_ids!" FROM segments WHERE delete_at IS NULL AND merge_job_id IS NULL AND seq < $1 GROUP BY index_id"#,
            oldest_pending_seq as i64
        )
        .fetch_all(&meta.pool)
        .await?;
        for m in merges {
            if m.len() > 1 {
                MergeJob::create(&meta, &m, oldest_confirmed_seq as i64).await?;
            }
        }

        sleep(Duration::from_secs(5)).await;
    }
}
