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

mod log_merge;
mod merge_task;
mod metrics_task;
mod purge_tasks;
mod vector_merge;

use crate::{metadata::MergeJob, settings::MergeSettings, NidxMetadata, Settings};
use async_nats::jetstream::consumer::PullConsumer;
use merge_task::MergeScheduler;
use nidx_types::Seq;
use object_store::DynObjectStore;
// TODO: This should not be public but it's used in tests
pub use purge_tasks::*;
use std::{future::Future, sync::Arc, time::Duration};
use tokio::{task::JoinSet, time::sleep};
use tokio_util::sync::CancellationToken;
use tracing::*;

pub async fn run(settings: Settings, shutdown: CancellationToken) -> anyhow::Result<()> {
    let indexer_settings = settings.indexer.as_ref().unwrap();
    let storage_settings = settings.storage.as_ref().unwrap();
    let merge_settings = settings.merge.clone();
    let meta = settings.metadata.clone();

    let client = async_nats::connect(&indexer_settings.nats_server).await?;
    let jetstream = async_nats::jetstream::new(client);
    let consumer: PullConsumer = jetstream.get_consumer_from_stream("nidx", "nidx").await?;

    tokio::select! {
        _ = run_tasks(meta, storage_settings.object_store.clone(), merge_settings, NatsAckFloor(consumer)) => {},
        _ = shutdown.cancelled() => {}
    }

    Ok(())
}

#[derive(Clone)]
struct NatsAckFloor(PullConsumer);

impl GetAckFloor for NatsAckFloor {
    async fn get(&mut self) -> anyhow::Result<i64> {
        Ok(self.0.info().await?.ack_floor.stream_sequence as i64)
    }
}

pub trait GetAckFloor {
    fn get(&mut self) -> impl Future<Output = anyhow::Result<i64>> + Send;
}

pub async fn run_tasks(
    meta: NidxMetadata,
    storage: Arc<DynObjectStore>,
    merge_settings: MergeSettings,
    mut ack_floor: impl GetAckFloor + Clone + Send + 'static,
) -> anyhow::Result<()> {
    let mut tasks = JoinSet::new();

    let meta2 = meta.clone();
    tasks.spawn(async move {
        loop {
            if let Err(e) = retry_jobs(&meta2).await {
                warn!("Error in retry_jobs task: {e:?}");
            }
            sleep(Duration::from_secs(30)).await;
        }
    });

    let meta2 = meta.clone();
    let storage = storage.clone();
    tasks.spawn(async move {
        loop {
            if let Err(e) = purge_segments(&meta2, &storage).await {
                warn!("Error in purge_segments task: {e:?}");
            }
            sleep(Duration::from_secs(60)).await;
        }
    });

    let meta2 = meta.clone();
    tasks.spawn(async move {
        loop {
            if let Err(e) = purge_deleted_shards_and_indexes(&meta2).await {
                warn!("Error purging deleted shards and indexes: {e:?}");
            }
            sleep(Duration::from_secs(60)).await;
        }
    });

    let meta2 = meta.clone();
    let mut ack_floor_copy = ack_floor.clone();
    tasks.spawn(async move {
        loop {
            match ack_floor_copy.get().await {
                Ok(oldest_confirmed_seq) => {
                    let oldest_pending_seq = oldest_confirmed_seq + 1;
                    if let Err(e) = purge_deletions(&meta2, oldest_pending_seq).await {
                        warn!("Error in purge_deletions task: {e:?}");
                    }
                }
                Err(e) => {
                    warn!("Error while getting consumer information: {e:?}");
                }
            }
            sleep(Duration::from_secs(15)).await;
        }
    });

    let meta2 = meta.clone();
    tasks.spawn(async move {
        let merge_scheduler = MergeScheduler::from_settings(merge_settings);
        loop {
            match ack_floor.get().await {
                Ok(oldest_confirmed_seq) => {
                    if let Err(e) = merge_scheduler.schedule_merges(&meta2, Seq::from(oldest_confirmed_seq)).await {
                        warn!("Error in schedule_merges task: {e:?}");
                    }
                }
                Err(e) => {
                    warn!("Error while getting consumer information: {e:?}");
                }
            }
            sleep(Duration::from_secs(15)).await;
        }
    });

    let meta2 = meta.clone();
    tasks.spawn(async move {
        loop {
            if let Err(e) = metrics_task::update_metrics(&meta2).await {
                info!("Error updating scheduler metrics: {e:?}");
            }
            sleep(Duration::from_secs(15)).await;
        }
    });

    let task = tasks.join_next().await;
    error!(?task, "A scheduling task finished, exiting");

    Ok(())
}

/// Re-enqueues jobs that have been stuck for a while without ack'ing
pub async fn retry_jobs(meta: &NidxMetadata) -> anyhow::Result<()> {
    // Requeue failed jobs (with retries left)
    let retry_jobs = sqlx::query_as!(
        MergeJob,
        "UPDATE merge_jobs
         SET started_at = NULL, running_at = NULL, retries = retries + 1
         WHERE running_at < NOW() - INTERVAL '1 minute' AND retries < 4 RETURNING *"
    )
    .fetch_all(&meta.pool)
    .await?;
    for j in retry_jobs {
        debug!(j.id, j.retries, "Retrying job");
    }

    // Delete failed jobs (no retries left)
    let failed_jobs = sqlx::query_as!(
        MergeJob,
        "DELETE FROM merge_jobs
         WHERE running_at < NOW() - INTERVAL '1 minute' AND retries >= 4 RETURNING *"
    )
    .fetch_all(&meta.pool)
    .await?;
    for j in failed_jobs {
        error!(j.id, "Failed job");
    }

    Ok(())
}
