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

use std::env;
use std::sync::Arc;

use lazy_static::lazy_static;
use nucliadb_core::tracing::error;
use nucliadb_core::NodeResult;
use tokio::sync::{Semaphore, SemaphorePermit};
use tokio::time::{self, Duration, Interval};

use crate::shards::shard_writer::ShardWriter;

lazy_static! {
    // Merge interval -- defaults to three hours
    static ref MERGE_INTERVAL_SECS: u64 = {
        let val = env::var("MERGE_INTERVAL_SECS").ok();
        val.and_then(|value| value.parse().ok())
            .unwrap_or(10800)
    };

    // GC interval -- defaults to 24 hours
    static ref GC_INTERVAL_SECS: u64 = {
        let val = env::var("GC_INTERVAL_SECS").ok();
        val.and_then(|value| value.parse().ok())
            .unwrap_or(86400)
    };
}

async fn schedule<'a>(
    interval: &mut Interval,
    permits: &'a Semaphore,
) -> NodeResult<SemaphorePermit<'a>> {
    interval.tick().await;
    let permit = permits.acquire().await?;
    Ok(permit)
}

pub async fn scheduler_task(shard: Arc<ShardWriter>, permits: Arc<Semaphore>) {
    let mut merge_interval = time::interval(Duration::from_secs(*MERGE_INTERVAL_SECS));
    let mut gc_interval = time::interval(Duration::from_secs(*GC_INTERVAL_SECS));

    // First instants return immediately
    merge_interval.tick().await;
    merge_interval.tick().await;

    loop {
        let (handler, permit) = tokio::select! {
            permit = schedule(&mut merge_interval, &permits) => {
                let Ok(permit) = permit else { break; };
                let event_shard = Arc::clone(&shard);
                let handler = tokio::task::spawn_blocking(move || event_shard.merge());
                (handler, permit)
            }
            permit = schedule(&mut gc_interval, &permits) => {
                let Ok(permit) = permit else { break; };
                let event_shard = Arc::clone(&shard);
                let handler = tokio::task::spawn_blocking(move || event_shard.gc());
                (handler, permit)
            }
        };
        match handler.await {
            Ok(Ok(_)) => (),
            Ok(Err(err)) => error!("Scheduler operation failed: {err:?}"),
            Err(error) => error!("Task panicked: {error:?}"),
        }
        std::mem::drop(permit)
    }
}
