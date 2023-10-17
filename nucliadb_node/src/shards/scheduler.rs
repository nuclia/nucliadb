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

use std::sync::Arc;

use nucliadb_core::tracing::error;
use tokio::time;
use tokio::time::Duration;

use crate::shards::shard_writer::ShardWriter;

// Every three hours a merge is triggered
const MERGE_INTERVAL_SECS: u64 = 10800;
// Everyday a shard is garbage collected
const GC_INTERVAL_SECS: u64 = 10800;

pub async fn scheduler_task(shard: Arc<ShardWriter>) {
    let mut merge_interval = time::interval(Duration::from_secs(MERGE_INTERVAL_SECS));
    let mut gc_interval = time::interval(Duration::from_secs(GC_INTERVAL_SECS));
    loop {
        let performed = tokio::select! {
            _ = merge_interval.tick() => {
                let event_shard = Arc::clone(&shard);
                tokio::task::spawn_blocking(move || event_shard.merge())
            }
            _ = gc_interval.tick() => {
                let event_shard = Arc::clone(&shard);
                tokio::task::spawn_blocking(move || event_shard.gc())
            }
        };
        match performed.await {
            Ok(Ok(_)) => (),
            Ok(Err(err)) => error!("Scheduler operation failed: {err:?}"),
            Err(error) => error!("Task panicked: {error:?}"),
        }
    }
}
