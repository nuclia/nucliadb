// Copyright 2021 Bosutech XXI S.L.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

use std::time::Duration;

use nidx::{
    metadata::{Index, IndexConfig, Segment, Shard},
    metrics,
};

use sqlx::PgPool;
use uuid::Uuid;

use crate::common::services::NidxFixture;

#[sqlx::test]
async fn test_initial_sync_fails(pool: PgPool) -> anyhow::Result<()> {
    // Create a shard that will fail to sync (no segment to download)
    let shard = Shard::create(&pool, Uuid::new_v4()).await?;
    let index = Index::create(&pool, shard.id, "text", IndexConfig::new_text()).await?;
    let segment = Segment::create(&pool, index.id, 1i64.into(), 1, Default::default()).await?;
    segment.mark_ready(&pool, 0).await?;

    // Run the shard
    let _fixture = NidxFixture::new(pool.clone()).await?;

    // Check that we are not synced yet
    tokio::time::sleep(Duration::from_secs(2)).await;
    let sync_delay = metrics::searcher::SYNC_DELAY.get();
    assert!(sync_delay > 1.0);

    // Delete the segment so now we can sync
    Segment::delete_many(&pool, &[segment.id]).await?;
    tokio::time::sleep(Duration::from_secs(10)).await;
    let sync_delay = metrics::searcher::SYNC_DELAY.get();
    assert_eq!(sync_delay, 0.0);

    Ok(())
}
