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
