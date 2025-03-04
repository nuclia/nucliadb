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

use std::sync::Arc;

use futures::StreamExt;
use object_store::DynObjectStore;
use tracing::*;

use crate::{NidxMetadata, metadata::Segment};

/// Purge segments that have not been ready for a while:
/// - Uploads that failed
/// - Recent deletions
pub async fn purge_segments(meta: &NidxMetadata, storage: &Arc<DynObjectStore>) -> anyhow::Result<()> {
    let deleted_segments = Segment::marked_as_deleted(&meta.pool).await?;
    let paths = deleted_segments.iter().map(|sid| Ok(sid.storage_key()));
    let results = storage
        .delete_stream(futures::stream::iter(paths).boxed())
        .collect::<Vec<_>>()
        .await;

    let mut deleted = Vec::new();
    for (segment_id, result) in deleted_segments.into_iter().zip(results.iter()) {
        match result {
            Ok(_) | Err(object_store::Error::NotFound { .. }) => deleted.push(segment_id),
            Err(e) => warn!("Error deleting segment from storage: {e:?}"),
        }
    }
    Segment::delete_many(&meta.pool, &deleted).await?;

    Ok(())
}

pub async fn purge_deletions(meta: &NidxMetadata, oldest_pending_seq: i64) -> anyhow::Result<()> {
    // Purge deletions that don't apply to any segment and won't apply to any
    // segment pending to process
    sqlx::query!(
        "WITH oldest_segments AS (
            SELECT index_id, MIN(seq) AS seq FROM segments
            WHERE delete_at IS NULL
            GROUP BY index_id
        )
        DELETE FROM deletions USING oldest_segments
        WHERE deletions.index_id = oldest_segments.index_id
        AND deletions.seq <= oldest_segments.seq
        AND deletions.seq <= $1",
        oldest_pending_seq
    )
    .execute(&meta.pool)
    .await?;

    // Purge deletions for indexes marked to delete
    sqlx::query!(
        "WITH indexes_to_delete AS (
             SELECT indexes.id
             FROM indexes
             WHERE indexes.deleted_at IS NOT NULL
         )
         DELETE FROM deletions USING indexes_to_delete
         WHERE deletions.index_id = indexes_to_delete.id"
    )
    .execute(&meta.pool)
    .await?;

    Ok(())
}

/// Purge shards and indexes marked to delete when it's safe to do so, i.e.,
/// after all segments and deletions have been removed
pub async fn purge_deleted_shards_and_indexes(meta: &NidxMetadata) -> anyhow::Result<()> {
    sqlx::query!(
        "DELETE FROM indexes
         WHERE (
             deleted_at IS NOT NULL
             AND NOT EXISTS(SELECT 1 FROM segments WHERE index_id = indexes.id)
             AND NOT EXISTS(SELECT 1 FROM deletions where index_id = indexes.id)
         )"
    )
    .execute(&meta.pool)
    .await?;

    sqlx::query!(
        "DELETE FROM shards
         WHERE (
             deleted_at IS NOT NULL
             AND NOT EXISTS(SELECT 1 FROM indexes WHERE shard_id = shards.id)
         )"
    )
    .execute(&meta.pool)
    .await?;

    Ok(())
}
