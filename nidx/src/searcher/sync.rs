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

use std::{cmp::max, sync::Arc, time::Duration};

use nidx_types::Seq;
use object_store::DynObjectStore;
use sqlx::types::time::PrimitiveDateTime;

use crate::{
    metadata::{Index, IndexId, SegmentId},
    searcher::metadata::SeqMetadata,
    segment_store::download_segment,
    NidxMetadata,
};

use super::metadata::{Operations, SearchMetadata};

pub async fn run_sync(
    meta: NidxMetadata,
    storage: Arc<DynObjectStore>,
    index_metadata: Arc<SearchMetadata>,
) -> anyhow::Result<()> {
    let mut last_updated_at = PrimitiveDateTime::MIN.replace_year(2000)?;
    loop {
        let indexes = Index::recently_updated(&meta.pool, last_updated_at).await?;
        for index in indexes {
            // TODO: Handle errors
            last_updated_at = max(last_updated_at, index.updated_at);
            sync_index(&meta, storage.clone(), index_metadata.clone(), index).await?;
        }

        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

pub struct SearcherSyncOperation {
    seq: Seq,
    segment_ids: Vec<SegmentId>,
    deleted_keys: Vec<String>,
}

async fn sync_index(
    meta: &NidxMetadata,
    storage: Arc<DynObjectStore>,
    index_metadata: Arc<SearchMetadata>,
    index: Index,
) -> anyhow::Result<()> {
    let new_meta = sqlx::query_as!(
        SeqMetadata,
        r#"WITH ready_segments AS (
            SELECT index_id, seq, array_agg(id) AS segment_ids
                FROM segments
                WHERE delete_at IS NULL
                GROUP BY index_id, seq
            )
            SELECT
            COALESCE(ready_segments.seq, deletions.seq) AS "seq!",
            COALESCE(segment_ids, '{}') AS "segment_ids!: Vec<SegmentId>",
            COALESCE(deletions.keys, '{}') AS "deleted_keys!"
            FROM ready_segments
            NATURAL FULL OUTER JOIN deletions
            WHERE index_id = $1
            ORDER BY seq;"#,
        index.id as IndexId
    )
    .fetch_all(&meta.pool)
    .await?;

    let operations = Operations(new_meta);

    let diff = index_metadata.diff(&index.id, &operations).await;

    // Download new segments
    for segment_id in diff.added_segments {
        download_segment(storage.clone(), segment_id, index_metadata.segment_location(&index.id, &segment_id)).await?;
    }

    // Switch meta
    let index_id = index.id;
    index_metadata.set(index, operations).await;

    // Delete unneeded segments
    for segment_id in diff.removed_segments {
        std::fs::remove_dir_all(index_metadata.segment_location(&index_id, &segment_id))?;
    }

    Ok(())
}
