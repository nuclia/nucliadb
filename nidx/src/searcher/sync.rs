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

use std::{
    cmp::max,
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use nidx_types::{SegmentMetadata, Seq};
use object_store::DynObjectStore;
use sqlx::types::time::PrimitiveDateTime;
use tokio::sync::RwLock;

use crate::{
    metadata::{Index, IndexId, Segment, SegmentId},
    segment_store::download_segment,
    NidxMetadata,
};

use super::{segment_path, SearcherOperation};

pub async fn run_sync(
    meta: NidxMetadata,
    work_dir: PathBuf,
    storage: Arc<DynObjectStore>,
    index_metadata: Arc<RwLock<HashMap<IndexId, Vec<SearcherOperation>>>>,
) -> anyhow::Result<()> {
    let mut last_updated_at = PrimitiveDateTime::MIN.replace_year(2000)?;
    loop {
        let indexes = Index::recently_updated(&meta.pool, last_updated_at).await?;
        for index in indexes {
            // TODO: Handle errors
            last_updated_at = max(last_updated_at, index.updated_at);
            sync_index(&meta, &work_dir, storage.clone(), index_metadata.clone(), index).await?;
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
    work_dir: &Path,
    storage: Arc<DynObjectStore>,
    index_metadata: Arc<RwLock<HashMap<IndexId, Vec<SearcherOperation>>>>,
    index: Index,
) -> anyhow::Result<()> {
    let read_index_metadata = index_metadata.read().await;
    let empty = vec![];
    let current_meta = read_index_metadata.get(&index.id).unwrap_or(&empty);
    let current_segs: HashSet<SegmentId> = current_meta.iter().flat_map(|s| s.segments.iter().map(|s| s.0)).collect();
    drop(read_index_metadata);

    let new_meta = sqlx::query_as!(
        SearcherSyncOperation,
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
    let new_segs: HashSet<SegmentId> = new_meta.iter().flat_map(|s| s.segment_ids.clone()).collect();

    // Calculate deletions
    let deleted_segments = current_segs.difference(&new_segs);
    let new_segments = new_segs.difference(&current_segs);

    // Download new segments
    for segment_id in new_segments {
        download_segment(storage.clone(), *segment_id, segment_path(work_dir, &index.id, segment_id)).await?;
    }

    // Switch meta
    let segments = sqlx::query_as!(
        Segment,
        "SELECT * FROM segments WHERE id = ANY($1)",
        new_segs.iter().collect::<Vec<&SegmentId>>() as Vec<&SegmentId>
    )
    .fetch_all(&meta.pool)
    .await?;
    let segment_meta_map: HashMap<SegmentId, Segment> = segments.into_iter().map(|s| (s.id, s)).collect();
    let new_full_meta = new_meta
        .into_iter()
        .map(|op| SearcherOperation {
            seq: op.seq,
            deleted_keys: op.deleted_keys,
            segments: op
                .segment_ids
                .iter()
                .map(|sid| {
                    let segment = segment_meta_map.get(sid).unwrap();
                    (
                        *sid,
                        SegmentMetadata {
                            records: segment.records.unwrap() as usize,
                            tags: HashSet::new(),
                            path: segment_path(work_dir, &index.id, sid),
                        },
                    )
                })
                .collect(),
        })
        .collect();
    index_metadata.write().await.insert(index.id, new_full_meta);

    // Delete unneeded segments
    for segment_id in deleted_segments {
        std::fs::remove_dir_all(segment_path(work_dir, &index.id, segment_id))?;
    }

    Ok(())
}
