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

use nidx_types::Seq;
use object_store::DynObjectStore;
use sqlx::types::time::PrimitiveDateTime;
use tempfile::tempdir;
use tokio::sync::RwLock;

use crate::{
    metadata::{Index, IndexId, SegmentId},
    segment_store::download_segment,
    NidxMetadata, Settings,
};

#[derive(Clone)]
pub struct SearchOperation {
    seq: Seq,
    segment_ids: Vec<SegmentId>,
    deleted_keys: Vec<String>,
}

pub async fn run() -> anyhow::Result<()> {
    let work_dir = tempdir()?;
    let settings = Settings::from_env();
    let meta = NidxMetadata::new(&settings.metadata.database_url).await?;
    let storage = settings.storage.expect("Storage settings needed").object_store.client();

    let index_metadata = Arc::new(RwLock::new(HashMap::new()));

    let sync_task =
        tokio::task::spawn(run_sync(meta, work_dir.path().to_path_buf(), storage.clone(), index_metadata.clone()));

    tokio::select! {
        r = sync_task => {
            println!("sync_task() completed first {:?}", r)
        }
    }

    Ok(())
}

async fn run_sync(
    meta: NidxMetadata,
    work_dir: PathBuf,
    storage: Arc<DynObjectStore>,
    index_metadata: Arc<RwLock<HashMap<IndexId, Vec<SearchOperation>>>>,
) -> anyhow::Result<()> {
    let mut last_updated_at = PrimitiveDateTime::MIN;
    loop {
        let indexes = Index::recently_updated(&meta.pool, last_updated_at).await?;
        for index in indexes {
            // TODO: Handle errors
            last_updated_at = max(last_updated_at, index.updated_at);
            sync_index(&meta, &work_dir, storage.clone(), index_metadata.clone(), index).await?;
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

fn segment_path(work_dir: &Path, index_id: &IndexId, segment_id: &SegmentId) -> PathBuf {
    work_dir.join(format!("{index_id:?}/{segment_id:?}"))
}

async fn sync_index(
    meta: &NidxMetadata,
    work_dir: &Path,
    storage: Arc<DynObjectStore>,
    index_metadata: Arc<RwLock<HashMap<IndexId, Vec<SearchOperation>>>>,
    index: Index,
) -> anyhow::Result<()> {
    let read_index_metadata = index_metadata.read().await;
    let empty = vec![];
    let current_meta = read_index_metadata.get(&index.id).unwrap_or(&empty);
    let current_segs: HashSet<SegmentId> = current_meta.iter().flat_map(|s| s.segment_ids.clone()).collect();
    drop(read_index_metadata);

    let new_meta = sqlx::query_as!(
        SearchOperation,
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
    index_metadata.write().await.insert(index.id, new_meta);

    // Delete unneeded segments
    for segment_id in deleted_segments {
        std::fs::remove_dir_all(segment_path(work_dir, &index.id, segment_id))?;
    }

    Ok(())
}
