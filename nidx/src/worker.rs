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
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::anyhow;
use nidx_paragraph::ParagraphIndexer;
use nidx_relation::RelationIndexer;
use nidx_text::TextIndexer;
use nidx_types::{OpenIndexMetadata, SegmentMetadata, Seq};
use nidx_vector::VectorIndexer;
use object_store::DynObjectStore;
use serde::Deserialize;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::*;

use crate::{
    NidxMetadata, Settings,
    metadata::{Deletion, Index, IndexKind, MergeJob, NewSegment, Segment},
    metrics::{IndexKindLabels, OperationStatusLabels, worker::*},
    segment_store::{download_segment, pack_and_upload},
    utilization_tracker::UtilizationTracker,
};

pub async fn run(settings: Settings, shutdown: CancellationToken) -> anyhow::Result<()> {
    let storage = settings.storage.as_ref().unwrap().object_store.clone();
    let meta = settings.metadata.clone();

    let work_path = match &settings.work_path {
        Some(work_path) => PathBuf::from(work_path),
        None => tempfile::env::temp_dir(),
    };

    let utilization = UtilizationTracker::new(|busy, duration| {
        WORKER_BUSY.get_or_create(&busy.into()).inc_by(duration.as_secs_f64());
    });

    while !shutdown.is_cancelled() {
        let job = MergeJob::take(&meta.pool).await?;
        if let Some(job) = job {
            utilization.busy().await;
            let span = info_span!("worker_job", ?job.id);
            info!(job.id, "Running job");

            // Start keep alive to mark progress
            let pool = meta.pool.clone();
            let job2 = job.clone();
            let keepalive = tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(45)).await;
                    job2.keep_alive(&pool).await.unwrap();
                }
            });

            match run_job(&meta, &job, storage.clone(), &work_path).instrument(span).await {
                Ok(_) => {
                    MERGE_COUNTER.get_or_create(&OperationStatusLabels::success()).inc();
                    info!(job.id, "Job completed")
                }
                Err(e) => {
                    MERGE_COUNTER.get_or_create(&OperationStatusLabels::failure()).inc();
                    error!(job.id, "Merge job failed: {e:?}")
                }
            }

            // Stop keep alives
            keepalive.abort();
        } else {
            utilization.idle().await;
            debug!("No jobs, waiting for more");
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(5)) => {},
                _ = shutdown.cancelled() => {},
            }
        }
    }

    Ok(())
}

/// This structure (its trait) is passed to the indexes in order to open a searcher.
/// This implementation of the trait takes the data in the format available to a worker
/// that is merging segments for an index: a list of DB models for Segments and Deletions.
struct MergeInputs {
    work_dir: PathBuf,
    segments: Vec<Segment>,
    deletions: Vec<Deletion>,
}

impl<T: for<'de> Deserialize<'de>> OpenIndexMetadata<T> for MergeInputs {
    fn segments(&self) -> impl Iterator<Item = (SegmentMetadata<T>, Seq)> {
        self.segments
            .iter()
            .enumerate()
            .map(|(idx, segment)| (segment.metadata(self.work_dir.join(idx.to_string())), segment.seq))
    }

    fn deletions(&self) -> impl Iterator<Item = (&String, Seq)> {
        self.deletions
            .iter()
            .flat_map(|del| del.keys.iter().map(|key| (key, del.seq)))
    }
}

pub async fn run_job(
    meta: &NidxMetadata,
    job: &MergeJob,
    storage: Arc<DynObjectStore>,
    work_path: &Path,
) -> anyhow::Result<()> {
    let t = Instant::now();
    let segments = job.segments(&meta.pool).await?;
    let index = Index::get(&meta.pool, job.index_id).await?;
    for s in &segments {
        assert!(
            s.index_id == index.id,
            "Jobs must only use segments from a single index or we could end with a multi-index merge!"
        );
    }
    let segment_ids = segments.iter().map(|s| s.id).collect::<Vec<_>>();
    let deletions = Deletion::for_index_and_seq(&meta.pool, index.id, job.seq).await?;
    let download_dir = tempfile::tempdir_in(work_path)?;

    // Download segments
    let mut download_tasks = JoinSet::new();
    segments.iter().enumerate().for_each(|(i, s)| {
        let storage = storage.clone();
        let work_dir = download_dir.path().join(i.to_string());
        download_tasks.spawn(download_segment(storage, s.id, work_dir));
    });
    let span = info_span!("download_segments");
    match download_tasks
        .join_all()
        .instrument(span)
        .await
        .into_iter()
        .reduce(Result::and)
    {
        None => return Err(anyhow!("No segments downloaded")),
        Some(Err(e)) => return Err(e),
        Some(Ok(())) => {}
    };

    let index = Index::get(&meta.pool, segments[0].index_id).await?;
    let merge_inputs = MergeInputs {
        work_dir: download_dir.path().to_path_buf(),
        segments,
        deletions,
    };

    let work_dir = tempfile::tempdir_in(work_path)?;
    let work_path = work_dir.path().to_path_buf();
    let span = Span::current();
    let index2 = index.clone();
    let merged: NewSegment = tokio::task::spawn_blocking(move || {
        span.in_scope(|| match index.kind {
            IndexKind::Vector => VectorIndexer
                .merge(&work_path, index2.config().unwrap(), merge_inputs)
                .map(|x| x.into()),
            IndexKind::Text => TextIndexer
                .merge(&work_path, index2.config().unwrap(), merge_inputs)
                .map(|x| x.into()),
            IndexKind::Paragraph => ParagraphIndexer.merge(&work_path, merge_inputs).map(|x| x.into()),
            IndexKind::Relation => RelationIndexer
                .merge(&work_path, index2.config().unwrap(), merge_inputs)
                .map(|x| x.into()),
        })
    })
    .await??;

    let mut tx = if merged.records == 0 {
        // If segment empty, do not upload/register it
        meta.transaction().await?
    } else {
        // Upload
        let segment = Segment::create(&meta.pool, job.index_id, job.seq, merged.records, merged.index_metadata).await?;
        let size = pack_and_upload(storage, work_dir.path(), segment.id.storage_key()).await?;

        // Record new segment and delete old ones
        let mut tx = meta.transaction().await?;
        segment.mark_ready(&mut *tx, size as i64).await?;
        tx
    };

    // Delete old segments
    Segment::mark_many_for_deletion(&mut *tx, &segment_ids).await?;
    Index::updated(&mut *tx, &index.id).await?;

    // Delete task if successful.
    job.finish(&mut *tx).await?;
    tx.commit().await?;

    PER_INDEX_MERGE_TIME
        .get_or_create(&IndexKindLabels::new(index.kind))
        .observe(t.elapsed().as_secs_f64());

    Ok(())
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;
    use uuid::Uuid;

    use super::*;
    use crate::{
        indexer::{delete_resource, index_resource},
        metadata::{IndexConfig, NidxMetadata, Shard},
    };
    use nidx_tests::*;

    #[sqlx::test]
    async fn test_merge_deleted_resource(pool: sqlx::PgPool) -> anyhow::Result<()> {
        let meta = NidxMetadata::new_with_pool(pool).await.unwrap();
        let kbid = Uuid::new_v4();
        let shard = Shard::create(&meta.pool, kbid).await.unwrap();
        let index = Index::create(&meta.pool, shard.id, "text", IndexConfig::new_text())
            .await
            .unwrap();

        let resource = little_prince(shard.id.to_string(), None);
        let resource_id = resource.resource.clone().unwrap().uuid;
        let storage = Arc::new(object_store::memory::InMemory::new());
        index_resource(
            &meta,
            storage.clone(),
            &tempfile::env::temp_dir(),
            &shard.id.to_string(),
            resource,
            1i64.into(),
        )
        .await?;

        let segments = index.segments(&meta.pool).await.unwrap();
        assert_eq!(segments.len(), 1);

        delete_resource(&meta, &shard.id.to_string(), resource_id, 2i64.into()).await?;

        // Run a job to merge the segment
        let output_dir = tempdir()?;
        let job = MergeJob::create(&meta, index.id, &[segments[0].id], 2i64.into(), 0).await?;
        run_job(&meta, &job, storage, output_dir.path()).await?;

        // Results in no segments
        let segments = index.segments(&meta.pool).await.unwrap();
        assert_eq!(segments.len(), 0);

        Ok(())
    }
}
