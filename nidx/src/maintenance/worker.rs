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

use std::{collections::HashSet, sync::Arc, time::Duration};

use anyhow::anyhow;
use nidx_types::SegmentMetadata;
use object_store::DynObjectStore;
use tempfile::tempdir;
use tokio::task::JoinSet;
use tracing::*;

use crate::{
    metadata::{Deletion, Index, IndexKind, MergeJob, Segment},
    segment_store::{download_segment, pack_and_upload},
    NidxMetadata, Settings,
};

pub async fn run() -> anyhow::Result<()> {
    let settings = Settings::from_env();
    let storage = settings.storage.as_ref().unwrap().object_store.client();
    let meta = NidxMetadata::new(&settings.metadata.database_url).await?;

    loop {
        let job = MergeJob::take(&meta.pool).await?;
        if let Some(job) = job {
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

            match run_job(&meta, &job, storage.clone()).await {
                Ok(_) => info!(job.id, "Job completed"),
                Err(e) => warn!(job.id, ?e, "Job failed"),
            }

            // Stop keep alives
            keepalive.abort();
        } else {
            debug!("No jobs, waiting for more");
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }
}

pub async fn run_job(meta: &NidxMetadata, job: &MergeJob, storage: Arc<DynObjectStore>) -> anyhow::Result<()> {
    // TODO: Should jobs be generic or keep the merge_job idea?
    let segments = job.segments(&meta.pool).await?;
    let index = Index::get(&meta.pool, job.index_id).await?;
    for s in &segments {
        assert!(
            s.index_id == index.id,
            "Jobs must only use segments from a single index or we could end with a multi-index merge!"
        );
    }
    let deletions = Deletion::for_index_and_seq(&meta.pool, index.id, job.seq).await?;
    let work_dir: tempfile::TempDir = tempdir()?;

    // Download segments
    let mut download_tasks = JoinSet::new();
    segments.iter().enumerate().for_each(|(i, s)| {
        let storage = storage.clone();
        let work_dir = work_dir.path().join(i.to_string());
        download_tasks.spawn(download_segment(storage, s.id, work_dir));
    });
    match download_tasks.join_all().await.into_iter().reduce(Result::and) {
        None => return Err(anyhow!("No segments downloaded")),
        Some(Err(e)) => return Err(e),
        Some(Ok(())) => {}
    };

    // TODO: Define a structure that gets passed to indices with all the needed information, better than random tuples :)
    let ssegments = segments
        .iter()
        .enumerate()
        .map(|(i, s)| {
            (
                SegmentMetadata {
                    path: work_dir.path().join(i.to_string()),
                    records: s.records.unwrap() as usize,
                    tags: HashSet::new(), // TODO: Record tags in database
                },
                s.seq,
            )
        })
        .collect::<Vec<_>>();
    let ddeletions = &deletions.iter().map(|d| (d.seq, &d.keys)).collect::<Vec<_>>();

    let index = Index::get(&meta.pool, segments[0].index_id).await?;
    let merged_records = match index.kind {
        IndexKind::Vector => nidx_vector::VectorIndexer.merge(work_dir.path(), ssegments, ddeletions)?,
        _ => unimplemented!(),
    };

    // Upload
    let segment = Segment::create(&meta.pool, segments[0].index_id, job.seq).await?;
    let size = pack_and_upload(storage, work_dir.path(), segment.id.storage_key()).await?;

    // Record new segment and delete old ones. TODO: Mark as deleted_at
    let mut tx = meta.transaction().await?;
    segment.mark_ready(&mut *tx, merged_records as i64, size as i64).await?;
    Segment::delete_many(&mut *tx, &segments.iter().map(|s| s.id).collect::<Vec<_>>()).await?;
    index.updated(&mut *tx).await?;
    // Delete task if successful. Mark as failed otherwise?
    job.finish(&mut *tx).await?;
    tx.commit().await?;

    Ok(())
}
