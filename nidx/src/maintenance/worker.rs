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

use std::{path::PathBuf, sync::Arc, time::Duration};

use anyhow::Ok;
use futures::TryStreamExt;
use object_store::DynObjectStore;
use tempfile::tempdir;
use tokio_util::compat::FuturesAsyncReadCompatExt;
use tokio_util::io::SyncIoBridge;

use crate::{
    indexer::WriteCounter,
    metadata::{Deletion, Index, MergeJob, Segment},
    NidxMetadata, Settings,
};

pub async fn run() -> anyhow::Result<()> {
    let settings = Settings::from_env();
    let storage = settings.indexer.as_ref().unwrap().object_store.client();
    let meta = NidxMetadata::new(&settings.metadata.database_url).await?;

    loop {
        let job = MergeJob::take(&meta.pool).await?;
        if let Some(job) = job {
            println!("Running job {}", job.id);
            run_job(&meta, &job, storage.clone()).await?;
        } else {
            println!("No jobs, waiting for more");
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }
}

pub async fn download_segment(
    storage: Arc<DynObjectStore>,
    segment_id: i64,
    output_dir: PathBuf,
) -> anyhow::Result<()> {
    println!("Download {segment_id}");
    let path = object_store::path::Path::parse(format!("segment/{}", segment_id)).unwrap();
    let response = storage.get(&path).await?.into_stream();
    let reader = response.map_err(|_| std::io::Error::last_os_error()).into_async_read(); // HACK: Mapping errors randomly
    let reader = SyncIoBridge::new(reader.compat());

    let mut tar = tar::Archive::new(reader);
    tokio::task::spawn_blocking(move || tar.unpack(output_dir.join(format!("{segment_id}"))).unwrap()).await?;
    println!("Downloaded {segment_id}");

    Ok(())
}

pub async fn run_job(meta: &NidxMetadata, job: &MergeJob, storage: Arc<DynObjectStore>) -> anyhow::Result<()> {
    // TODO: It's weird that we take the index_id from the first segment. There is no check to ensure all segments are from the same index
    // we just trust the scheduler to do the right thing. Maybe add a job param and check here? Should jobs be generic or keep the merge_job idea?
    let segments = job.segments(meta).await?;
    let deletions = sqlx::query_as!(
        Deletion,
        "SELECT * FROM deletions WHERE index_id = $1 AND seq <= $2 ORDER BY seq",
        segments[0].index_id,
        job.seq
    )
    .fetch_all(&meta.pool)
    .await?;

    // Start keep alive to mark progress
    let pool = meta.pool.clone();
    let job2 = (*job).clone();
    let keepalive = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            job2.keep_alive(&pool).await.unwrap();
        }
    });
    let work_dir: tempfile::TempDir = tempdir()?;

    // Download segments
    let downloads: Vec<_> = segments
        .iter()
        .map(|s| {
            let storage = storage.clone();
            let work_dir = work_dir.path().to_owned();
            tokio::spawn(download_segment(storage, s.id, work_dir))
        })
        .collect();
    for d in downloads {
        d.await??;
    }

    println!("Downloaded to {work_dir:?}, merging");
    let ssegments = &segments.iter().map(|s| (s.id, s.seq, s.records.unwrap())).collect::<Vec<_>>();
    let ddeletions = &deletions.iter().map(|d| (d.seq, &d.keys)).collect::<Vec<_>>();
    // HACK: Get index, match on kind
    let (merged, merged_records) = match segments[0].index_id {
        1 => nidx_vector::VectorIndexer::new().merge(work_dir.path(), ssegments, ddeletions)?,
        2 => nidx_fulltext::TextIndexer::new().merge(work_dir.path(), ssegments, ddeletions)?,
        _ => unimplemented!(),
    };
    println!("Merged to {merged:?}");

    // Upload (copied code from indexer)
    let segment = Segment::create(&meta, segments[0].index_id, job.seq).await?;
    let store_path = format!("segment/{}", segment.id);

    let mut upload = WriteCounter::new(object_store::buffered::BufWriter::new(storage, store_path.into()));
    let size = tokio::task::spawn_blocking(move || -> anyhow::Result<i64> {
        let mut tar = tar::Builder::new(&mut upload);
        tar.mode(tar::HeaderMode::Deterministic);
        tar.append_dir_all(".", work_dir.path().join(merged))?;
        tar.finish()?;
        drop(tar);
        let bytes = upload.finish()?;
        Ok(bytes as i64)
    })
    .await??;

    // Record new segment and delete old ones. TODO: Mark as deleted_at
    let mut tx = meta.transaction().await?;
    segment.mark_ready(&mut *tx, merged_records as i64, size).await?;
    Segment::delete_many(&mut *tx, &segments.iter().map(|s| s.id).collect::<Vec<_>>()).await?;
    job.finish(&mut *tx).await?;
    tx.commit().await?;

    // Stop keep alives. TODO: Stop on failure as well. This probably makes more sense on the outer function and/or wrapped in an struct with Drop
    // It currently works because everything panics on error
    keepalive.abort();

    // Delete task if successful. Mark as failed otherwise?
    // The scheduler will requeue this if no activity in a while
    // job.finish(meta).await?;

    Ok(())
}
