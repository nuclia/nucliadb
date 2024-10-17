use core::sync;
use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use futures::TryStreamExt;
use nidx_text::TextSearcher;
use nidx_vector::VectorSearcher;
use object_store::DynObjectStore;
use tempfile::{env::temp_dir, tempdir};
use tokio::sync::{Mutex, RwLock};
use tokio_util::{compat::FuturesAsyncReadCompatExt, io::SyncIoBridge};

use crate::{metadata::Index, NidxMetadata, Settings};

#[derive(Clone)]
pub struct SearchOperation {
    seq: i64,
    segment_ids: Vec<i64>,
    deleted_keys: Vec<String>,
}

pub async fn run() -> anyhow::Result<()> {
    let work_dir = tempdir()?;
    let settings = Settings::from_env();
    let indexer_settings = settings.indexer.expect("Indexer not configured");
    let indexer_storage = indexer_settings.object_store.client();
    let meta = NidxMetadata::new(&settings.metadata.database_url).await?;
    let storage = indexer_settings.object_store.client();

    let index_metadata = Arc::new(RwLock::new(HashMap::new()));

    let sync_task = tokio::task::spawn(run_sync(
        meta,
        work_dir.path().to_path_buf(),
        indexer_storage.clone(),
        index_metadata.clone(),
    ));
    let vector_search_task =
        tokio::task::spawn(run_vector_search(work_dir.path().to_path_buf(), index_metadata.clone()));
    let text_search_task = tokio::task::spawn(run_text_search(work_dir.path().to_path_buf(), index_metadata.clone()));

    tokio::select! {
        r = sync_task => {
            println!("sync_task() completed first {:?}", r)
        }
        r = vector_search_task => {
            println!("vector_search_task() completed {:?}", r)
        }
        r = text_search_task => {
            println!("text_search_task() completed {:?}", r)
        }
    }

    Ok(())
}

async fn run_vector_search(
    work_dir: PathBuf,
    index_metadata: Arc<RwLock<HashMap<i64, Vec<SearchOperation>>>>,
) -> anyhow::Result<()> {
    loop {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let meta_read = index_metadata.read().await;
        let meta: Vec<SearchOperation> = {
            let index_1 = meta_read.get(&1);
            if let Some(index_1) = index_1 {
                index_1.iter().cloned().collect()
            } else {
                println!("No metadata for index 1 yet");
                continue;
            }
        };

        let searcher = VectorSearcher::new(
            &work_dir,
            1,
            meta.iter().flat_map(|m| m.segment_ids.iter().map(|sid| (*sid, m.seq))).collect(),
            meta.iter().flat_map(|m| m.deleted_keys.iter().map(|d| (m.seq, d.clone()))).collect(),
        )?;
        drop(meta_read); // Keep lock until searcher is loaded, to avoid deletions from happening while opening
        println!("Did vector search with {} results", searcher.dummy_search()?);
    }
}

async fn run_text_search(
    work_dir: PathBuf,
    index_metadata: Arc<RwLock<HashMap<i64, Vec<SearchOperation>>>>,
) -> anyhow::Result<()> {
    loop {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let meta_read = index_metadata.read().await;
        let meta: Vec<SearchOperation> = {
            let index_2 = meta_read.get(&2);
            if let Some(index_2) = index_2 {
                index_2.iter().cloned().collect()
            } else {
                println!("No metadata for index 2 yet");
                continue;
            }
        };

        let searcher = TextSearcher::new(
            &work_dir,
            2,
            meta.iter().flat_map(|m| m.segment_ids.iter().map(|sid| (*sid, m.seq))).collect(),
            meta.iter().flat_map(|m| m.deleted_keys.iter().map(|d| (m.seq, d.clone()))).collect(),
        )?;
        drop(meta_read); // Keep lock until searcher is loaded, to avoid deletions from happening while opening
        println!("Did text search with {:?} results", searcher.dummy_search()?);
    }
}

async fn run_sync(
    meta: NidxMetadata,
    work_dir: PathBuf,
    storage: Arc<DynObjectStore>,
    index_metadata: Arc<RwLock<HashMap<i64, Vec<SearchOperation>>>>,
) -> anyhow::Result<()> {
    loop {
        // Get metadata from DB
        // TODO: updated_at on index to avoid always pulling everything
        let indexes = sqlx::query_scalar!("SELECT id FROM indexes").fetch_all(&meta.pool).await?;
        for index_id in indexes {
            let read_index_metadata = index_metadata.read().await;
            let empty = vec![];
            let current_meta = read_index_metadata.get(&index_id).unwrap_or(&empty);
            let current_segs: HashSet<i64> = current_meta.iter().flat_map(|s| s.segment_ids.clone()).collect();
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
                       COALESCE(segment_ids, '{}') AS "segment_ids!",
                       COALESCE(deletions.keys, '{}') AS "deleted_keys!"
                       FROM ready_segments
                       NATURAL FULL OUTER JOIN deletions
                       WHERE index_id = $1
                       ORDER BY seq;"#,
                index_id
            )
            .fetch_all(&meta.pool)
            .await?;
            let new_segs: HashSet<i64> = new_meta.iter().flat_map(|s| s.segment_ids.clone()).collect();

            // Calculate deletions
            let deleted_segments = current_segs.difference(&new_segs);
            let new_segments = new_segs.difference(&current_segs);

            // Download new segments
            for segment_id in new_segments {
                download_segment(storage.clone(), *segment_id, work_dir.join(format!("{index_id}/{segment_id}")))
                    .await?;
            }

            // Switch meta
            index_metadata.write().await.insert(index_id, new_meta);

            // Delete unneeded segments
            for d in deleted_segments {
                std::fs::remove_dir_all(work_dir.join(format!("{index_id}/{d}")))?;
            }
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

pub async fn download_segment(
    storage: Arc<DynObjectStore>,
    segment_id: i64,
    output_dir: PathBuf,
) -> anyhow::Result<()> {
    let path = object_store::path::Path::parse(format!("segment/{}", segment_id)).unwrap();
    let response = storage.get(&path).await?.into_stream();
    let reader = response.map_err(|_| std::io::Error::last_os_error()).into_async_read(); // HACK: Mapping errors randomly
    let reader = SyncIoBridge::new(reader.compat());

    let mut tar = tar::Archive::new(reader);
    tokio::task::spawn_blocking(move || tar.unpack(output_dir).unwrap()).await?;
    println!("Downloaded {segment_id}");

    Ok(())
}
