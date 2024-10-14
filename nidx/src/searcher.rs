use core::sync;
use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use futures::TryStreamExt;
use nidx_vector::VectorSearcher;
use object_store::DynObjectStore;
use tempfile::{env::temp_dir, tempdir};
use tokio::sync::{Mutex, RwLock};
use tokio_util::{compat::FuturesAsyncReadCompatExt, io::SyncIoBridge};

use crate::{metadata::Index, NidxMetadata, Settings};

#[derive(Clone)]
pub struct SearchOperation {
    seq: i64,
    segment_id: Option<i64>,
    deleted_keys: Option<Vec<String>>,
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
    let search_task = tokio::task::spawn(run_search(work_dir.path().to_path_buf(), index_metadata.clone()));

    tokio::select! {
        r = sync_task => {
            println!("sync_task() completed first {:?}", r)
        }
        r = search_task => {
            println!("search_task() completed {:?}", r)
        }
    }

    Ok(())
}

async fn run_search(
    work_dir: PathBuf,
    index_metadata: Arc<RwLock<HashMap<i64, Vec<SearchOperation>>>>,
) -> anyhow::Result<()> {
    loop {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let meta: Vec<SearchOperation> = {
            let meta_read = index_metadata.read().await;
            let index_1 = meta_read.get(&1);
            if let Some(index_1) = index_1 {
                index_1.iter().cloned().collect()
            } else {
                println!("No metadata for index 1 yet");
                continue;
            }
        };

        let empty = Vec::new();
        let searcher = VectorSearcher::new(
            &work_dir,
            1,
            meta.iter().filter_map(|m| m.segment_id.map(|sid| (sid, m.seq))).collect(),
            meta.iter()
                .flat_map(|m| m.deleted_keys.as_ref().unwrap_or(&empty).iter().map(|d| (m.seq, d.clone())))
                .collect(),
        )?;
        println!("Did search with {} results", searcher.dummy_search()?);
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
            let current_seqs: HashSet<_> = current_meta.iter().map(|s| s.seq).collect();
            let new_meta = sqlx::query_as!(SearchOperation, r#"SELECT COALESCE(segments.seq, deletions.seq) AS "seq!", segments.id AS "segment_id?", deletions.keys AS "deleted_keys?" FROM segments NATURAL FULL JOIN deletions WHERE segments.ready = true ORDER BY 1"#).fetch_all(&meta.pool).await?;
            let new_seqs: HashSet<_> = new_meta.iter().map(|s| s.seq).collect();

            // Calculate deletions
            let deleted_segments = current_meta
                .iter()
                .filter_map(|m| {
                    if new_seqs.contains(&m.seq) {
                        None
                    } else {
                        m.segment_id
                    }
                })
                .collect::<Vec<_>>();

            let new_segments = new_meta
                .iter()
                .filter_map(|m| {
                    if current_seqs.contains(&m.seq) {
                        None
                    } else {
                        m.segment_id
                    }
                })
                .collect::<Vec<_>>();
            drop(read_index_metadata);

            // New segments
            for segment_id in new_segments {
                download_segment(storage.clone(), segment_id, work_dir.join(format!("{index_id}/{segment_id}")))
                    .await?;
            }

            // Switch meta
            index_metadata.write().await.insert(index_id, new_meta);

            // Delete unneeded segments
            for d in deleted_segments {
                std::fs::remove_dir_all(work_dir.join(format!("{index_id}/{d}")))?;
            }
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
    tokio::task::spawn_blocking(move || tar.unpack(output_dir).unwrap()).await?;
    println!("Downloaded {segment_id}");

    Ok(())
}
