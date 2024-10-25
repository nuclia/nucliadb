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

mod grpc;
mod index_cache;
mod shard_search;
mod sync;

use index_cache::IndexCache;
use object_store::DynObjectStore;
use sync::run_sync;
use sync::SyncMetadata;

use std::path::Path;
use std::sync::Arc;

use tempfile::tempdir;

use crate::{NidxMetadata, Settings};

pub use index_cache::IndexSearcher;

pub struct SyncedSearcher {
    index_cache: Arc<IndexCache>,
    sync_metadata: Arc<SyncMetadata>,
    meta: NidxMetadata,
}

impl SyncedSearcher {
    pub fn new(meta: NidxMetadata, work_dir: &Path) -> Self {
        let sync_metadata = Arc::new(SyncMetadata::new(work_dir.to_path_buf()));
        let index_cache = Arc::new(IndexCache::new(sync_metadata.clone(), meta.clone()));

        Self {
            meta,
            index_cache,
            sync_metadata,
        }
    }

    pub async fn run(&self, storage: Arc<DynObjectStore>) {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1000);
        let index_cache_copy = self.index_cache.clone();
        let refresher_task = tokio::task::spawn(async move {
            while let Some(index_id) = rx.recv().await {
                // TODO: Do something smarter
                index_cache_copy.remove(&index_id).await;
            }
        });
        let sync_task =
            tokio::task::spawn(run_sync(self.meta.clone(), storage.clone(), self.sync_metadata.clone(), tx));
        tokio::select! {
            r = sync_task => {
                println!("sync_task() completed first {:?}", r)
            }
            r = refresher_task => {
                println!("refresher_task() completed first {:?}", r)
            }
        }
    }

    pub fn index_cache(&self) -> Arc<IndexCache> {
        self.index_cache.clone()
    }
}

pub async fn run() -> anyhow::Result<()> {
    let work_dir = tempdir()?;
    let settings = Settings::from_env();
    let meta = NidxMetadata::new(&settings.metadata.database_url).await?;
    let storage = settings.storage.expect("Storage settings needed").object_store.client();

    let searcher = SyncedSearcher::new(meta.clone(), work_dir.path());

    let api = grpc::SearchServer::new(meta.clone(), searcher.index_cache());
    let api_task = tokio::task::spawn(api.serve());
    let search_task = tokio::task::spawn(async move { searcher.run(storage).await });

    tokio::select! {
        r = search_task => {
            println!("sync_task() completed first {:?}", r)
        }
        r = api_task => {
            println!("api_task() completed first {:?}", r)
        }
    }

    Ok(())
}
