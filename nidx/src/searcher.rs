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

pub mod grpc;
mod index_cache;
mod query_language;
mod query_planner;
mod shard_search;
mod shard_suggest;
mod streams;
mod sync;

use index_cache::IndexCache;
use object_store::DynObjectStore;
use prometheus_client::registry::Registry;
use sync::run_sync;
use sync::SyncMetadata;
use tokio::sync::watch;

use std::path::Path;
use std::sync::Arc;

use tempfile::tempdir;

use crate::grpc_server::GrpcServer;
use crate::{NidxMetadata, Settings};

pub use index_cache::IndexSearcher;
pub use sync::SyncStatus;

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

    pub async fn run(&self, storage: Arc<DynObjectStore>, watcher: Option<watch::Sender<SyncStatus>>) {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1000);
        let index_cache_copy = self.index_cache.clone();
        let refresher_task = tokio::task::spawn(async move {
            while let Some(index_id) = rx.recv().await {
                // TODO: Do something smarter
                index_cache_copy.remove(&index_id).await;
            }
        });
        let sync_task =
            tokio::task::spawn(run_sync(self.meta.clone(), storage.clone(), self.sync_metadata.clone(), tx, watcher));
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
        Arc::clone(&self.index_cache)
    }
}

pub async fn run(settings: Settings, metrics: Arc<Registry>) -> anyhow::Result<()> {
    let work_dir = tempdir()?;
    let meta = settings.metadata.clone();
    let storage = settings.storage.as_ref().expect("Storage settings needed").object_store.clone();

    let searcher = SyncedSearcher::new(meta.clone(), work_dir.path());

    let api = grpc::SearchServer::new(meta.clone(), searcher.index_cache());
    let server = GrpcServer::new("0.0.0.0:10001").await?;
    let api_task = tokio::task::spawn(server.serve(api.into_service(), metrics));
    let search_task = tokio::task::spawn(async move { searcher.run(storage, None).await });

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
