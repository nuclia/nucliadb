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
use sync::run_sync;
use sync::SyncMetadata;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::*;

use std::path::Path;
use std::path::PathBuf;
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

    pub async fn run(
        &self,
        storage: Arc<DynObjectStore>,
        shutdown: CancellationToken,
        watcher: Option<watch::Sender<SyncStatus>>,
    ) -> anyhow::Result<()> {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1000);
        let index_cache_copy = self.index_cache.clone();

        let mut tasks = JoinSet::new();
        let refresher_task = tasks
            .spawn(async move {
                while let Some(index_id) = rx.recv().await {
                    // TODO: Do something smarter
                    index_cache_copy.remove(&index_id).await;
                }

                Ok(())
            })
            .id();

        let sync_task = tasks.spawn(run_sync(
            self.meta.clone(),
            storage.clone(),
            self.sync_metadata.clone(),
            shutdown.clone(),
            tx,
            watcher,
        ));
        let sync_task_id = sync_task.id();

        while let Some(join_result) = tasks.join_next_with_id().await {
            let (id, result) = join_result.unwrap();
            let task_name = if id == refresher_task {
                "refresher"
            } else if id == sync_task_id {
                "sync"
            } else {
                unreachable!()
            };
            if let Err(e) = result {
                error!("Task {task_name} exited with error {e:?}");
                return Err(e);
            } else {
                info!("Task {task_name} exited without error");
                if !shutdown.is_cancelled() {
                    panic!("Unexpected task termination without graceful shutdown");
                }
            }
        }

        Ok(())
    }

    pub fn index_cache(&self) -> Arc<IndexCache> {
        Arc::clone(&self.index_cache)
    }
}

pub async fn run(settings: Settings, shutdown: CancellationToken) -> anyhow::Result<()> {
    let work_dir = tempdir()?;
    let work_path = match &settings.work_path {
        Some(work_path) => &PathBuf::from(&work_path.clone()),
        None => work_dir.path(),
    };
    let meta = settings.metadata.clone();
    let storage = settings.storage.as_ref().expect("Storage settings needed").object_store.clone();

    let searcher = SyncedSearcher::new(meta.clone(), work_path);

    let api = grpc::SearchServer::new(meta.clone(), searcher.index_cache());
    let server = GrpcServer::new("0.0.0.0:10001").await?;

    let mut tasks = JoinSet::new();
    let api_task = tasks.spawn(server.serve(api.into_service(), shutdown.clone())).id();
    let shutdown2 = shutdown.clone();
    let search_task = tasks.spawn(async move { searcher.run(storage, shutdown2, None).await }).id();

    while let Some(join_result) = tasks.join_next_with_id().await {
        let (id, result) = join_result.unwrap();
        let task_name = if id == api_task {
            "searcher_api"
        } else if id == search_task {
            "synced_searcher"
        } else {
            unreachable!()
        };
        if let Err(e) = result {
            error!("Task {task_name} exited with error {e:?}");
            return Err(e);
        } else {
            info!("Task {task_name} exited without error");
            if !shutdown.is_cancelled() {
                panic!("Unexpected task termination without graceful shutdown");
            }
        }
    }

    Ok(())
}
