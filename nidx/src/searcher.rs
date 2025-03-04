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
pub mod shard_selector;
mod shard_suggest;
mod streams;
mod sync;

use index_cache::IndexCache;
use object_store::DynObjectStore;
use shard_selector::KubernetesCluster;
pub use shard_selector::ListNodes;
use shard_selector::ShardSelector;
use shard_selector::SingleNodeCluster;
use sync::SyncMetadata;
use sync::run_sync;
use tokio::sync::mpsc::Receiver;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::*;

use std::collections::HashSet;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use tempfile::tempdir;

use crate::grpc_server::GrpcServer;
use crate::metadata::IndexId;
use crate::metrics::searcher::REFRESH_QUEUE_LEN;
use crate::settings::SearcherSettings;
use crate::{NidxMetadata, Settings};

pub use index_cache::IndexSearcher;
pub use sync::SyncStatus;

pub struct SyncedSearcher {
    index_cache: Arc<IndexCache>,
    sync_metadata: Arc<SyncMetadata>,
    meta: NidxMetadata,
}

async fn refresher_task(mut rx: Receiver<IndexId>, index_cache: Arc<IndexCache>) -> anyhow::Result<()> {
    let mut try_later = Vec::new();
    loop {
        // Read all available messages to a set in order to deduplicated requests for the same index
        let mut recv_buf = Vec::new();
        if rx.recv_many(&mut recv_buf, 100).await == 0 {
            // Channel closed
            return Ok(());
        }

        let unique_indexes: HashSet<IndexId> =
            HashSet::from_iter(try_later.drain(std::ops::RangeFull).chain(recv_buf.into_iter()));
        for index_id in unique_indexes {
            match index_cache.reload(&index_id).await {
                Ok(true) => {
                    // Index is currently loading, no need to reload now, nut will enqueue a reload for later
                    debug!(?index_id, "Index being loaded by cache, will reload it later");
                    try_later.push(index_id);
                }
                Ok(false) => {
                    debug!(?index_id, "Index reloaded");
                }
                Err(e) => {
                    error!(?index_id, "Index failed to reload, might become out of date: {e:?}");
                }
            }
        }
        let pending_refreshes = rx.max_capacity() - rx.capacity();
        REFRESH_QUEUE_LEN.set(pending_refreshes as i64);
    }
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
        settings: SearcherSettings,
        shutdown: CancellationToken,
        shard_selector: ShardSelector,
        watcher: Option<watch::Sender<SyncStatus>>,
        request_sync: Option<Receiver<()>>,
    ) -> anyhow::Result<()> {
        let (tx, rx) = tokio::sync::mpsc::channel(1000);

        let mut tasks = JoinSet::new();
        let refresher_task = tasks.spawn(refresher_task(rx, self.index_cache.clone())).id();

        let sync_task = tasks.spawn(run_sync(
            self.meta.clone(),
            storage.clone(),
            self.sync_metadata.clone(),
            settings,
            shutdown.clone(),
            tx,
            watcher,
            request_sync,
            shard_selector,
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
    let searcher_settings = settings.searcher.clone().unwrap_or_default();
    let work_dir = tempdir()?;
    let work_path = match &settings.work_path {
        Some(work_path) => &PathBuf::from(&work_path.clone()),
        None => work_dir.path(),
    };
    let meta = settings.metadata.clone();
    let storage = settings
        .storage
        .as_ref()
        .expect("Storage settings needed")
        .object_store
        .clone();

    let mut tasks = JoinSet::new();
    let (list_nodes, shard_selector_task_id): (Arc<dyn ListNodes>, _) =
        match searcher_settings.shard_partitioning.method {
            crate::settings::ShardPartitioningMethod::Single => (Arc::new(SingleNodeCluster), None),
            crate::settings::ShardPartitioningMethod::Kubernetes => {
                let (node_lister, task) = KubernetesCluster::new_cluster_and_task(shutdown.clone()).await?;
                let task_id = tasks.spawn(task).id();
                // Now that the reader task is started, we can wait until we are ready
                info!("Initializing kubernetes API");
                node_lister.wait_until_ready().await?;
                let available_nodes = node_lister.list_nodes();
                info!(?available_nodes, "Kubernetes API initialized");
                (Arc::new(node_lister), Some(task_id))
            }
        };

    let searcher = SyncedSearcher::new(meta.clone(), work_path);

    let shard_selector = ShardSelector::new(list_nodes.clone(), searcher_settings.shard_partitioning.replicas);
    let api = grpc::SearchServer::new(searcher.index_cache(), shard_selector);
    let server = GrpcServer::new("0.0.0.0:10001").await?;

    let api_task = tasks.spawn(server.serve(api.into_router(), shutdown.clone())).id();
    let shutdown2 = shutdown.clone();
    let shard_selector = ShardSelector::new(list_nodes, searcher_settings.shard_partitioning.replicas);
    let search_task = tasks
        .spawn(async move {
            searcher
                .run(storage, searcher_settings, shutdown2, shard_selector, None, None)
                .await
        })
        .id();

    while let Some(join_result) = tasks.join_next_with_id().await {
        let (id, result) = join_result.unwrap();
        let task_name = if id == api_task {
            "searcher_api"
        } else if id == search_task {
            "synced_searcher"
        } else if Some(id) == shard_selector_task_id {
            "shard_selector"
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
