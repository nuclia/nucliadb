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
use nidx::scheduler::{self, GetAckFloor};
use nidx::searcher::shard_selector::ShardSelector;
use nidx::worker;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;

use nidx::Settings;
use nidx::api::grpc::ApiServer;
use nidx::grpc_server::GrpcServer;
use nidx::indexer::process_index_message;
use nidx::searcher::grpc::SearchServer;
use nidx::searcher::{SyncStatus, SyncedSearcher};
use nidx::settings::EnvSettings;
use nidx_protos::IndexMessage;
use nidx_protos::prost::*;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicI64;
use tempfile::{TempDir, tempdir};
use tokio::runtime::Runtime;
use tokio::sync::mpsc::{Sender, channel};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
struct SeqSource(Arc<AtomicI64>);

impl GetAckFloor for SeqSource {
    async fn get(&mut self) -> anyhow::Result<i64> {
        let seq = self.0.load(std::sync::atomic::Ordering::Relaxed);
        Ok(seq)
    }

    async fn cleanup(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

#[pyclass]
pub struct NidxBinding {
    #[pyo3(get)]
    searcher_port: u16,
    #[pyo3(get)]
    api_port: u16,
    settings: Settings,
    seq: SeqSource,
    runtime: Option<Runtime>,
    sync_watcher: watch::Receiver<SyncStatus>,
    shutdown: CancellationToken,
    request_sync: Sender<()>,
    _searcher_work_dir: TempDir,
}

#[pymethods]
impl NidxBinding {
    #[new]
    pub fn start(mut settings: HashMap<String, String>) -> NidxBinding {
        settings.insert("INDEXER__NATS_SERVER".to_string(), "".to_string());

        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut binding = rt.block_on(NidxBinding::new(settings)).unwrap();
        binding.runtime = Some(rt);
        binding
    }

    pub fn index(&mut self, bytes: Vec<u8>) -> PyResult<i64> {
        let msg = IndexMessage::decode(&bytes[..]).unwrap();

        let seq = self.seq.0.load(std::sync::atomic::Ordering::Relaxed);
        let object_store = self.settings.indexer.as_ref().unwrap().object_store.clone();
        let result = self.runtime.as_ref().unwrap().block_on(async {
            process_index_message(
                &self.settings.metadata,
                object_store,
                self.settings.storage.as_ref().unwrap().object_store.clone(),
                &tempfile::env::temp_dir(),
                msg,
                seq.into(),
            )
            .await
        });

        // Always increment seq, even on failure
        self.seq.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        result.map_err(|e| PyException::new_err(format!("Error indexing {e}")))?;
        let _ = self.request_sync.try_send(());

        Ok(seq)
    }

    /// Wait for the searcher to be synced. Used in nucliadb tests
    pub fn wait_for_sync(&mut self) {
        self.runtime.as_ref().unwrap().block_on(async {
            // Wait for a new sync to start
            self.sync_watcher
                .wait_for(|s| matches!(s, SyncStatus::Syncing))
                .await
                .unwrap();
            // Wait for it to finish
            self.sync_watcher
                .wait_for(|s| matches!(s, SyncStatus::Synced))
                .await
                .unwrap();
        });
    }
}

impl Drop for NidxBinding {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

impl NidxBinding {
    pub async fn new(binding_settings: HashMap<String, String>) -> anyhow::Result<Self> {
        let settings = Settings::from_env_settings(EnvSettings::from_map(binding_settings)).await?;
        let shutdown = CancellationToken::new();

        // We are setting the global subscriber, so it will only take the settings (RUST_LOG) of the first initialization
        let _ = tracing_subscriber::fmt::try_init();

        // API server
        let api_service = ApiServer::new(&settings).into_router();
        let api_server = GrpcServer::new("localhost:0").await?;
        let api_port = api_server.port()?;
        tokio::task::spawn(api_server.serve(api_service, shutdown.clone()));

        // Searcher API
        let (request_sync, sync_requested) = channel(2);
        let searcher_work_dir = tempdir()?;
        let (sync_reporter, sync_watcher) = watch::channel(SyncStatus::Syncing);
        let searcher = SyncedSearcher::new(settings.metadata.clone(), searcher_work_dir.path());
        let searcher_api = SearchServer::new(searcher.index_cache(), ShardSelector::new_single());
        let searcher_server = GrpcServer::new("localhost:0").await?;
        let searcher_port = searcher_server.port()?;
        tokio::task::spawn(searcher_server.serve(searcher_api.into_router(), shutdown.clone()));
        let settings_copy = settings.clone();
        let shutdown2 = shutdown.clone();
        tokio::task::spawn(async move {
            searcher
                .run(
                    settings_copy.storage.as_ref().unwrap().object_store.clone(),
                    settings_copy.searcher.clone().unwrap_or_default(),
                    shutdown2,
                    ShardSelector::new_single(),
                    Some(sync_reporter),
                    Some(sync_requested),
                )
                .await
        });

        // Scheduler
        let seq = SeqSource(Arc::new(AtomicI64::new(settings.metadata.max_seq().await? + 1i64)));
        tokio::task::spawn(scheduler::run_tasks(
            settings.metadata.clone(),
            settings.storage.as_ref().unwrap().object_store.clone(),
            settings.clone(),
            seq.clone(),
        ));

        // Worker
        let settings_copy = settings.clone();
        tokio::task::spawn(worker::run(settings_copy, shutdown.clone()));

        Ok(NidxBinding {
            searcher_port,
            api_port,
            settings,
            seq,
            runtime: None,
            sync_watcher,
            shutdown,
            request_sync,
            _searcher_work_dir: searcher_work_dir,
        })
    }
}

/// A Python module implemented in Rust.
#[pymodule]
fn nidx_binding(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<NidxBinding>()?;
    Ok(())
}
