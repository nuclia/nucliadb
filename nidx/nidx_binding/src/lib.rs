use nidx::maintenance::scheduler::{self, GetAckFloor};
use nidx::maintenance::worker;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;

use nidx::api::grpc::ApiServer;
use nidx::grpc_server::GrpcServer;
use nidx::indexer::{download_message, index_resource};
use nidx::searcher::grpc::SearchServer;
use nidx::searcher::SyncedSearcher;
use nidx::settings::EnvSettings;
use nidx::Settings;
use nidx_protos::prost::*;
use nidx_protos::IndexMessage;
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::AtomicI64;
use std::sync::Arc;
use tokio::runtime::Runtime;

#[derive(Clone)]
struct SeqSource(Arc<AtomicI64>);

impl GetAckFloor for SeqSource {
    async fn get(&mut self) -> anyhow::Result<i64> {
        let seq = self.0.load(std::sync::atomic::Ordering::Relaxed);
        Ok(seq)
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
        // TODO: Can this be simplified into the indexer code?
        let msg = IndexMessage::decode(&bytes[..]).unwrap();

        let seq = self.seq.0.load(std::sync::atomic::Ordering::Relaxed);
        let object_store = self.settings.indexer.as_ref().unwrap().object_store.clone();
        let result = self.runtime.as_ref().unwrap().block_on(async {
            let resource = download_message(object_store, &msg.storage_key).await?;
            index_resource(
                &self.settings.metadata,
                self.settings.storage.as_ref().unwrap().object_store.clone(),
                &msg.shard,
                resource,
                seq.into(),
            )
            .await
        });

        // Always increment seq, even on failure
        self.seq.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        result.map_err(|e| PyException::new_err(format!("Error indexing {e}")))?;

        Ok(seq)
    }
}

impl NidxBinding {
    pub async fn new(binding_settings: HashMap<String, String>) -> anyhow::Result<Self> {
        let settings = Settings::from_env_settings(EnvSettings::from_map(binding_settings)).await?;

        // API server
        let api_service = ApiServer::new(settings.metadata.clone()).into_service();
        let api_server = GrpcServer::new("localhost:0").await?;
        let api_port = api_server.port()?;
        tokio::task::spawn(api_server.serve(api_service));

        // Searcher API
        let searcher = SyncedSearcher::new(settings.metadata.clone(), Path::new("/tmp/searcher"));
        let searcher_api = SearchServer::new(settings.metadata.clone(), searcher.index_cache());
        let searcher_server = GrpcServer::new("localhost:0").await?;
        let searcher_port = searcher_server.port()?;
        tokio::task::spawn(searcher_server.serve(searcher_api.into_service()));
        let settings_copy = settings.clone();
        tokio::task::spawn(
            async move { searcher.run(settings_copy.storage.as_ref().unwrap().object_store.clone()).await },
        );

        // Scheduler
        let seq = SeqSource(Arc::new(settings.metadata.max_seq().await?.into()));
        tokio::task::spawn(scheduler::run_tasks(
            settings.metadata.clone(),
            settings.storage.as_ref().unwrap().object_store.clone(),
            settings.merge.clone(),
            seq.clone(),
        ));

        // Worker
        let settings_copy = settings.clone();
        tokio::task::spawn(worker::run(settings_copy));

        Ok(NidxBinding {
            searcher_port,
            api_port,
            settings,
            seq,
            runtime: None,
        })
    }
}

/// A Python module implemented in Rust.
#[pymodule]
fn nidx_binding(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<NidxBinding>()?;
    Ok(())
}
