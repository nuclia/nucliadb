use pyo3::prelude::*;

use nidx::api::grpc::ApiServer;
use nidx::grpc_server::GrpcServer;
use nidx::indexer::index_resource;
use nidx::searcher::grpc::SearchServer;
use nidx::searcher::SyncedSearcher;
use nidx::settings::EnvSettings;
use nidx::Settings;
use nidx_protos::Resource;
use std::collections::HashMap;
use std::path::Path;
use tokio::runtime::Runtime;

#[pyclass]
pub struct NidxBinding {
    #[pyo3(get)]
    searcher_port: u16,
    #[pyo3(get)]
    api_port: u16,
    settings: Settings,
    seq: i64,
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

        Ok(NidxBinding {
            searcher_port,
            api_port,
            settings,
            seq: 1,
            runtime: None,
        })
    }

    pub async fn index_resource(&mut self, shard_id: &str, resource: Resource) -> anyhow::Result<()> {
        index_resource(
            &self.settings.metadata,
            self.settings.storage.as_ref().unwrap().object_store.clone(),
            shard_id,
            resource,
            self.seq.into(),
        )
        .await?;
        self.seq += 1;
        Ok(())
    }
}

/// A Python module implemented in Rust.
#[pymodule]
fn nidx_binding(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<NidxBinding>()?;
    Ok(())
}
