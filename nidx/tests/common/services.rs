// Copyright 2021 Bosutech XXI S.L.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

use std::sync::Arc;
use std::time::Duration;

use nidx::api::grpc::ApiServer;
use nidx::grpc_server::GrpcServer;
use nidx::indexer::{IndexerServer, delete_resource, index_resource};
use nidx::searcher::SyncedSearcher;
use nidx::searcher::grpc::SearchServer;
use nidx::searcher::shard_selector::ShardSelector;
use nidx::settings::{EnvSettings, LpgSettings, MetadataSettings, SearcherSettings, StorageSettings};
use nidx::{NidxMetadata, Settings};
use nidx_protos::Resource;
use nidx_protos::nidx::nidx_api_client::NidxApiClient;
use nidx_protos::nidx::nidx_lpg_client::NidxLpgClient;
use nidx_protos::nidx::nidx_searcher_client::NidxSearcherClient;
use object_store::memory::InMemory;
use sqlx::PgPool;
use tempfile::tempdir;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;

pub struct NidxFixture {
    pub searcher_client: NidxSearcherClient<Channel>,
    pub api_client: NidxApiClient<Channel>,
    pub lpg_client: NidxLpgClient<Channel>,
    pub settings: Settings,
    _work_dir: tempfile::TempDir,
    seq: i64,
}

impl NidxFixture {
    pub async fn new(pool: PgPool) -> anyhow::Result<Self> {
        let shutdown = CancellationToken::new();
        let mut settings = Settings {
            metadata: NidxMetadata::new_with_pool(pool).await?,
            settings: EnvSettings {
                lpg: None,
                indexer: Some(nidx::settings::IndexerSettings {
                    object_store: Arc::new(InMemory::new()),
                    nats_server: None,
                }),
                storage: Some(StorageSettings {
                    object_store: Arc::new(InMemory::new()),
                }),
                merge: Default::default(),
                metadata: Some(MetadataSettings {
                    database_url: "ignored".to_string(),
                    disable_migrations: false,
                    idle_timeout_seconds: None,
                    max_lifetime_seconds: None,
                }),
                telemetry: Default::default(),
                work_path: None,
                control_socket: None,
                searcher: Some(SearcherSettings::default()),
                audit: None,
            },
        };
        // Searcher API
        let work_dir = tempdir()?;
        let searcher = SyncedSearcher::new(settings.metadata.clone(), work_dir.path());
        let searcher_api = SearchServer::new(searcher.index_cache(), ShardSelector::new_single());
        let searcher_server = GrpcServer::new("localhost:0").await?;
        let searcher_port = searcher_server.port()?;
        tokio::task::spawn(searcher_server.serve(searcher_api.into_router(), shutdown.clone()));
        let settings_copy = settings.clone();
        let searcher_shutdown = shutdown.clone();
        tokio::task::spawn(async move {
            searcher
                .run(
                    settings_copy.storage.as_ref().unwrap().object_store.clone(),
                    settings_copy.searcher.clone().unwrap_or_default(),
                    searcher_shutdown,
                    ShardSelector::new_single(),
                    None,
                    None,
                )
                .await
        });

        let indexer_server = GrpcServer::new("localhost:0").await?;
        let indexer_port = indexer_server.port()?;
        let indexer = IndexerServer::new(settings.clone())?.into_service();
        tokio::task::spawn(indexer_server.serve(indexer, shutdown.clone()));
        settings.settings.lpg = Some(LpgSettings {
            indexer_address: format!("http://localhost:{indexer_port}"),
            searcher_address: format!("http://localhost:{searcher_port}"),
        });
        let api_service = ApiServer::new(&settings).into_router();
        let api_server = GrpcServer::new("localhost:0").await?;
        let api_port = api_server.port()?;
        tokio::task::spawn(api_server.serve(api_service, shutdown.clone()));

        // Clients
        let searcher_client = NidxSearcherClient::connect(format!("http://localhost:{searcher_port}")).await?;
        let api_client = NidxApiClient::connect(format!("http://localhost:{api_port}")).await?;
        let lpg_client = NidxLpgClient::connect(format!("http://localhost:{api_port}")).await?;

        Ok(NidxFixture {
            searcher_client,
            api_client,
            lpg_client,
            settings,
            _work_dir: work_dir,
            seq: 1,
        })
    }

    pub async fn index_resource(&mut self, shard_id: &str, resource: Resource) -> anyhow::Result<()> {
        index_resource(
            &self.settings.metadata,
            self.settings.storage.as_ref().unwrap().object_store.clone(),
            &tempfile::env::temp_dir(),
            shard_id,
            resource,
            self.seq.into(),
        )
        .await?;
        self.seq += 1;
        Ok(())
    }

    pub async fn delete_resource(&mut self, shard_id: &str, resource_uuid: &str) -> anyhow::Result<()> {
        delete_resource(
            &self.settings.metadata,
            shard_id,
            resource_uuid.to_string(),
            self.seq.into(),
        )
        .await?;
        self.seq += 1;
        Ok(())
    }

    pub async fn wait_sync(&self) {
        // TODO: Check the searcher has synced? For now, waiting twice the sync interval
        tokio::time::sleep(Duration::from_secs_f32(
            2.0 * self
                .settings
                .searcher
                .as_ref()
                .map_or(1.0, |s| s.metadata_refresh_interval),
        ))
        .await;
    }
}
