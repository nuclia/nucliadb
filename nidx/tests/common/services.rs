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

use std::sync::Arc;
use std::time::Duration;

use nidx::api::grpc::ApiServer;
use nidx::grpc_server::GrpcServer;
use nidx::indexer::index_resource;
use nidx::searcher::SyncedSearcher;
use nidx::searcher::grpc::SearchServer;
use nidx::searcher::shard_selector::ShardSelector;
use nidx::settings::{EnvSettings, MetadataSettings, StorageSettings};
use nidx::{NidxMetadata, Settings};
use nidx_protos::Resource;
use nidx_protos::nidx::nidx_api_client::NidxApiClient;
use nidx_protos::nidx::nidx_searcher_client::NidxSearcherClient;
use object_store::memory::InMemory;
use sqlx::PgPool;
use tempfile::tempdir;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;

pub struct NidxFixture {
    pub searcher_client: NidxSearcherClient<Channel>,
    pub api_client: NidxApiClient<Channel>,
    pub settings: Settings,
    seq: i64,
}

impl NidxFixture {
    pub async fn new(pool: PgPool) -> anyhow::Result<Self> {
        let shutdown = CancellationToken::new();
        let settings = Settings {
            metadata: NidxMetadata::new_with_pool(pool).await?,
            settings: EnvSettings {
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
                }),
                telemetry: Default::default(),
                work_path: None,
                control_socket: None,
                searcher: Default::default(),
                audit: None,
            },
        };
        // API server
        let api_service = ApiServer::new(&settings).into_router();
        let api_server = GrpcServer::new("localhost:0").await?;
        let api_port = api_server.port()?;
        tokio::task::spawn(api_server.serve(api_service, shutdown.clone()));

        // Searcher API
        let work_dir = tempdir()?;
        let searcher = SyncedSearcher::new(settings.metadata.clone(), work_dir.path());
        let searcher_api = SearchServer::new(searcher.index_cache(), ShardSelector::new_single());
        let searcher_server = GrpcServer::new("localhost:0").await?;
        let searcher_port = searcher_server.port()?;
        tokio::task::spawn(searcher_server.serve(searcher_api.into_router(), shutdown.clone()));
        let settings_copy = settings.clone();
        tokio::task::spawn(async move {
            searcher
                .run(
                    settings_copy.storage.as_ref().unwrap().object_store.clone(),
                    settings_copy.searcher.clone().unwrap_or_default(),
                    shutdown.clone(),
                    ShardSelector::new_single(),
                    None,
                    None,
                )
                .await
        });

        // Clients
        let searcher_client = NidxSearcherClient::connect(format!("http://localhost:{searcher_port}")).await?;
        let api_client = NidxApiClient::connect(format!("http://localhost:{api_port}")).await?;

        Ok(NidxFixture {
            searcher_client,
            api_client,
            settings,
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

    pub async fn wait_sync(&self) {
        // TODO: Check the searcher has synced? For now, waiting twice the sync interval
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}
