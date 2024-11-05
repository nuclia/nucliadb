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

use nidx::api::grpc::ApiServer;
use nidx::grpc_server::GrpcServer;
use nidx::searcher::grpc::SearchServer;
use nidx::searcher::SyncedSearcher;
use nidx::settings::{ObjectStoreConfig, StorageSettings};
use nidx::{NidxMetadata, Settings};
use nidx_protos::node_reader_client::NodeReaderClient;
use nidx_protos::node_writer_client::NodeWriterClient;
use sqlx::PgPool;
use tempfile::tempdir;
use tonic::transport::Channel;

pub struct NidxFixture {
    pub searcher_client: NodeReaderClient<Channel>,
    pub api_client: NodeWriterClient<Channel>,
}

impl NidxFixture {
    pub async fn new(pool: PgPool) -> anyhow::Result<Self> {
        let settings = Settings {
            metadata: NidxMetadata::new_with_pool(pool).await?,
            indexer: Some(nidx::settings::IndexerSettings {
                object_store: ObjectStoreConfig::Memory.client(),
                nats_server: String::new(),
            }),
            storage: Some(StorageSettings {
                object_store: ObjectStoreConfig::Memory.client(),
            }),
            merge: Default::default(),
        };
        // API server
        let api_service = ApiServer::new(settings.metadata.clone()).into_service();
        let api_server = GrpcServer::new("localhost:0").await?;
        let api_port = api_server.port()?;
        tokio::task::spawn(api_server.serve(api_service));

        // Searcher API
        let work_dir = tempdir()?;
        let searcher = SyncedSearcher::new(settings.metadata.clone(), work_dir.path());
        let searcher_api = SearchServer::new(settings.metadata.clone(), searcher.index_cache());
        let searcher_server = GrpcServer::new("localhost:0").await?;
        let searcher_port = searcher_server.port()?;
        tokio::task::spawn(searcher_server.serve(searcher_api.into_service()));
        tokio::task::spawn(async move { searcher.run(settings.storage.unwrap().object_store).await });

        // Clients
        let searcher_client = NodeReaderClient::connect(format!("http://localhost:{searcher_port}")).await?;
        let api_client = NodeWriterClient::connect(format!("http://localhost:{api_port}")).await?;

        Ok(NidxFixture {
            searcher_client,
            api_client,
        })
    }
}
