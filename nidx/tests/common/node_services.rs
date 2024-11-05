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

use std::time::Duration;

use nidx::api::grpc::ApiServer;
use nidx::grpc_server::GrpcServer;
use nidx::settings::{ObjectStoreConfig, StorageSettings};
use nidx::{searcher, NidxMetadata, Settings};
use nidx_protos::node_reader_client::NodeReaderClient;
use nidx_protos::node_writer_client::NodeWriterClient;
use sqlx::PgPool;
use tonic::transport::Channel;

pub struct NidxFixture {
    pub searcher_client: NodeReaderClient<Channel>,
    pub api_client: NodeWriterClient<Channel>,
    searcher_task: Option<tokio::task::JoinHandle<anyhow::Result<()>>>,
    api_task: Option<tokio::task::JoinHandle<anyhow::Result<()>>>,
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
        let searcher_task = Some(tokio::task::spawn(searcher::run(settings.clone())));

        let api_service = ApiServer::new(settings.metadata.clone()).into_service();
        let api_server = GrpcServer::new("localhost:0").await?;
        let port = api_server.port()?;
        let api_task = Some(tokio::task::spawn(api_server.serve(api_service)));

        tokio::time::sleep(Duration::from_millis(100)).await;

        let searcher_client = NodeReaderClient::connect("http://localhost:10001").await?;
        let api_client = NodeWriterClient::connect(format!("http://localhost:{port}")).await?;

        Ok(NidxFixture {
            searcher_client,
            api_client,
            searcher_task,
            api_task,
        })
    }
}
