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

use futures::executor::block_on;
use nidx::settings::{ObjectStoreConfig, StorageSettings};
use nidx::{api, searcher, Settings};
use nidx_protos::node_reader_client::NodeReaderClient;
use nidx_protos::node_writer_client::NodeWriterClient;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use tonic::transport::Channel;

pub struct NidxFixture {
    pub searcher_client: NodeReaderClient<Channel>,
    pub api_client: NodeWriterClient<Channel>,
    searcher_task: Option<tokio::task::JoinHandle<anyhow::Result<()>>>,
    api_task: Option<tokio::task::JoinHandle<anyhow::Result<()>>>,
}

impl NidxFixture {
    pub async fn new(db_options: PgConnectOptions) -> anyhow::Result<Self> {
        let settings = Settings {
            metadata: nidx::settings::MetadataSettings {
                database_url: db_options,
            },
            indexer: Some(nidx::settings::IndexerSettings {
                object_store: ObjectStoreConfig::Memory,
                nats_server: String::new(),
            }),
            storage: Some(StorageSettings {
                object_store: ObjectStoreConfig::Memory,
            }),
            merge: Default::default(),
        };
        let searcher_task = Some(tokio::task::spawn(searcher::run(settings.clone())));
        let api_task = Some(tokio::task::spawn(api::run(settings.clone())));

        tokio::time::sleep(Duration::from_millis(100)).await;

        let searcher_client = NodeReaderClient::connect("http://localhost:10001").await?;
        let api_client = NodeWriterClient::connect("http://localhost:10000").await?;

        Ok(NidxFixture {
            searcher_client,
            api_client,
            searcher_task,
            api_task,
        })
    }
}
