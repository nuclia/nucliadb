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

use async_nats::jetstream::consumer::PullConsumer;
use futures::stream::StreamExt;
use nidx::{index_resource, NidxMetadata, Settings};
use nucliadb_core::protos::{prost::Message, Resource};
use tokio;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let settings = Settings::from_env();
    let indexer_settings = settings.indexer.expect("Indexer not configured");
    let indexer_storage = indexer_settings.object_store.client();
    let meta = NidxMetadata::new(&settings.metadata.database_url).await?;

    let client = async_nats::connect(indexer_settings.nats_server).await?;
    let jetstream = async_nats::jetstream::new(client);
    let consumer: PullConsumer = jetstream.get_consumer_from_stream("nidx", "nidx").await?;
    let mut msg_stream = consumer.messages().await?;

    while let Some(Ok(msg)) = msg_stream.next().await {
        let body = msg.message.payload.clone();

        let get_result =
            indexer_storage.get(&object_store::path::Path::from(String::from_utf8(body.to_vec()).unwrap())).await?;
        let bytes = get_result.bytes().await?;
        let resource = Resource::decode(bytes)?;

        index_resource(&meta, indexer_storage.clone(), &resource).await?;
        msg.ack().await.unwrap();
    }

    Ok(())
}
