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
use tokio;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // NATS test
    let client = async_nats::connect("localhost:4222").await?;
    let jetstream = async_nats::jetstream::new(client);

    let consumer: PullConsumer = jetstream.get_consumer_from_stream("nidx", "nidx").await?;
    let mut msg_stream = consumer.messages().await?;
    while let Some(Ok(msg)) = msg_stream.next().await {
        println!("GOT = {:?}", msg);
        msg.ack().await.unwrap();
    }

    Ok(())
}
