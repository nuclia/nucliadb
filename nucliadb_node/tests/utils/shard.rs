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
use nucliadb_protos::node_writer_client::NodeWriterClient;
use nucliadb_protos::EmptyQuery;
use tonic::Request;

#[tokio::test]
pub async fn create_shard() {
    let mut client = NodeWriterClient::connect("http://127.0.0.1:4446")
        .await
        .expect("Error creating NodeWriter client");

    let response = client
        .new_shard(Request::new(EmptyQuery {}))
        .await
        .expect("Error in new_shard request");

    println!("response id {}", response.get_ref().id)
}
