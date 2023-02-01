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

use nucliadb_core::protos::{EmptyQuery, ShardId};

mod common;

use common::node_writer;
use tonic::Request;

#[tokio::test]
async fn test_create_shard() -> Result<(), Box<dyn std::error::Error>> {
    let mut writer = node_writer().await;

    let new_shard_response = writer.new_shard(Request::new(EmptyQuery {})).await?;
    let shard_id = &new_shard_response.get_ref().id;

    let response = writer
        .get_shard(Request::new(ShardId {
            id: shard_id.clone(),
        }))
        .await?;

    assert_eq!(shard_id, &response.get_ref().id);

    Ok(())
}
