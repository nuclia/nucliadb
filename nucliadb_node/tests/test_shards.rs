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

use nucliadb_core::protos::{GetShardRequest, NewShardRequest, ShardId};

mod common;

use common::{node_reader, node_writer};
use tonic::Request;

#[tokio::test]
async fn test_create_shard() -> Result<(), Box<dyn std::error::Error>> {
    let mut writer = node_writer().await;
    const KB0: &str = "KB0";
    const KB1: &str = "KB1";
    const KB2: &str = "KB2";
    let new_shard_response = writer
        .new_shard(Request::new(NewShardRequest::default()))
        .await?;
    let shard_id = &new_shard_response.get_ref().id;

    let response = writer
        .get_shard(Request::new(ShardId {
            id: shard_id.clone(),
        }))
        .await?;

    assert_eq!(shard_id, &response.get_ref().id);

    Ok(())
}

#[tokio::test]
async fn test_metadata() -> Result<(), Box<dyn std::error::Error>> {
    let mut writer = node_writer().await;
    let mut reader = node_reader().await;
    const KB0: &str = "KB0";
    const KB1: &str = "KB1";
    const KB2: &str = "KB2";
    let shard_0 = writer
        .new_shard(Request::new(NewShardRequest {
            kbid: KB0.to_string(),
            ..Default::default()
        }))
        .await?
        .into_inner()
        .id;
    let shard_1 = writer
        .new_shard(Request::new(NewShardRequest {
            kbid: KB1.to_string(),
            ..Default::default()
        }))
        .await?
        .into_inner()
        .id;
    let shard_2 = writer
        .new_shard(Request::new(NewShardRequest {
            kbid: KB2.to_string(),
            ..Default::default()
        }))
        .await?
        .into_inner()
        .id;

    let get_shard_0 = GetShardRequest {
        shard_id: Some(ShardId { id: shard_0 }),
        vectorset: "".to_string(),
    };
    let request_0 = reader
        .get_shard(Request::new(get_shard_0))
        .await?
        .into_inner()
        .metadata
        .unwrap();

    let get_shard_1 = GetShardRequest {
        shard_id: Some(ShardId { id: shard_1 }),
        vectorset: "".to_string(),
    };
    let request_1 = reader
        .get_shard(Request::new(get_shard_1))
        .await?
        .into_inner()
        .metadata
        .unwrap();

    let get_shard_2 = GetShardRequest {
        shard_id: Some(ShardId { id: shard_2 }),
        vectorset: "".to_string(),
    };
    let request_2 = reader
        .get_shard(Request::new(get_shard_2))
        .await?
        .into_inner()
        .metadata
        .unwrap();

    assert_eq!(request_0.kbid, KB0);
    assert_eq!(request_1.kbid, KB1);
    assert_eq!(request_2.kbid, KB2);
    Ok(())
}
