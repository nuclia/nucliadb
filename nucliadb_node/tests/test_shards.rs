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

mod common;

use common::{NodeFixture, TestNodeReader, TestNodeWriter};
use nucliadb_core::protos::{EmptyQuery, GetShardRequest, NewShardRequest, ShardId};
use tonic::Request;

#[tokio::test]
async fn test_create_shard() -> Result<(), Box<dyn std::error::Error>> {
    let mut fixture = NodeFixture::new();
    fixture.with_writer().await?.with_reader().await?;
    let mut writer = fixture.writer_client();
    let mut reader = fixture.reader_client();

    let new_shard_response = writer
        .new_shard(Request::new(NewShardRequest::default()))
        .await?;
    let shard_id = &new_shard_response.get_ref().id;

    let response = reader
        .get_shard(Request::new(GetShardRequest {
            shard_id: Some(ShardId {
                id: shard_id.to_owned(),
            }),
            ..Default::default()
        }))
        .await?;

    assert_eq!(shard_id, &response.get_ref().shard_id);

    Ok(())
}

#[tokio::test]
async fn test_shard_metadata() -> Result<(), Box<dyn std::error::Error>> {
    let mut fixture = NodeFixture::new();
    fixture.with_writer().await?.with_reader().await?;
    let mut writer = fixture.writer_client();
    let mut reader = fixture.reader_client();

    async fn create_shard_with_metadata(
        writer: &mut TestNodeWriter,
        kbid: String,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let shard = writer
            .new_shard(Request::new(NewShardRequest {
                kbid,
                ..Default::default()
            }))
            .await?
            .into_inner();
        Ok(shard.id)
    }

    async fn validate_shard_metadata(
        reader: &mut TestNodeReader,
        shard_id: String,
        kbid: String,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let shard = reader
            .get_shard(Request::new(GetShardRequest {
                shard_id: Some(ShardId { id: shard_id }),
                ..Default::default()
            }))
            .await?
            .into_inner();

        assert!(shard.metadata.is_some());

        let shard_metadata = shard.metadata.unwrap();
        assert_eq!(shard_metadata.kbid, kbid);

        Ok(())
    }

    const KB0: &str = "KB0";
    const KB1: &str = "KB1";
    const KB2: &str = "KB2";

    // Used to validate correct creation
    let shard_0 = create_shard_with_metadata(&mut writer, KB0.to_string()).await?;
    // Used to check 1 is not overwritting 0
    let shard_1 = create_shard_with_metadata(&mut writer, KB1.to_string()).await?;
    // Used to validate correct creation when there are more shards
    let shard_2 = create_shard_with_metadata(&mut writer, KB2.to_string()).await?;

    validate_shard_metadata(&mut reader, shard_0, KB0.to_string()).await?;
    validate_shard_metadata(&mut reader, shard_1, KB1.to_string()).await?;
    validate_shard_metadata(&mut reader, shard_2, KB2.to_string()).await?;

    Ok(())
}

#[tokio::test]
async fn test_list_shards() -> Result<(), Box<dyn std::error::Error>> {
    let mut fixture = NodeFixture::new();
    fixture.with_writer().await?.with_reader().await?;
    let mut writer = fixture.writer_client();

    let current = writer
        .list_shards(Request::new(EmptyQuery {}))
        .await?
        .get_ref()
        .ids
        .iter()
        .map(|s| s.id.clone())
        .len();

    let request_ids = create_shards(&mut writer, 5).await;

    let response = writer
        .list_shards(Request::new(EmptyQuery {}))
        .await
        .expect("Error in list_shards request");

    let response_ids: Vec<String> = response
        .get_ref()
        .ids
        .iter()
        .map(|s| s.id.clone())
        .collect();

    assert!(!request_ids.is_empty());
    assert_eq!(request_ids.len() + current, response_ids.len());
    assert!(request_ids
        .iter()
        .all(|item| { response_ids.contains(item) }));

    Ok(())
}

#[tokio::test]
async fn test_delete_shards() -> anyhow::Result<()> {
    let mut fixture = NodeFixture::new();
    fixture.with_writer().await?.with_reader().await?;
    let mut writer = fixture.writer_client();

    let current = writer
        .list_shards(Request::new(EmptyQuery {}))
        .await?
        .get_ref()
        .ids
        .iter()
        .map(|s| s.id.clone())
        .len();

    let request_ids = create_shards(&mut writer, 5).await;

    // XXX why are we doing this?
    for id in request_ids.iter().cloned() {
        _ = writer
            .clean_and_upgrade_shard(Request::new(ShardId { id }))
            .await
            .expect("Error in new_shard request");
    }

    for (id, expected) in request_ids.iter().map(|v| (v.clone(), v.clone())) {
        let response = writer
            .delete_shard(Request::new(ShardId { id }))
            .await
            .expect("Error in delete_shard request");
        let deleted_id = response.get_ref().id.clone();
        assert_eq!(deleted_id, expected);
    }

    let response = writer
        .list_shards(Request::new(EmptyQuery {}))
        .await
        .expect("Error in list_shards request");

    assert_eq!(response.get_ref().ids.len(), current);

    Ok(())
}

async fn create_shards(writer: &mut TestNodeWriter, n: usize) -> Vec<String> {
    let mut shard_ids = Vec::with_capacity(n);

    for _ in 0..n {
        let response = writer
            .new_shard(Request::new(NewShardRequest::default()))
            .await
            .expect("Error in new_shard request");

        shard_ids.push(response.get_ref().id.clone());
    }

    shard_ids
}
