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

use std::collections::HashMap;

use common::services::NidxFixture;
use nidx_protos::{
    EmptyQuery, GetShardRequest, NewShardRequest, ShardId, VectorIndexConfig, nidx::nidx_api_client::NidxApiClient,
};
use sqlx::PgPool;
use tonic::{Code, Request, transport::Channel};
use uuid::Uuid;

#[sqlx::test]
async fn test_create_shard(pool: PgPool) -> anyhow::Result<()> {
    let mut fixture = NidxFixture::new(pool).await?;

    let new_shard_response = fixture
        .api_client
        .new_shard(Request::new(NewShardRequest {
            kbid: "aabbccdd-eeff-1122-3344-556677889900".to_string(),
            vectorsets_configs: HashMap::from([(
                "english".to_string(),
                VectorIndexConfig {
                    vector_dimension: Some(3),
                    ..Default::default()
                },
            )]),
            ..Default::default()
        }))
        .await?;
    let shard_id = &new_shard_response.get_ref().id;

    let response = fixture
        .api_client
        .get_shard(Request::new(GetShardRequest {
            shard_id: Some(ShardId {
                id: shard_id.to_owned(),
            }),
            ..Default::default()
        }))
        .await?;

    let response = response.into_inner();
    assert_eq!(&response.shard_id, shard_id);
    assert_eq!(&response.metadata.unwrap().kbid, "aabbccdd-eeff-1122-3344-556677889900");

    // get_shard error handling
    let response = fixture
        .api_client
        .get_shard(Request::new(GetShardRequest {
            shard_id: Some(ShardId {
                id: Uuid::new_v4().to_string(),
            }),
            ..Default::default()
        }))
        .await;
    let err = response.expect_err("Should have failed");
    assert_eq!(err.code(), Code::NotFound);

    let response = fixture
        .api_client
        .get_shard(Request::new(GetShardRequest {
            shard_id: None,
            ..Default::default()
        }))
        .await;
    let err = response.expect_err("Should have failed");
    assert_eq!(err.code(), Code::InvalidArgument);

    Ok(())
}

#[sqlx::test]
async fn test_list_shards(pool: PgPool) -> anyhow::Result<()> {
    let mut fixture = NidxFixture::new(pool).await?;

    let current = fixture
        .api_client
        .list_shards(Request::new(EmptyQuery {}))
        .await?
        .get_ref()
        .ids
        .iter()
        .map(|s| s.id.clone())
        .len();

    let request_ids = create_shards(&mut fixture.api_client, 5).await;

    let response = fixture
        .api_client
        .list_shards(Request::new(EmptyQuery {}))
        .await
        .expect("Error in list_shards request");

    let response_ids: Vec<String> = response.get_ref().ids.iter().map(|s| s.id.clone()).collect();

    assert!(!request_ids.is_empty());
    assert_eq!(request_ids.len() + current, response_ids.len());
    assert!(request_ids.iter().all(|item| { response_ids.contains(item) }));

    Ok(())
}

#[sqlx::test]
async fn test_delete_shards(pool: PgPool) -> anyhow::Result<()> {
    let mut fixture = NidxFixture::new(pool).await?;

    let current = fixture
        .api_client
        .list_shards(Request::new(EmptyQuery {}))
        .await?
        .get_ref()
        .ids
        .iter()
        .map(|s| s.id.clone())
        .len();

    let request_ids = create_shards(&mut fixture.api_client, 5).await;

    for (id, expected) in request_ids.iter().map(|v| (v.clone(), v.clone())) {
        let response = fixture
            .api_client
            .delete_shard(Request::new(ShardId { id }))
            .await
            .expect("Error in delete_shard request");
        let deleted_id = response.get_ref().id.clone();
        assert_eq!(deleted_id, expected);
    }

    let response = fixture
        .api_client
        .list_shards(Request::new(EmptyQuery {}))
        .await
        .expect("Error in list_shards request");

    assert_eq!(response.get_ref().ids.len(), current);

    Ok(())
}

async fn create_shards(writer: &mut NidxApiClient<Channel>, n: usize) -> Vec<String> {
    let mut shard_ids = Vec::with_capacity(n);

    for _ in 0..n {
        let response = writer
            .new_shard(Request::new(NewShardRequest {
                kbid: "aabbccdd-eeff-1122-3344-556677889900".to_string(),
                vectorsets_configs: HashMap::from([(
                    "english".to_string(),
                    VectorIndexConfig {
                        vector_dimension: Some(3),
                        ..Default::default()
                    },
                )]),
                ..Default::default()
            }))
            .await
            .expect("Error in new_shard request");

        shard_ids.push(response.get_ref().id.clone());
    }

    shard_ids
}
