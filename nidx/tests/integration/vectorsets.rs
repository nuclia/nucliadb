// Copyright 2021 Bosutech XXI S.L.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

use crate::common::services::NidxFixture;
use nidx_protos::{
    NewShardRequest, NewVectorSetRequest, ShardId, VectorIndexConfig, VectorSetId, VectorSimilarity, op_status::Status,
};
use sqlx::PgPool;
use std::collections::{HashMap, HashSet};
use tonic::{Code, Request};

#[sqlx::test]
async fn test_new_shard_with_single_vectorset(pool: PgPool) -> anyhow::Result<()> {
    let mut fixture = NidxFixture::new(pool).await?;

    let response = fixture
        .api_client
        .new_shard(Request::new(NewShardRequest {
            kbid: "aabbccdd-eeff-1122-3344-556677889900".to_string(),
            vectorsets_configs: HashMap::from([(
                "english".to_string(),
                VectorIndexConfig {
                    similarity: VectorSimilarity::Dot.into(),
                    normalize_vectors: true,
                    vector_dimension: Some(1000),
                    ..Default::default()
                },
            )]),
            ..Default::default()
        }))
        .await;
    assert!(response.is_ok());
    let shard_id = &response.as_ref().unwrap().get_ref().id;

    let response = fixture
        .api_client
        .list_vector_sets(Request::new(ShardId { id: shard_id.clone() }))
        .await?;
    assert_eq!(response.into_inner().vectorsets, ["english"]);

    Ok(())
}

#[sqlx::test]
async fn test_new_shard_with_multiple_vectorset(pool: PgPool) -> anyhow::Result<()> {
    let mut fixture = NidxFixture::new(pool).await?;

    let response = fixture
        .api_client
        .new_shard(Request::new(NewShardRequest {
            kbid: "aabbccdd-eeff-1122-3344-556677889900".to_string(),
            vectorsets_configs: HashMap::from([
                (
                    "english".to_string(),
                    VectorIndexConfig {
                        vector_dimension: Some(3),
                        ..Default::default()
                    },
                ),
                (
                    "multilingual".to_string(),
                    VectorIndexConfig {
                        vector_dimension: Some(3),
                        ..Default::default()
                    },
                ),
                (
                    "spanish".to_string(),
                    VectorIndexConfig {
                        vector_dimension: Some(3),
                        ..Default::default()
                    },
                ),
            ]),
            ..Default::default()
        }))
        .await;
    assert!(response.is_ok());
    let shard_id = &response.as_ref().unwrap().get_ref().id;

    let response = fixture
        .api_client
        .list_vector_sets(Request::new(ShardId { id: shard_id.clone() }))
        .await?;
    let vectorsets = response.into_inner().vectorsets;
    assert_eq!(
        HashSet::from_iter(vectorsets.iter().map(|v| v.as_str())),
        HashSet::from(["english", "multilingual", "spanish"])
    );

    Ok(())
}

#[sqlx::test]
async fn test_add_vectorset_to_shard(pool: PgPool) -> anyhow::Result<()> {
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
        .add_vector_set(Request::new(NewVectorSetRequest {
            id: Some(VectorSetId {
                shard: Some(ShardId { id: shard_id.clone() }),
                vectorset: "multilingual".to_string(),
            }),
            config: Some(VectorIndexConfig {
                similarity: VectorSimilarity::Dot.into(),
                normalize_vectors: true,
                vector_dimension: Some(3),
                ..Default::default()
            }),
        }))
        .await?;

    assert_eq!(response.get_ref().status(), Status::Ok);

    Ok(())
}

#[sqlx::test]
async fn test_new_shard_without_vectorset_not_allowed(pool: PgPool) -> anyhow::Result<()> {
    let mut fixture = NidxFixture::new(pool).await?;

    let response = fixture
        .api_client
        .new_shard(Request::new(NewShardRequest {
            kbid: "aabbccdd-eeff-1122-3344-556677889900".to_string(),
            vectorsets_configs: HashMap::new(),
            ..Default::default()
        }))
        .await;
    assert!(response.is_err());

    let error = response.unwrap_err();
    assert_eq!(error.code(), Code::InvalidArgument);

    Ok(())
}

#[sqlx::test]
async fn test_remove_vectorset(pool: PgPool) -> anyhow::Result<()> {
    let mut fixture = NidxFixture::new(pool).await?;

    let response = fixture
        .api_client
        .new_shard(Request::new(NewShardRequest {
            kbid: "aabbccdd-eeff-1122-3344-556677889900".to_string(),
            vectorsets_configs: HashMap::from([
                (
                    "english".to_string(),
                    VectorIndexConfig {
                        vector_dimension: Some(3),
                        ..Default::default()
                    },
                ),
                (
                    "multilingual".to_string(),
                    VectorIndexConfig {
                        vector_dimension: Some(3),
                        ..Default::default()
                    },
                ),
                (
                    "spanish".to_string(),
                    VectorIndexConfig {
                        vector_dimension: Some(3),
                        ..Default::default()
                    },
                ),
            ]),
            ..Default::default()
        }))
        .await;
    assert!(response.is_ok());
    let shard_id = &response.as_ref().unwrap().get_ref().id;

    let response = fixture
        .api_client
        .remove_vector_set(Request::new(VectorSetId {
            shard: Some(ShardId { id: shard_id.clone() }),
            vectorset: "multilingual".to_string(),
        }))
        .await?;
    assert_eq!(response.into_inner().status(), Status::Ok);

    let response = fixture
        .api_client
        .list_vector_sets(Request::new(ShardId { id: shard_id.clone() }))
        .await?;
    let vectorsets = response.into_inner().vectorsets;
    assert_eq!(
        HashSet::from_iter(vectorsets.iter().map(|v| v.as_str())),
        HashSet::from(["english", "spanish"])
    );

    Ok(())
}

#[sqlx::test]
async fn test_cant_remove_all_vectorsets(pool: PgPool) -> anyhow::Result<()> {
    let mut fixture = NidxFixture::new(pool).await?;

    let response = fixture
        .api_client
        .new_shard(Request::new(NewShardRequest {
            kbid: "aabbccdd-eeff-1122-3344-556677889900".to_string(),
            vectorsets_configs: HashMap::from([
                (
                    "english".to_string(),
                    VectorIndexConfig {
                        vector_dimension: Some(3),
                        ..Default::default()
                    },
                ),
                (
                    "multilingual".to_string(),
                    VectorIndexConfig {
                        vector_dimension: Some(3),
                        ..Default::default()
                    },
                ),
            ]),
            ..Default::default()
        }))
        .await;
    assert!(response.is_ok());
    let shard_id = &response.as_ref().unwrap().get_ref().id;

    let response = fixture
        .api_client
        .remove_vector_set(Request::new(VectorSetId {
            shard: Some(ShardId { id: shard_id.clone() }),
            vectorset: "english".to_string(),
        }))
        .await?;
    assert_eq!(response.into_inner().status(), Status::Ok);

    let response = fixture
        .api_client
        .remove_vector_set(Request::new(VectorSetId {
            shard: Some(ShardId { id: shard_id.clone() }),
            vectorset: "multilingual".to_string(),
        }))
        .await;
    assert!(response.is_err());

    let response = fixture
        .api_client
        .list_vector_sets(Request::new(ShardId { id: shard_id.clone() }))
        .await?;
    let vectorsets = response.into_inner().vectorsets;
    assert_eq!(
        HashSet::from_iter(vectorsets.iter().map(|v| v.as_str())),
        HashSet::from(["multilingual"])
    );

    Ok(())
}
