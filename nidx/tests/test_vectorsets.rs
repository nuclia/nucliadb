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

use common::services::NidxFixture;
use nidx_protos::{
    op_status::Status, NewShardRequest, NewVectorSetRequest, ShardId, VectorIndexConfig, VectorSetId, VectorSimilarity,
};
use sqlx::PgPool;
use std::collections::HashMap;
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
                ("english".to_string(), VectorIndexConfig::default()),
                ("multilingual".to_string(), VectorIndexConfig::default()),
                ("spanish".to_string(), VectorIndexConfig::default()),
            ]),
            ..Default::default()
        }))
        .await;
    assert!(response.is_ok());

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
