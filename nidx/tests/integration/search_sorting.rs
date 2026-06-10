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

use std::collections::HashMap;
use std::time::SystemTime;

use crate::common::services::NidxFixture;
use nidx_protos::nodewriter::VectorIndexConfig;
use nidx_protos::prost_types::Timestamp;
use nidx_protos::resource::ResourceStatus;
use nidx_protos::{IndexMetadata, NewShardRequest, Resource, ResourceId, SearchRequest};
use sqlx::PgPool;
use tonic::Request;
use uuid::Uuid;

async fn create_dummy_resources(total: u8, writer: &mut NidxFixture, shard_id: String) {
    for i in 0..total {
        let rid = Uuid::new_v4();
        let field = format!("dummy-{i:0>3}");

        let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap();
        let timestamp = Timestamp {
            seconds: now.as_secs() as i64 - (total - i) as i64,
            nanos: 0,
        };

        let labels = vec![format!("/dummy{i:0>3}")];
        let mut texts = HashMap::new();
        texts.insert(
            field,
            nidx_protos::TextInformation {
                text: format!("Dummy text {i:0>3}"),
                labels: vec![],
            },
        );
        writer
            .index_resource(
                &shard_id,
                Resource {
                    shard_id: shard_id.clone(),
                    resource: Some(ResourceId {
                        shard_id: shard_id.clone(),
                        uuid: rid.to_string(),
                    }),
                    status: ResourceStatus::Processed as i32,
                    metadata: Some(IndexMetadata {
                        created: Some(timestamp),
                        modified: Some(timestamp),
                    }),
                    labels,
                    texts,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
    }
}

#[sqlx::test]
async fn test_search_sorting(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let mut fixture = NidxFixture::new(pool).await?;
    let new_shard_response = fixture
        .api_client
        .new_shard(Request::new(NewShardRequest {
            kbid: "aabbccddeeff11223344556677889900".to_string(),
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

    create_dummy_resources(20, &mut fixture, shard_id.clone()).await;
    fixture.wait_sync().await;

    async fn do_search(
        fixture: &mut NidxFixture,
        shard_id: String,
        order: Option<nidx_protos::OrderBy>,
        page_size: i32,
    ) -> Vec<String> {
        let mut fields = Vec::new();

        let response = fixture
            .searcher_client
            .search(SearchRequest {
                shard: shard_id.clone(),
                order,
                document: true,
                vectorset: "english".to_string(),
                result_per_page: page_size,
                ..Default::default()
            })
            .await
            .unwrap();

        let document_response = response.get_ref().document.as_ref().unwrap();
        fields.extend(document_response.results.iter().cloned().map(|r| r.field));

        fields
    }

    // --------------------------------------------------------------
    // Test: sort by creation date in ascending order
    // --------------------------------------------------------------

    let order = Some(nidx_protos::OrderBy {
        sort_by: nidx_protos::order_by::OrderField::Created.into(),
        r#type: nidx_protos::order_by::OrderType::Asc.into(),
    });
    let fields = do_search(&mut fixture, shard_id.clone(), order, 5).await;
    let mut sorted_fields = fields.clone();
    sorted_fields.sort();
    assert_eq!(fields, sorted_fields);

    // --------------------------------------------------------------
    // Test: sort by modified date in ascending order
    // --------------------------------------------------------------

    let order = Some(nidx_protos::OrderBy {
        sort_by: nidx_protos::order_by::OrderField::Modified.into(),
        r#type: nidx_protos::order_by::OrderType::Asc.into(),
    });
    let fields = do_search(&mut fixture, shard_id.clone(), order, 5).await;

    let mut sorted_fields = fields.clone();
    sorted_fields.sort();
    assert_eq!(fields, sorted_fields);

    // --------------------------------------------------------------
    // Test: sort by creation date in descending order
    // --------------------------------------------------------------

    let order = Some(nidx_protos::OrderBy {
        sort_by: nidx_protos::order_by::OrderField::Created.into(),
        r#type: nidx_protos::order_by::OrderType::Desc.into(),
    });
    let fields = do_search(&mut fixture, shard_id.clone(), order, 5).await;

    let mut sorted_fields = fields.clone();
    sorted_fields.sort();
    sorted_fields.reverse();
    assert_eq!(fields, sorted_fields);

    // --------------------------------------------------------------
    // Test: sort by modified date in descending order
    // --------------------------------------------------------------

    let order = Some(nidx_protos::OrderBy {
        sort_by: nidx_protos::order_by::OrderField::Modified.into(),
        r#type: nidx_protos::order_by::OrderType::Desc.into(),
    });
    let fields = do_search(&mut fixture, shard_id.clone(), order, 5).await;

    let mut sorted_fields = fields.clone();
    sorted_fields.sort();
    sorted_fields.reverse();
    assert_eq!(fields, sorted_fields);

    Ok(())
}
