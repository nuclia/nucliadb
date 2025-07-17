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
use std::time::SystemTime;

use common::services::NidxFixture;
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
                order: order.clone(),
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
        ..Default::default()
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
        ..Default::default()
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
        ..Default::default()
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
        ..Default::default()
    });
    let fields = do_search(&mut fixture, shard_id.clone(), order, 5).await;

    let mut sorted_fields = fields.clone();
    sorted_fields.sort();
    sorted_fields.reverse();
    assert_eq!(fields, sorted_fields);

    Ok(())
}
