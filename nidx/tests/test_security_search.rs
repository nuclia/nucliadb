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
use nidx_protos::relation::RelationType;
use nidx_protos::relation_node::NodeType;
use nidx_protos::resource::ResourceStatus;
use nidx_protos::{
    GraphSearchRequest, IndexMetadata, NewShardRequest, RelationNode, Resource, ResourceId, SearchRequest,
};
use nidx_protos::{IndexRelation, IndexRelations, Relation, Security};
use sqlx::PgPool;
use tonic::Request;
use uuid::Uuid;

async fn create_dummy_resources(total: u8, fixture: &mut NidxFixture, shard_id: String, access_groups: Vec<String>) {
    for i in 0..total {
        println!("Creating dummy resource {}/{}", i, total);
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
        let mut security = Security::default();
        if !access_groups.is_empty() {
            security.access_groups = access_groups.clone();
        }

        fixture
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
                    security: Some(security),
                    field_relations: HashMap::from([(
                        "f/file".to_string(),
                        IndexRelations {
                            relations: vec![IndexRelation {
                                relation: Some(Relation {
                                    source: Some(RelationNode {
                                        value: rid.to_string(),
                                        ntype: NodeType::Resource.into(),
                                        subtype: "RESOURCE".to_string(),
                                    }),
                                    to: Some(RelationNode {
                                        value: "/l/a/b".to_string(),
                                        ntype: NodeType::Label.into(),
                                        subtype: "LABEL".to_string(),
                                    }),
                                    relation: RelationType::About.into(),
                                    relation_label: "LABEL".to_string(),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            }],
                        },
                    )]),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
    }
}

#[sqlx::test]
async fn test_security_search(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
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

    let group_engineering = "engineering".to_string();

    create_dummy_resources(1, &mut fixture, shard_id.clone(), vec![group_engineering.clone()]).await;
    fixture.wait_sync().await;

    // Searching with no security should return 1 results
    let response = fixture
        .searcher_client
        .search(SearchRequest {
            body: "text".to_string(),
            shard: shard_id.clone(),
            document: true,
            vectorset: "english".to_string(),
            security: Some(Security { access_groups: vec![] }),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(response.get_ref().document.as_ref().unwrap().total, 1);

    let response = fixture
        .searcher_client
        .graph_search(GraphSearchRequest {
            shard: shard_id.clone(),
            query: Some(nidx_protos::GraphQuery {
                path: Some(Default::default()),
            }),
            top_k: 100,
            security: Some(Security { access_groups: vec![] }),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(response.get_ref().relations.len(), 1);

    // Searching with an unknown group should return no results
    let response = fixture
        .searcher_client
        .search(SearchRequest {
            shard: shard_id.clone(),
            document: true,
            vectorset: "english".to_string(),
            security: Some(Security {
                access_groups: vec!["unknown".to_string()],
            }),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(response.get_ref().document.as_ref(), None);

    let response = fixture
        .searcher_client
        .graph_search(GraphSearchRequest {
            shard: shard_id.clone(),
            query: Some(nidx_protos::GraphQuery {
                path: Some(Default::default()),
            }),
            top_k: 100,
            security: Some(Security {
                access_groups: vec!["unknown".to_string()],
            }),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(response.get_ref().relations.len(), 0);

    // Searching with engineering group should return only one result
    let response = fixture
        .searcher_client
        .search(SearchRequest {
            shard: shard_id.clone(),
            document: true,
            vectorset: "english".to_string(),
            security: Some(Security {
                access_groups: vec![group_engineering.clone()],
            }),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(response.get_ref().document.as_ref().unwrap().total, 1);

    let response = fixture
        .searcher_client
        .graph_search(GraphSearchRequest {
            shard: shard_id.clone(),
            query: Some(nidx_protos::GraphQuery {
                path: Some(Default::default()),
            }),
            top_k: 100,
            security: Some(Security {
                access_groups: vec![group_engineering.to_string()],
            }),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(response.get_ref().relations.len(), 1);

    // Searching with engineering group and another unknown group
    // should return 1 result, as the results are expected to be the union of the groups.
    let response = fixture
        .searcher_client
        .search(SearchRequest {
            shard: shard_id.clone(),
            document: true,
            vectorset: "english".to_string(),
            security: Some(Security {
                access_groups: vec!["/unknown".to_string(), group_engineering.clone()],
            }),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(response.get_ref().document.as_ref().unwrap().total, 1);

    let response = fixture
        .searcher_client
        .graph_search(GraphSearchRequest {
            shard: shard_id.clone(),
            query: Some(nidx_protos::GraphQuery {
                path: Some(Default::default()),
            }),
            top_k: 100,
            security: Some(Security {
                access_groups: vec!["/unknown".to_string(), group_engineering.clone()],
            }),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(response.get_ref().relations.len(), 1);

    Ok(())
}

#[sqlx::test]
async fn test_security_search_public_resource(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
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

    create_dummy_resources(1, &mut fixture, shard_id.clone(), vec![]).await;
    fixture.wait_sync().await;

    // Searching with no security should return 1 results
    let response = fixture
        .searcher_client
        .search(SearchRequest {
            body: "text".to_string(),
            vectorset: "english".to_string(),
            shard: shard_id.clone(),
            document: true,
            security: Some(Security { access_groups: vec![] }),
            ..Default::default()
        })
        .await
        .unwrap();

    assert_eq!(response.get_ref().document.as_ref().unwrap().total, 1);

    // Searching with an unknown group should return one result too, as the resource is public
    let response = fixture
        .searcher_client
        .search(SearchRequest {
            shard: shard_id.clone(),
            vectorset: "english".to_string(),
            document: true,
            security: Some(Security {
                access_groups: vec!["/unknown".to_string()],
            }),
            ..Default::default()
        })
        .await
        .unwrap();

    assert_eq!(response.get_ref().document.as_ref().unwrap().total, 1);

    Ok(())
}
