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
use nidx_protos::VectorIndexConfig;
use nidx_protos::filter_expression::{Expr, ResourceFilter};
use nidx_protos::graph_query::node::MatchKind;
use nidx_protos::graph_query::path_query;
use nidx_protos::graph_search_request::QueryKind;
use nidx_protos::prost_types::Timestamp;
use nidx_protos::relation_node::NodeType;
use nidx_protos::resource::ResourceStatus;
use nidx_protos::{
    FilterExpression, GraphQuery, GraphSearchRequest, IndexMetadata, IndexRelations, NewShardRequest, Resource,
    ResourceId, TextInformation, graph_query,
};
use nidx_tests::graph::friendly_parse;
use nidx_tests::people_and_places;
use sqlx::PgPool;
use tonic::Request;
use uuid::Uuid;

// NOTE: The following tests are closely related with nidx_relations/tests/test_graph_search.rs.
// They aim to test the same queries on the same graph to ensure protobuf query is equally capable

async fn setup_knowledge_graph(fixture: &mut NidxFixture) -> anyhow::Result<String> {
    let shard_id = create_shard(fixture).await?;

    // Create resource with graph
    let rid = Uuid::new_v4();
    let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap();
    let timestamp = Timestamp {
        seconds: now.as_secs() as i64,
        nanos: 0,
    };
    let knowledge_graph = nidx_tests::graph::knowledge_graph_as_relations();
    fixture
        .index_resource(
            &shard_id,
            Resource {
                shard_id: shard_id.clone(),
                resource: Some(ResourceId {
                    shard_id: shard_id.clone(),
                    uuid: rid.simple().to_string(),
                }),
                status: ResourceStatus::Processed as i32,
                field_relations: HashMap::from([(
                    "a/metadata".to_string(),
                    IndexRelations {
                        relations: knowledge_graph,
                    },
                )]),
                metadata: Some(IndexMetadata {
                    created: Some(timestamp),
                    modified: Some(timestamp),
                }),
                texts: HashMap::from([(
                    "a/title".to_string(),
                    TextInformation {
                        text: "Knowledge graph".to_string(),
                        ..Default::default()
                    },
                )]),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    fixture.wait_sync().await;

    Ok(shard_id)
}

#[sqlx::test]
async fn test_graph_search_nodes(pool: PgPool) -> anyhow::Result<()> {
    let mut fixture = NidxFixture::new(pool).await?;
    let shard_id = setup_knowledge_graph(&mut fixture).await?;

    // Bad query
    let response = fixture
        .searcher_client
        .graph_search(GraphSearchRequest {
            shard: shard_id.clone(),
            query: Some(GraphQuery {
                path: Some(graph_query::PathQuery {
                    query: Some(path_query::Query::Path(graph_query::Path::default())),
                }),
            }),
            top_k: 100,
            kind: QueryKind::Nodes.into(),
            ..Default::default()
        })
        .await;
    // node query not in the proper format
    assert!(response.is_err());

    // Search all nodes
    // (s)-[]->(d)
    let response = fixture
        .searcher_client
        .graph_search(GraphSearchRequest {
            shard: shard_id.clone(),
            query: Some(GraphQuery {
                path: Some(graph_query::PathQuery {
                    query: Some(path_query::Query::Path(graph_query::Path {
                        source: Some(graph_query::Node::default()),
                        undirected: true,
                        ..Default::default()
                    })),
                }),
            }),
            top_k: 100,
            kind: QueryKind::Nodes.into(),
            ..Default::default()
        })
        .await?
        .into_inner();
    let nodes = &response.nodes;
    assert_eq!(nodes.len(), 18);

    // Limit by top_k
    let response = fixture
        .searcher_client
        .graph_search(GraphSearchRequest {
            shard: shard_id.clone(),
            query: Some(GraphQuery {
                path: Some(graph_query::PathQuery {
                    query: Some(path_query::Query::Path(graph_query::Path {
                        source: Some(graph_query::Node::default()),
                        undirected: true,
                        ..Default::default()
                    })),
                }),
            }),
            top_k: 10,
            kind: QueryKind::Nodes.into(),
            ..Default::default()
        })
        .await?
        .into_inner();
    let nodes = &response.nodes;
    assert_eq!(nodes.len(), 10);

    // Search all ANIMAL nodes
    let response = fixture
        .searcher_client
        .graph_search(GraphSearchRequest {
            shard: shard_id.clone(),
            query: Some(GraphQuery {
                path: Some(graph_query::PathQuery {
                    query: Some(path_query::Query::Path(graph_query::Path {
                        source: Some(graph_query::Node {
                            node_subtype: Some("ANIMAL".to_string()),
                            ..Default::default()
                        }),
                        undirected: true,
                        ..Default::default()
                    })),
                }),
            }),
            top_k: 100,
            kind: QueryKind::Nodes.into(),
            ..Default::default()
        })
        .await?
        .into_inner();
    let nodes = &response.nodes;
    assert_eq!(nodes.len(), 4);

    Ok(())
}

#[sqlx::test]
#[allow(non_snake_case)]
async fn test_graph_search__node_query(pool: PgPool) -> anyhow::Result<()> {
    let mut fixture = NidxFixture::new(pool).await?;
    let shard_id = setup_knowledge_graph(&mut fixture).await?;

    // ()-[]->()
    let response = fixture
        .searcher_client
        .graph_search(GraphSearchRequest {
            shard: shard_id.clone(),
            query: Some(GraphQuery {
                path: Some(graph_query::PathQuery {
                    query: Some(path_query::Query::Path(graph_query::Path::default())),
                }),
            }),
            top_k: 100,
            ..Default::default()
        })
        .await?
        .into_inner();
    let relations = friendly_parse(&response);
    assert_eq!(relations.len(), 17);

    // (:PERSON)-[]->()
    let response = fixture
        .searcher_client
        .graph_search(GraphSearchRequest {
            shard: shard_id.clone(),
            query: Some(GraphQuery {
                path: Some(graph_query::PathQuery {
                    query: Some(path_query::Query::Path(graph_query::Path {
                        source: Some(graph_query::Node {
                            node_type: Some(NodeType::Entity.into()),
                            node_subtype: Some("PERSON".to_string()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    })),
                }),
            }),
            top_k: 100,
            ..Default::default()
        })
        .await?
        .into_inner();
    let relations = friendly_parse(&response);
    assert_eq!(relations.len(), 12);

    // (:Anna)-[]->()
    let response = fixture
        .searcher_client
        .graph_search(GraphSearchRequest {
            shard: shard_id.clone(),
            query: Some(GraphQuery {
                path: Some(graph_query::PathQuery {
                    query: Some(path_query::Query::Path(graph_query::Path {
                        source: Some(graph_query::Node {
                            value: Some("Anna".to_string()),
                            node_type: Some(NodeType::Entity.into()),
                            node_subtype: Some("PERSON".to_string()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    })),
                }),
            }),
            top_k: 100,
            ..Default::default()
        })
        .await?
        .into_inner();
    let relations = friendly_parse(&response);
    assert_eq!(relations.len(), 4);
    assert!(relations.contains(&("Anna", "FOLLOW", "Erin")));
    assert!(relations.contains(&("Anna", "LIVE_IN", "New York")));
    assert!(relations.contains(&("Anna", "LOVE", "Cat")));
    assert!(relations.contains(&("Anna", "WORK_IN", "New York")));

    // ()-[]->(:Anna)
    let response = fixture
        .searcher_client
        .graph_search(GraphSearchRequest {
            shard: shard_id.clone(),
            query: Some(GraphQuery {
                path: Some(graph_query::PathQuery {
                    query: Some(path_query::Query::Path(graph_query::Path {
                        destination: Some(graph_query::Node {
                            value: Some("Anna".to_string()),
                            node_type: Some(NodeType::Entity.into()),
                            node_subtype: Some("PERSON".to_string()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    })),
                }),
            }),
            top_k: 100,
            ..Default::default()
        })
        .await?
        .into_inner();
    let relations = friendly_parse(&response);
    assert_eq!(relations.len(), 1);
    assert!(relations.contains(&("Anastasia", "IS_FRIEND", "Anna")));

    // (:Anna)
    let response = fixture
        .searcher_client
        .graph_search(GraphSearchRequest {
            shard: shard_id.clone(),
            query: Some(GraphQuery {
                path: Some(graph_query::PathQuery {
                    query: Some(path_query::Query::Path(graph_query::Path {
                        source: Some(graph_query::Node {
                            value: Some("Anna".to_string()),
                            node_type: Some(NodeType::Entity.into()),
                            node_subtype: Some("PERSON".to_string()),
                            ..Default::default()
                        }),
                        undirected: true,
                        ..Default::default()
                    })),
                }),
            }),
            top_k: 100,
            ..Default::default()
        })
        .await?
        .into_inner();
    let relations = friendly_parse(&response);
    assert_eq!(relations.len(), 5);
    assert!(relations.contains(&("Anna", "FOLLOW", "Erin")));
    assert!(relations.contains(&("Anna", "LIVE_IN", "New York")));
    assert!(relations.contains(&("Anna", "LOVE", "Cat")));
    assert!(relations.contains(&("Anna", "WORK_IN", "New York")));
    assert!(relations.contains(&("Anastasia", "IS_FRIEND", "Anna")));

    Ok(())
}

#[sqlx::test]
#[allow(non_snake_case)]
async fn test_graph_search__fuzzy_node_query(pool: PgPool) -> anyhow::Result<()> {
    let mut fixture = NidxFixture::new(pool).await?;
    let shard_id = setup_knowledge_graph(&mut fixture).await?;

    // (:~Anastas)
    #[allow(deprecated)]
    let response = fixture
        .searcher_client
        .graph_search(GraphSearchRequest {
            shard: shard_id.clone(),
            query: Some(GraphQuery {
                path: Some(graph_query::PathQuery {
                    query: Some(path_query::Query::Path(graph_query::Path {
                        source: Some(graph_query::Node {
                            value: Some("Anastas".to_string()),
                            match_kind: MatchKind::DeprecatedFuzzy.into(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    })),
                }),
            }),
            top_k: 100,
            ..Default::default()
        })
        .await?
        .into_inner();
    let relations = friendly_parse(&response);
    assert_eq!(relations.len(), 1);
    assert!(relations.contains(&("Anastasia", "IS_FRIEND", "Anna")));

    // (:~AnXstXsiX)
    #[allow(deprecated)]
    let response = fixture
        .searcher_client
        .graph_search(GraphSearchRequest {
            shard: shard_id.clone(),
            query: Some(GraphQuery {
                path: Some(graph_query::PathQuery {
                    query: Some(path_query::Query::Path(graph_query::Path {
                        source: Some(graph_query::Node {
                            value: Some("AnXstXsiX".to_string()),
                            match_kind: MatchKind::DeprecatedFuzzy.into(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    })),
                }),
            }),
            top_k: 100,
            ..Default::default()
        })
        .await?
        .into_inner();
    let relations = friendly_parse(&response);
    assert_eq!(relations.len(), 0);

    // (:~Ansatasia)
    #[allow(deprecated)]
    let response = fixture
        .searcher_client
        .graph_search(GraphSearchRequest {
            shard: shard_id.clone(),
            query: Some(GraphQuery {
                path: Some(graph_query::PathQuery {
                    query: Some(path_query::Query::Path(graph_query::Path {
                        source: Some(graph_query::Node {
                            value: Some("Ansatasia".to_string()),
                            match_kind: MatchKind::DeprecatedFuzzy.into(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    })),
                }),
            }),
            top_k: 100,
            ..Default::default()
        })
        .await?
        .into_inner();
    let relations = friendly_parse(&response);
    assert_eq!(relations.len(), 1);
    assert!(relations.contains(&("Anastasia", "IS_FRIEND", "Anna")));

    Ok(())
}

#[sqlx::test]
#[allow(non_snake_case)]
async fn test_graph_search_relations(pool: PgPool) -> anyhow::Result<()> {
    let mut fixture = NidxFixture::new(pool).await?;
    let shard_id = setup_knowledge_graph(&mut fixture).await?;

    // Does LIVE_IN relation exist?
    // ()-[:LIVE_IN]->()
    let response = fixture
        .searcher_client
        .graph_search(GraphSearchRequest {
            shard: shard_id.clone(),
            query: Some(GraphQuery {
                path: Some(graph_query::PathQuery {
                    query: Some(path_query::Query::Path(graph_query::Path {
                        relation: Some(graph_query::Relation {
                            value: Some("LIVE_IN".to_string()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    })),
                }),
            }),
            kind: QueryKind::Relations.into(),
            top_k: 100,
            ..Default::default()
        })
        .await?
        .into_inner();
    let relations = response.relations;
    assert_eq!(relations.len(), 1);
    assert!(relations.iter().any(|r| r.label == "LIVE_IN"));

    // Does PLAYED relation exist?
    // ()-[:PLAYED]->()
    let response = fixture
        .searcher_client
        .graph_search(GraphSearchRequest {
            shard: shard_id.clone(),
            query: Some(GraphQuery {
                path: Some(graph_query::PathQuery {
                    query: Some(path_query::Query::Path(graph_query::Path {
                        relation: Some(graph_query::Relation {
                            value: Some("PLAYED".to_string()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    })),
                }),
            }),
            kind: QueryKind::Relations.into(),
            top_k: 100,
            ..Default::default()
        })
        .await?
        .into_inner();
    let relations = response.relations;
    assert_eq!(relations.len(), 0);

    // Search all relations from Anna
    // (:Anna)-[r]->()
    let response = fixture
        .searcher_client
        .graph_search(GraphSearchRequest {
            shard: shard_id.clone(),
            query: Some(GraphQuery {
                path: Some(graph_query::PathQuery {
                    query: Some(path_query::Query::Path(graph_query::Path {
                        source: Some(graph_query::Node {
                            value: Some("Anna".to_string()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    })),
                }),
            }),
            kind: QueryKind::Relations.into(),
            top_k: 100,
            ..Default::default()
        })
        .await?
        .into_inner();
    let relations = response.relations;
    assert_eq!(relations.len(), 4);
    assert!(relations.iter().any(|r| r.label == "FOLLOW"));
    assert!(relations.iter().any(|r| r.label == "LIVE_IN"));
    assert!(relations.iter().any(|r| r.label == "LOVE"));
    assert!(relations.iter().any(|r| r.label == "WORK_IN"));

    Ok(())
}

#[sqlx::test]
#[allow(non_snake_case)]
async fn test_graph_search__relation_query(pool: PgPool) -> anyhow::Result<()> {
    let mut fixture = NidxFixture::new(pool).await?;
    let shard_id = setup_knowledge_graph(&mut fixture).await?;

    // ()-[:LIVE_IN]->()
    let response = fixture
        .searcher_client
        .graph_search(GraphSearchRequest {
            shard: shard_id.clone(),
            query: Some(GraphQuery {
                path: Some(graph_query::PathQuery {
                    query: Some(path_query::Query::Path(graph_query::Path {
                        relation: Some(graph_query::Relation {
                            value: Some("LIVE_IN".to_string()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    })),
                }),
            }),
            top_k: 100,
            ..Default::default()
        })
        .await?
        .into_inner();
    let relations = friendly_parse(&response);
    assert_eq!(relations.len(), 2);
    // TODO: this shoulnd't contain the paths, as we are asking only for relations
    assert!(relations.contains(&("Anna", "LIVE_IN", "New York")));
    assert!(relations.contains(&("Peter", "LIVE_IN", "New York")));

    // ()-[:LIVE_IN]->()
    let response = fixture
        .searcher_client
        .graph_search(GraphSearchRequest {
            shard: shard_id.clone(),
            query: Some(GraphQuery {
                path: Some(graph_query::PathQuery {
                    query: Some(path_query::Query::Path(graph_query::Path {
                        relation: Some(graph_query::Relation {
                            value: Some("LIVE_IN".to_string()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    })),
                }),
            }),
            top_k: 100,
            ..Default::default()
        })
        .await?
        .into_inner();
    let relations = friendly_parse(&response);
    assert_eq!(relations.len(), 2);
    assert!(relations.contains(&("Anna", "LIVE_IN", "New York")));
    assert!(relations.contains(&("Peter", "LIVE_IN", "New York")));

    // TODO: OR and NOT relations

    Ok(())
}

#[sqlx::test]
#[allow(non_snake_case)]
async fn test_graph_search__directed_path_query(pool: PgPool) -> anyhow::Result<()> {
    let mut fixture = NidxFixture::new(pool).await?;
    let shard_id = setup_knowledge_graph(&mut fixture).await?;

    // (:Erin)-[]->(:UK)
    let response = fixture
        .searcher_client
        .graph_search(GraphSearchRequest {
            shard: shard_id.clone(),
            query: Some(GraphQuery {
                path: Some(graph_query::PathQuery {
                    query: Some(path_query::Query::Path(graph_query::Path {
                        source: Some(graph_query::Node {
                            value: Some("Erin".to_string()),
                            node_type: Some(NodeType::Entity.into()),
                            node_subtype: Some("PERSON".to_string()),
                            ..Default::default()
                        }),
                        relation: None,
                        destination: Some(graph_query::Node {
                            value: Some("UK".to_string()),
                            node_type: Some(NodeType::Entity.into()),
                            node_subtype: Some("PLACE".to_string()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    })),
                }),
            }),
            top_k: 100,
            ..Default::default()
        })
        .await?
        .into_inner();
    let relations = friendly_parse(&response);
    assert_eq!(relations.len(), 1);
    assert!(relations.contains(&("Erin", "BORN_IN", "UK")));

    // (:PERSON)-[]->(:PLACE)
    let response = fixture
        .searcher_client
        .graph_search(GraphSearchRequest {
            shard: shard_id.clone(),
            query: Some(GraphQuery {
                path: Some(graph_query::PathQuery {
                    query: Some(path_query::Query::Path(graph_query::Path {
                        source: Some(graph_query::Node {
                            node_type: Some(NodeType::Entity.into()),
                            node_subtype: Some("PERSON".to_string()),
                            ..Default::default()
                        }),
                        relation: None,
                        destination: Some(graph_query::Node {
                            node_subtype: Some("PLACE".to_string()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    })),
                }),
            }),
            top_k: 100,
            ..Default::default()
        })
        .await?
        .into_inner();
    let relations = friendly_parse(&response);
    assert_eq!(relations.len(), 4);
    assert!(relations.contains(&("Anna", "LIVE_IN", "New York")));
    assert!(relations.contains(&("Anna", "WORK_IN", "New York")));
    assert!(relations.contains(&("Erin", "BORN_IN", "UK")));
    assert!(relations.contains(&("Peter", "LIVE_IN", "New York")));

    // (:PERSON)-[:LIVE_IN]->(:PLACE)
    let response = fixture
        .searcher_client
        .graph_search(GraphSearchRequest {
            shard: shard_id.clone(),
            query: Some(GraphQuery {
                path: Some(graph_query::PathQuery {
                    query: Some(path_query::Query::Path(graph_query::Path {
                        source: Some(graph_query::Node {
                            node_type: Some(NodeType::Entity.into()),
                            node_subtype: Some("PERSON".to_string()),
                            ..Default::default()
                        }),
                        relation: Some(graph_query::Relation {
                            value: Some("LIVE_IN".to_string()),
                            ..Default::default()
                        }),
                        destination: Some(graph_query::Node {
                            node_subtype: Some("PLACE".to_string()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    })),
                }),
            }),
            top_k: 100,
            ..Default::default()
        })
        .await?
        .into_inner();
    let relations = friendly_parse(&response);
    assert_eq!(relations.len(), 2);
    assert!(relations.contains(&("Anna", "LIVE_IN", "New York")));
    assert!(relations.contains(&("Peter", "LIVE_IN", "New York")));

    // TODO: OR and NOT expressions

    Ok(())
}

#[sqlx::test]
#[allow(non_snake_case)]
async fn test_graph_search__undirected_path_query(pool: PgPool) -> anyhow::Result<()> {
    let mut fixture = NidxFixture::new(pool).await?;
    let shard_id = setup_knowledge_graph(&mut fixture).await?;

    // (:Anna)-[:IS_FRIEND]-()
    let response = fixture
        .searcher_client
        .graph_search(GraphSearchRequest {
            shard: shard_id.clone(),
            query: Some(GraphQuery {
                path: Some(graph_query::PathQuery {
                    query: Some(path_query::Query::Path(graph_query::Path {
                        source: Some(graph_query::Node {
                            value: Some("Anna".to_string()),
                            node_type: Some(NodeType::Entity.into()),
                            node_subtype: Some("PERSON".to_string()),
                            ..Default::default()
                        }),
                        relation: Some(graph_query::Relation {
                            value: Some("IS_FRIEND".to_string()),
                            ..Default::default()
                        }),
                        destination: Some(graph_query::Node::default()),
                        undirected: true,
                    })),
                }),
            }),
            top_k: 100,
            ..Default::default()
        })
        .await?
        .into_inner();
    let relations = friendly_parse(&response);
    assert_eq!(relations.len(), 1);
    assert!(relations.contains(&("Anastasia", "IS_FRIEND", "Anna")));

    Ok(())
}

#[sqlx::test]
async fn test_graph_search_prefilter(pool: PgPool) -> anyhow::Result<()> {
    let mut fixture = NidxFixture::new(pool).await?;
    let shard_id = setup_knowledge_graph(&mut fixture).await?;
    let other_relations = people_and_places(shard_id.clone());
    let pap_id = other_relations.resource.as_ref().unwrap().uuid.clone();
    fixture.index_resource(&shard_id, other_relations).await?;
    fixture.wait_sync().await;

    // Search both resources
    let response = fixture
        .searcher_client
        .graph_search(GraphSearchRequest {
            shard: shard_id.clone(),
            query: Some(GraphQuery {
                path: Some(graph_query::PathQuery::default()),
            }),
            top_k: 100,
            ..Default::default()
        })
        .await?
        .into_inner();
    assert_eq!(response.relations.len(), 30);

    // Search people_and_places
    let response = fixture
        .searcher_client
        .graph_search(GraphSearchRequest {
            shard: shard_id.clone(),
            query: Some(GraphQuery {
                path: Some(graph_query::PathQuery::default()),
            }),
            field_filter: Some(FilterExpression {
                expr: Some(Expr::Resource(ResourceFilter {
                    resource_id: pap_id.clone(),
                })),
            }),
            top_k: 100,
            ..Default::default()
        })
        .await?
        .into_inner();
    assert_eq!(response.relations.len(), 13);

    // Search not people_and_places
    let response = fixture
        .searcher_client
        .graph_search(GraphSearchRequest {
            shard: shard_id.clone(),
            query: Some(GraphQuery {
                path: Some(graph_query::PathQuery::default()),
            }),
            field_filter: Some(FilterExpression {
                expr: Some(Expr::BoolNot(Box::new(FilterExpression {
                    expr: Some(Expr::Resource(ResourceFilter { resource_id: pap_id })),
                }))),
            }),
            top_k: 100,
            ..Default::default()
        })
        .await?
        .into_inner();
    assert_eq!(response.relations.len(), 17);

    Ok(())
}

async fn create_shard(fixture: &mut NidxFixture) -> anyhow::Result<String> {
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
    let shard_id = new_shard_response.into_inner().id;
    Ok(shard_id)
}
