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

use std::collections::{HashMap, HashSet};
use std::time::SystemTime;

use common::services::NidxFixture;
use nidx_protos::graph_query::node::MatchKind;
use nidx_protos::graph_query::path_query;
use nidx_protos::graph_search_request::QueryKind;
use nidx_protos::prost_types::Timestamp;
use nidx_protos::relation::RelationType;
use nidx_protos::relation_node::NodeType;
use nidx_protos::relation_prefix_search_request::Search;
use nidx_protos::resource::ResourceStatus;
use nidx_protos::{
    EntitiesSubgraphRequest, GraphQuery, GraphSearchRequest, IndexMetadata, NewShardRequest, Relation,
    RelationMetadata, RelationNode, RelationNodeFilter, RelationPrefixSearchRequest, RelationSearchRequest,
    RelationSearchResponse, Resource, ResourceId, graph_query,
};
use nidx_protos::{SearchRequest, VectorIndexConfig};
use nidx_tests::graph::friendly_parse;
use sqlx::PgPool;
use tonic::Request;
use uuid::Uuid;

#[sqlx::test]
async fn test_search_relations_by_prefix(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let mut fixture = NidxFixture::new(pool).await?;
    let shard_id = create_shard(&mut fixture).await?;
    create_knowledge_graph(&mut fixture, shard_id.clone()).await;
    fixture.wait_sync().await;

    // --------------------------------------------------------------
    // Test: prefixed search with empty term. Results are limited
    // --------------------------------------------------------------

    let response = relation_search(
        &mut fixture,
        RelationSearchRequest {
            shard_id: shard_id.clone(),
            prefix: Some(RelationPrefixSearchRequest {
                search: Some(Search::Prefix(String::new())),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await?;

    assert!(response.prefix.is_some());
    let prefix_response = response.prefix.as_ref().unwrap();
    let results = &prefix_response.nodes;
    // TODO this constants is spread between relations and paragraphs. It should
    // be in a single place and common for everyone
    const MAX_SUGGEST_RESULTS: usize = 10;
    assert_eq!(results.len(), MAX_SUGGEST_RESULTS);

    // --------------------------------------------------------------
    // Test: prefixed search with "cat" term (some results)
    // --------------------------------------------------------------

    let response = relation_search(
        &mut fixture,
        RelationSearchRequest {
            shard_id: shard_id.clone(),
            prefix: Some(RelationPrefixSearchRequest {
                search: Some(Search::Prefix("cat".to_string())),
                node_filters: vec![RelationNodeFilter {
                    node_subtype: None,
                    node_type: NodeType::Entity as i32,
                }],
            }),
            ..Default::default()
        },
    )
    .await?;

    let expected = HashSet::from_iter(["Cat".to_string(), "Catwoman".to_string(), "Batman".to_string()]);
    assert!(response.prefix.is_some());
    let prefix_response = response.prefix.as_ref().unwrap();
    let results = prefix_response
        .nodes
        .iter()
        .map(|node| node.value.to_owned())
        .collect::<HashSet<_>>();
    assert_eq!(results, expected);

    // --------------------------------------------------------------
    // Test: prefixed search with "cat" and filters
    // --------------------------------------------------------------

    let response = relation_search(
        &mut fixture,
        RelationSearchRequest {
            shard_id: shard_id.clone(),
            prefix: Some(RelationPrefixSearchRequest {
                search: Some(Search::Prefix("cat".to_string())),
                node_filters: vec![RelationNodeFilter {
                    node_subtype: Some("animal".to_string()),
                    node_type: NodeType::Entity as i32,
                }],
            }),
            ..Default::default()
        },
    )
    .await?;

    let expected = HashSet::from_iter(["Cat".to_string()]);
    assert!(response.prefix.is_some());
    let prefix_response = response.prefix.as_ref().unwrap();
    let results = prefix_response
        .nodes
        .iter()
        .map(|node| node.value.to_owned())
        .collect::<HashSet<_>>();
    assert_eq!(results, expected);

    let response = relation_search(
        &mut fixture,
        RelationSearchRequest {
            shard_id: shard_id.clone(),
            prefix: Some(RelationPrefixSearchRequest {
                search: Some(Search::Prefix("cat".to_string())),
                node_filters: vec![RelationNodeFilter {
                    node_subtype: Some("superhero".to_string()),
                    node_type: NodeType::Entity as i32,
                }],
            }),
            ..Default::default()
        },
    )
    .await?;

    let expected = HashSet::from_iter(["Catwoman".to_string()]);
    assert!(response.prefix.is_some());
    let prefix_response = response.prefix.as_ref().unwrap();
    let results = prefix_response
        .nodes
        .iter()
        .map(|node| node.value.to_owned())
        .collect::<HashSet<_>>();
    assert_eq!(results, expected);

    // --------------------------------------------------------------
    // Test: prefixed search with node filters and empty query
    // --------------------------------------------------------------

    let response = relation_search(
        &mut fixture,
        RelationSearchRequest {
            shard_id: shard_id.clone(),
            prefix: Some(RelationPrefixSearchRequest {
                search: Some(Search::Prefix(String::new())),
                node_filters: vec![RelationNodeFilter {
                    node_type: NodeType::Entity as i32,
                    node_subtype: Some("animal".to_string()),
                }],
            }),
            ..Default::default()
        },
    )
    .await?;

    let expected = HashSet::from_iter(["Cat".to_string()]);
    assert!(response.prefix.is_some());
    let prefix_response = response.prefix.as_ref().unwrap();
    let results = prefix_response
        .nodes
        .iter()
        .map(|node| node.value.to_owned())
        .collect::<HashSet<_>>();
    assert_eq!(results, expected);

    // --------------------------------------------------------------
    // Test: prefixed search with "zzz" term (empty results)
    // --------------------------------------------------------------

    let response = relation_search(
        &mut fixture,
        RelationSearchRequest {
            shard_id: shard_id.clone(),
            prefix: Some(RelationPrefixSearchRequest {
                search: Some(Search::Prefix("zzz".to_string())),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await?;

    assert!(response.prefix.is_some());
    let prefix_response = response.prefix.as_ref().unwrap();
    let results = &prefix_response.nodes;
    assert!(results.is_empty());

    Ok(())
}

#[sqlx::test]
async fn test_search_relations_neighbours(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let mut fixture = NidxFixture::new(pool).await?;
    let shard_id = create_shard(&mut fixture).await?;
    let (_, relation_nodes) = create_knowledge_graph(&mut fixture, shard_id.clone()).await;
    fixture.wait_sync().await;

    fn extract_relations(response: RelationSearchResponse) -> HashSet<(String, String)> {
        response
            .subgraph
            .iter()
            .flat_map(|neighbours| neighbours.relations.iter())
            .flat_map(|node| {
                [(
                    node.source.as_ref().unwrap().value.to_owned(),
                    node.to.as_ref().unwrap().value.to_owned(),
                )]
            })
            .collect::<HashSet<_>>()
    }

    // --------------------------------------------------------------
    // Test: neighbours search on existent node
    // --------------------------------------------------------------

    let response = relation_search(
        &mut fixture,
        RelationSearchRequest {
            shard_id: shard_id.clone(),
            subgraph: Some(EntitiesSubgraphRequest {
                entry_points: vec![relation_nodes.get("Swallow").unwrap().clone()],
                depth: Some(1),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await?;

    let expected = HashSet::from_iter([
        ("Poetry".to_string(), "Swallow".to_string()),
        ("Swallow".to_string(), "Animal".to_string()),
        ("Swallow".to_string(), "Fly".to_string()),
    ]);
    let neighbour_relations = extract_relations(response);
    assert_eq!(neighbour_relations, expected);

    // --------------------------------------------------------------
    // Test: neighbours search on multiple existent nodes
    // --------------------------------------------------------------

    let response = relation_search(
        &mut fixture,
        RelationSearchRequest {
            shard_id: shard_id.clone(),
            subgraph: Some(EntitiesSubgraphRequest {
                entry_points: vec![
                    relation_nodes.get("Becquer").unwrap().clone(),
                    relation_nodes.get("Newton").unwrap().clone(),
                ],
                depth: Some(1),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await?;

    let expected = HashSet::from_iter([
        ("Newton".to_string(), "Physics".to_string()),
        ("Newton".to_string(), "Gravity".to_string()),
        ("Becquer".to_string(), "Poetry".to_string()),
        ("Joan Antoni".to_string(), "Becquer".to_string()),
    ]);
    let neighbour_relations = extract_relations(response);
    assert_eq!(neighbour_relations, expected);

    // --------------------------------------------------------------
    // Test: neighbours search on non existent node
    // --------------------------------------------------------------

    let response = relation_search(
        &mut fixture,
        RelationSearchRequest {
            shard_id: shard_id.clone(),
            subgraph: Some(EntitiesSubgraphRequest {
                entry_points: vec![RelationNode {
                    value: "Fake".to_string(),
                    ntype: NodeType::Entity as i32,
                    subtype: String::new(),
                }],
                depth: Some(1),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await?;

    let neighbours = extract_relations(response);
    assert!(neighbours.is_empty());

    // --------------------------------------------------------------
    // Test: neighbours search with filters
    // --------------------------------------------------------------

    let response = relation_search(
        &mut fixture,
        RelationSearchRequest {
            shard_id: shard_id.clone(),
            subgraph: Some(EntitiesSubgraphRequest {
                entry_points: vec![relation_nodes.get("Poetry").unwrap().clone()],
                depth: Some(1),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await?;

    let expected = HashSet::from_iter([("Poetry".to_string(), "Swallow".to_string())]);

    // Check that the relation obtained as response has appropiate metadata
    let subgraph = response.subgraph.clone().unwrap();
    let first_relation = &subgraph.relations[0];
    let metadata = first_relation.metadata.as_ref().unwrap();

    assert_eq!(
        metadata.paragraph_id,
        Some("myresource/0/myresource/100-200".to_string())
    );
    assert_eq!(metadata.source_start, Some(0));
    assert_eq!(metadata.source_end, Some(10));
    assert_eq!(metadata.to_start, Some(11));
    assert_eq!(metadata.to_end, Some(20));
    assert_eq!(metadata.data_augmentation_task_id, Some("mytask".to_string()));

    let neighbour_relations = extract_relations(response);
    assert!(expected.is_subset(&neighbour_relations));

    Ok(())
}

// NOTE: The following tests are closely related with nidx_relations/tests/test_graph_search.rs.
// They aim to test the same queries on the same graph to ensure protobuf query is equally capable

async fn setup_knowledge_graph(fixture: &mut NidxFixture) -> anyhow::Result<String> {
    let shard_id = create_shard(fixture).await?;

    // Create resource with graph
    //
    // FIXME: we should use the same graph in all tests to simplify mental overhead
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
                    uuid: rid.to_string(),
                }),
                status: ResourceStatus::Processed as i32,
                relations: knowledge_graph,
                metadata: Some(IndexMetadata {
                    created: Some(timestamp),
                    modified: Some(timestamp),
                }),
                texts: HashMap::new(),
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
    assert_eq!(nodes.len(), 17);

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
    assert_eq!(relations.len(), 16);

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
    let response = fixture
        .searcher_client
        .graph_search(GraphSearchRequest {
            shard: shard_id.clone(),
            query: Some(GraphQuery {
                path: Some(graph_query::PathQuery {
                    query: Some(path_query::Query::Path(graph_query::Path {
                        source: Some(graph_query::Node {
                            value: Some("Anastas".to_string()),
                            match_kind: MatchKind::Fuzzy.into(),
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
    let response = fixture
        .searcher_client
        .graph_search(GraphSearchRequest {
            shard: shard_id.clone(),
            query: Some(GraphQuery {
                path: Some(graph_query::PathQuery {
                    query: Some(path_query::Query::Path(graph_query::Path {
                        source: Some(graph_query::Node {
                            value: Some("AnXstXsiX".to_string()),
                            match_kind: MatchKind::Fuzzy.into(),
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
    let response = fixture
        .searcher_client
        .graph_search(GraphSearchRequest {
            shard: shard_id.clone(),
            query: Some(GraphQuery {
                path: Some(graph_query::PathQuery {
                    query: Some(path_query::Query::Path(graph_query::Path {
                        source: Some(graph_query::Node {
                            value: Some("Ansatasia".to_string()),
                            match_kind: MatchKind::Fuzzy.into(),
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

async fn create_knowledge_graph(fixture: &mut NidxFixture, shard_id: String) -> (Uuid, HashMap<String, RelationNode>) {
    let rid = Uuid::new_v4();
    let string_rid = rid.to_string();

    let entities = HashMap::from([
        (string_rid.as_str(), (NodeType::Resource, "".to_string())),
        ("Animal", (NodeType::Entity, "".to_string())),
        ("Batman", (NodeType::Entity, "".to_string())),
        ("Becquer", (NodeType::Entity, "".to_string())),
        ("Cat", (NodeType::Entity, "animal".to_string())),
        ("Catwoman", (NodeType::Entity, "superhero".to_string())),
        ("Eric", (NodeType::Entity, "".to_string())),
        ("Fly", (NodeType::Entity, "".to_string())),
        ("Gravity", (NodeType::Entity, "".to_string())),
        ("Joan Antoni", (NodeType::Entity, "".to_string())),
        ("Joker", (NodeType::Entity, "".to_string())),
        ("Newton", (NodeType::Entity, "".to_string())),
        ("Physics", (NodeType::Entity, "".to_string())),
        ("Poetry", (NodeType::Entity, "".to_string())),
        ("Swallow", (NodeType::Entity, "".to_string())),
    ]);

    let relations = HashMap::from([
        ("about", RelationType::About),
        ("can", RelationType::Entity),
        ("collaborate", RelationType::Entity),
        ("defy", RelationType::Entity),
        ("enjoy", RelationType::Entity),
        ("fight", RelationType::Entity),
        ("formulate", RelationType::Entity),
        ("imitate", RelationType::Entity),
        ("like", RelationType::Entity),
        ("love", RelationType::Entity),
        ("read", RelationType::Entity),
        ("species", RelationType::Entity),
        ("study", RelationType::Entity),
        ("subject", RelationType::Other),
        ("write", RelationType::Entity),
    ]);

    let graph = vec![
        (string_rid.as_str(), "subject", "Poetry", None),
        ("Batman", "fight", "Joker", None),
        ("Batman", "love", "Catwoman", None),
        ("Becquer", "like", "Poetry", None),
        ("Becquer", "write", "Poetry", None),
        ("Cat", "species", "Animal", None),
        ("Catwoman", "imitate", "Cat", None),
        ("Eric", "collaborate", "Joan Antoni", None),
        ("Eric", "like", "Cat", None),
        ("Fly", "defy", "Gravity", None),
        ("Joan Antoni", "collaborate", "Eric", None),
        ("Joan Antoni", "read", "Becquer", None),
        ("Joker", "enjoy", "Physics", None),
        ("Newton", "formulate", "Gravity", None),
        ("Newton", "study", "Physics", None),
        (
            "Poetry",
            "about",
            "Swallow",
            Some(RelationMetadata {
                paragraph_id: Some("myresource/0/myresource/100-200".to_string()),
                source_start: Some(0),
                source_end: Some(10),
                to_start: Some(11),
                to_end: Some(20),
                data_augmentation_task_id: Some("mytask".to_string()),
            }),
        ),
        ("Swallow", "can", "Fly", None),
        ("Swallow", "species", "Animal", None),
    ];

    let mut relation_edges = vec![];
    for (source, relation, destination, relation_metadata) in graph {
        let (source_type, source_subtype) = entities.get(source).unwrap();
        let relation_type = relations
            .get(relation)
            .expect(&format!("Expecting to find relation '{relation}'"));
        let (destination_type, destination_subtype) = entities.get(destination).unwrap();

        relation_edges.push(Relation {
            source: Some(RelationNode {
                value: source.to_string(),
                ntype: *source_type as i32,
                subtype: source_subtype.clone(),
            }),
            to: Some(RelationNode {
                value: destination.to_string(),
                ntype: *destination_type as i32,
                subtype: destination_subtype.clone(),
            }),
            relation: *relation_type as i32,
            relation_label: relation.to_string(),
            metadata: relation_metadata.clone(),
            resource_id: None,
        });
    }

    let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap();
    let timestamp = Timestamp {
        seconds: now.as_secs() as i64,
        nanos: 0,
    };

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
                relations: relation_edges.clone(),
                metadata: Some(IndexMetadata {
                    created: Some(timestamp),
                    modified: Some(timestamp),
                }),
                texts: HashMap::new(),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let relation_nodes = HashMap::from_iter(entities.iter().map(|(node, (node_type, node_subtype))| {
        (
            node.to_string(),
            RelationNode {
                value: node.to_string(),
                ntype: *node_type as i32,
                subtype: node_subtype.to_string(),
            },
        )
    }));

    (rid, relation_nodes)
}

async fn relation_search(
    fixture: &mut NidxFixture,
    request: RelationSearchRequest,
) -> anyhow::Result<RelationSearchResponse> {
    let request = SearchRequest {
        shard: request.shard_id,
        vectorset: "english".to_string(),
        relation_prefix: request.prefix,
        relation_subgraph: request.subgraph,
        ..Default::default()
    };
    let response = fixture.searcher_client.search(request).await?;
    Ok(response.into_inner().relation.unwrap())
}
