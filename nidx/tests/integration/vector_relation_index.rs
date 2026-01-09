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

use nidx_protos::graph_query::{Node, Path};
use nidx_protos::graph_search_request::QueryKind;
use nidx_protos::relation::RelationType;
use nidx_protos::relation_node::NodeType;
use nidx_protos::{
    GraphSearchRequest, IndexRelation, IndexRelations, Relation, RelationEdgeVector, RelationNode, RelationNodeVector,
    Resource, SearchRequest,
};

use uuid::Uuid;

use nidx::api::shards;

use nidx::NidxMetadata;
use nidx_tests::*;
use nidx_vector::config::{VectorConfig, VectorType};

use crate::common::services::NidxFixture;

fn relation(from: &str, relation: &str, to: &str) -> IndexRelation {
    IndexRelation {
        relation: Some(Relation {
            source: Some(RelationNode {
                value: from.into(),
                ntype: NodeType::Entity as i32,
                subtype: "animal".into(),
            }),
            to: Some(RelationNode {
                value: to.into(),
                ntype: NodeType::Entity as i32,
                subtype: "animal".into(),
            }),
            relation: RelationType::Entity as i32,
            relation_label: relation.into(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn node_vector(value: &str, vector: Vec<f32>) -> RelationNodeVector {
    RelationNodeVector {
        node: Some(RelationNode {
            value: value.into(),
            ntype: NodeType::Entity as i32,
            subtype: "animal".into(),
        }),
        vector,
    }
}

fn relation_vector(value: &str, vector: Vec<f32>) -> RelationEdgeVector {
    RelationEdgeVector {
        vector,
        relation_type: RelationType::Entity as i32,
        relation_label: value.to_string(),
    }
}

fn resource() -> Resource {
    Resource {
        field_relations: [(
            "f/file".into(),
            IndexRelations {
                relations: vec![
                    relation("dog", "bigger than", "fish"),
                    relation("fish", "faster than", "snail"),
                    relation("lion", "bigger than", "dog"),
                    relation("lion", "eats", "fish"),
                ],
            },
        )]
        .into(),
        relation_node_vectors: vec![
            node_vector("dog", vec![0.7, 0.7, 0.0, 0.0]),
            node_vector("fish", vec![0.0, 0.0, 0.7, 0.7]),
            node_vector("snail", vec![0.0, 0.7, 0.7, 0.0]),
            node_vector("lion", vec![0.58, 0.58, 0.0, 0.58]),
        ],
        relation_edge_vectors: vec![
            relation_vector("bigger than", vec![0.6, 0.6, 0.2, 0.0]),
            relation_vector("faster than", vec![0.7, 0.7, 0.0, 0.0]),
            relation_vector("eats", vec![0.0, 0.0, 0.8, 0.3]),
        ],
        ..minimal_resource("shard".into())
    }
}

async fn shard_with_resource(pool: sqlx::PgPool) -> anyhow::Result<(NidxFixture, String)> {
    let meta = NidxMetadata::new_with_pool(pool.clone()).await?;

    let kbid = Uuid::new_v4();
    let vector_configs = vec![
        (
            "minivectors".to_string(),
            VectorConfig::for_relation_nodes(VectorType::DenseF32 { dimension: 4 }),
        ),
        (
            "minivectors".to_string(),
            VectorConfig::for_relation_edges(VectorType::DenseF32 { dimension: 4 }),
        ),
    ];

    // TODO: Skipping the fixture because it cannot create relation node indexes yet
    let shard = shards::create_shard(&meta, kbid, vector_configs).await?;

    let mut fixture = NidxFixture::new(pool).await?;

    // Index a resource with relations
    let resource = resource();
    fixture.index_resource(&shard.id.to_string(), resource.clone()).await?;
    fixture.wait_sync().await;

    Ok((fixture, shard.id.to_string()))
}

#[sqlx::test]
async fn test_relation_node_search(pool: sqlx::PgPool) -> anyhow::Result<()> {
    let (mut fixture, shard_id) = shard_with_resource(pool).await?;

    let graph_query = nidx_protos::GraphQuery {
        path: Some(nidx_protos::graph_query::PathQuery {
            query: Some(nidx_protos::graph_query::path_query::Query::Path(Path {
                source: Some(Node {
                    value: Some("dog".to_string()),
                    match_kind: Some(nidx_protos::graph_query::node::MatchKind::Vector(
                        nidx_protos::graph_query::node::VectorMatch {
                            vector: vec![0.6, 0.8, 0.0, 0.0],
                        },
                    )),
                    ..Default::default()
                }),
                undirected: true,
                ..Default::default()
            })),
        }),
    };

    let results = fixture
        .searcher_client
        .graph_search(GraphSearchRequest {
            shard: shard_id,
            query: Some(graph_query.clone()),
            kind: QueryKind::Nodes as i32,
            top_k: 100,
            graph_vectorset: Some("minivectors".to_string()),
            ..Default::default()
        })
        .await?
        .into_inner();

    // Expect dog & lion
    assert_eq!(results.nodes.len(), 2);
    assert_eq!(results.nodes[0].value, "dog");
    assert_eq!(results.nodes[1].value, "lion");

    assert_eq!(results.scores.len(), 2);
    assert!(results.scores[0] > results.scores[1]);

    Ok(())
}

#[sqlx::test]
async fn test_relation_path_search(pool: sqlx::PgPool) -> anyhow::Result<()> {
    let (mut fixture, shard_id) = shard_with_resource(pool).await?;

    let graph_query = nidx_protos::GraphQuery {
        path: Some(nidx_protos::graph_query::PathQuery {
            query: Some(nidx_protos::graph_query::path_query::Query::Path(Path {
                source: Some(Node {
                    value: Some("dog".to_string()),
                    match_kind: Some(nidx_protos::graph_query::node::MatchKind::Vector(
                        nidx_protos::graph_query::node::VectorMatch {
                            vector: vec![0.6, 0.8, 0.0, 0.0],
                        },
                    )),
                    ..Default::default()
                }),
                undirected: true,
                ..Default::default()
            })),
        }),
    };

    // Use graph path search
    let results = fixture
        .searcher_client
        .graph_search(GraphSearchRequest {
            shard: shard_id.clone(),
            query: Some(graph_query.clone()),
            kind: QueryKind::Path as i32,
            top_k: 100,
            graph_vectorset: Some("minivectors".to_string()),
            ..Default::default()
        })
        .await?
        .into_inner();

    assert_eq!(results.graph.len(), 3);
    let relations: Vec<_> = results
        .graph
        .iter()
        .map(|p| {
            (
                results.nodes[p.source as usize].value.as_str(),
                results.relations[p.relation as usize].label.as_str(),
                results.nodes[p.destination as usize].value.as_str(),
            )
        })
        .collect();

    assert_eq!(
        relations,
        vec![
            ("lion", "bigger than", "dog"),
            ("dog", "bigger than", "fish"),
            ("lion", "eats", "fish")
        ]
    );

    assert!(results.scores[0] > 1.5); // Matches both sides, big score
    assert!(results.scores[0] > results.scores[1]);
    assert!(results.scores[1] > results.scores[2]);

    // Check paragraph search with graph results
    let results = fixture
        .searcher_client
        .search(SearchRequest {
            shard: shard_id,
            graph_search: Some(nidx_protos::search_request::GraphSearch {
                query: Some(graph_query),
            }),
            graph_vectorset: Some("minivectors".to_string()),
            ..Default::default()
        })
        .await?
        .into_inner();

    // Expect dog & lion
    let graph_results = results.graph.unwrap();
    assert_eq!(graph_results.graph.len(), 3);
    let relations: Vec<_> = graph_results
        .graph
        .iter()
        .map(|p| {
            (
                graph_results.nodes[p.source as usize].value.as_str(),
                graph_results.relations[p.relation as usize].label.as_str(),
                graph_results.nodes[p.destination as usize].value.as_str(),
            )
        })
        .collect();

    assert_eq!(
        relations,
        vec![
            ("lion", "bigger than", "dog"),
            ("dog", "bigger than", "fish"),
            ("lion", "eats", "fish")
        ]
    );

    assert!(graph_results.scores[0] > 1.5); // Matches both sides, big score
    assert!(graph_results.scores[0] > graph_results.scores[1]);
    assert!(graph_results.scores[1] > graph_results.scores[2]);

    Ok(())
}

#[sqlx::test]
async fn test_relation_edge_search(pool: sqlx::PgPool) -> anyhow::Result<()> {
    let (mut fixture, shard_id) = shard_with_resource(pool).await?;

    let graph_query = nidx_protos::GraphQuery {
        path: Some(nidx_protos::graph_query::PathQuery {
            query: Some(nidx_protos::graph_query::path_query::Query::Path(Path {
                relation: Some(nidx_protos::graph_query::Relation {
                    value: Some("comparison".to_string()),
                    match_kind: Some(nidx_protos::graph_query::relation::MatchKind::Vector(
                        nidx_protos::graph_query::relation::VectorMatch {
                            vector: vec![0.6, 0.8, 0.0, 0.0],
                        },
                    )),
                    ..Default::default()
                }),
                undirected: true,
                ..Default::default()
            })),
        }),
    };

    let results = fixture
        .searcher_client
        .graph_search(GraphSearchRequest {
            shard: shard_id,
            query: Some(graph_query.clone()),
            kind: QueryKind::Relations as i32,
            top_k: 100,
            graph_vectorset: Some("minivectors".to_string()),
            ..Default::default()
        })
        .await?
        .into_inner();

    // Expect bigger than & faster than
    assert_eq!(results.relations.len(), 2);
    assert_eq!(results.relations[0].label, "faster than");
    assert_eq!(results.relations[1].label, "bigger than");

    assert_eq!(results.scores.len(), 2);
    assert!(results.scores[0] > results.scores[1]);

    Ok(())
}
