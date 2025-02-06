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
use nidx_protos::prost_types::Timestamp;
use nidx_protos::relation::RelationType;
use nidx_protos::relation_node::NodeType;
use nidx_protos::relation_prefix_search_request::Search;
use nidx_protos::resource::ResourceStatus;
use nidx_protos::{
    EntitiesSubgraphRequest, IndexMetadata, NewShardRequest, Relation, RelationMetadata, RelationNode,
    RelationNodeFilter, RelationPrefixSearchRequest, RelationSearchRequest, RelationSearchResponse, Resource,
    ResourceId,
};
use nidx_protos::{SearchRequest, VectorIndexConfig};
use sqlx::PgPool;
use tonic::Request;
use uuid::Uuid;

async fn create_knowledge_graph(fixture: &mut NidxFixture, shard_id: String) -> HashMap<String, RelationNode> {
    let rid = Uuid::new_v4();

    let mut relation_nodes = HashMap::new();
    relation_nodes.insert(
        rid.to_string(),
        RelationNode {
            value: rid.to_string(),
            ntype: NodeType::Resource as i32,
            subtype: String::new(),
        },
    );
    relation_nodes.insert(
        "Animal".to_string(),
        RelationNode {
            value: "Animal".to_string(),
            ntype: NodeType::Entity as i32,
            subtype: String::new(),
        },
    );
    relation_nodes.insert(
        "Batman".to_string(),
        RelationNode {
            value: "Batman".to_string(),
            ntype: NodeType::Entity as i32,
            subtype: String::new(),
        },
    );
    relation_nodes.insert(
        "Becquer".to_string(),
        RelationNode {
            value: "Becquer".to_string(),
            ntype: NodeType::Entity as i32,
            subtype: String::new(),
        },
    );
    relation_nodes.insert(
        "Cat".to_string(),
        RelationNode {
            value: "Cat".to_string(),
            ntype: NodeType::Entity as i32,
            subtype: "animal".to_string(),
        },
    );
    relation_nodes.insert(
        "Catwoman".to_string(),
        RelationNode {
            value: "Catwoman".to_string(),
            ntype: NodeType::Entity as i32,
            subtype: "superhero".to_string(),
        },
    );
    relation_nodes.insert(
        "Eric".to_string(),
        RelationNode {
            value: "Eric".to_string(),
            ntype: NodeType::Entity as i32,
            subtype: String::new(),
        },
    );
    relation_nodes.insert(
        "Fly".to_string(),
        RelationNode {
            value: "Fly".to_string(),
            ntype: NodeType::Entity as i32,
            subtype: String::new(),
        },
    );
    relation_nodes.insert(
        "Gravity".to_string(),
        RelationNode {
            value: "Gravity".to_string(),
            ntype: NodeType::Entity as i32,
            subtype: String::new(),
        },
    );
    relation_nodes.insert(
        "Joan Antoni".to_string(),
        RelationNode {
            value: "Joan Antoni".to_string(),
            ntype: NodeType::Entity as i32,
            subtype: String::new(),
        },
    );
    relation_nodes.insert(
        "Joker".to_string(),
        RelationNode {
            value: "Joker".to_string(),
            ntype: NodeType::Entity as i32,
            subtype: String::new(),
        },
    );
    relation_nodes.insert(
        "Newton".to_string(),
        RelationNode {
            value: "Newton".to_string(),
            ntype: NodeType::Entity as i32,
            subtype: String::new(),
        },
    );
    relation_nodes.insert(
        "Physics".to_string(),
        RelationNode {
            value: "Physics".to_string(),
            ntype: NodeType::Entity as i32,
            subtype: String::new(),
        },
    );
    relation_nodes.insert(
        "Poetry".to_string(),
        RelationNode {
            value: "Poetry".to_string(),
            ntype: NodeType::Entity as i32,
            subtype: String::new(),
        },
    );
    relation_nodes.insert(
        "Swallow".to_string(),
        RelationNode {
            value: "Swallow".to_string(),
            ntype: NodeType::Entity as i32,
            subtype: String::new(),
        },
    );

    let relation_edges = vec![
        Relation {
            relation: RelationType::Entity as i32,
            source: Some(relation_nodes.get("Batman").unwrap().clone()),
            to: Some(relation_nodes.get("Catwoman").unwrap().clone()),
            relation_label: "love".to_string(),
            ..Default::default()
        },
        Relation {
            relation: RelationType::Entity as i32,
            source: Some(relation_nodes.get("Batman").unwrap().clone()),
            to: Some(relation_nodes.get("Joker").unwrap().clone()),
            relation_label: "fight".to_string(),
            ..Default::default()
        },
        Relation {
            relation: RelationType::Entity as i32,
            source: Some(relation_nodes.get("Joker").unwrap().clone()),
            to: Some(relation_nodes.get("Physics").unwrap().clone()),
            relation_label: "enjoy".to_string(),
            ..Default::default()
        },
        Relation {
            relation: RelationType::Entity as i32,
            source: Some(relation_nodes.get("Catwoman").unwrap().clone()),
            to: Some(relation_nodes.get("Cat").unwrap().clone()),
            relation_label: "imitate".to_string(),
            ..Default::default()
        },
        Relation {
            relation: RelationType::Entity as i32,
            source: Some(relation_nodes.get("Cat").unwrap().clone()),
            to: Some(relation_nodes.get("Animal").unwrap().clone()),
            relation_label: "species".to_string(),
            ..Default::default()
        },
        Relation {
            relation: RelationType::Entity as i32,
            source: Some(relation_nodes.get("Newton").unwrap().clone()),
            to: Some(relation_nodes.get("Physics").unwrap().clone()),
            relation_label: "study".to_string(),
            ..Default::default()
        },
        Relation {
            relation: RelationType::Entity as i32,
            source: Some(relation_nodes.get("Newton").unwrap().clone()),
            to: Some(relation_nodes.get("Gravity").unwrap().clone()),
            relation_label: "formulate".to_string(),
            ..Default::default()
        },
        Relation {
            relation: RelationType::Entity as i32,
            source: Some(relation_nodes.get("Eric").unwrap().clone()),
            to: Some(relation_nodes.get("Cat").unwrap().clone()),
            relation_label: "like".to_string(),
            ..Default::default()
        },
        Relation {
            relation: RelationType::Entity as i32,
            source: Some(relation_nodes.get("Eric").unwrap().clone()),
            to: Some(relation_nodes.get("Joan Antoni").unwrap().clone()),
            relation_label: "collaborate".to_string(),
            ..Default::default()
        },
        Relation {
            relation: RelationType::Entity as i32,
            source: Some(relation_nodes.get("Joan Antoni").unwrap().clone()),
            to: Some(relation_nodes.get("Eric").unwrap().clone()),
            relation_label: "collaborate".to_string(),
            ..Default::default()
        },
        Relation {
            relation: RelationType::Entity as i32,
            source: Some(relation_nodes.get("Joan Antoni").unwrap().clone()),
            to: Some(relation_nodes.get("Becquer").unwrap().clone()),
            relation_label: "read".to_string(),
            ..Default::default()
        },
        Relation {
            relation: RelationType::Entity as i32,
            source: Some(relation_nodes.get("Becquer").unwrap().clone()),
            to: Some(relation_nodes.get("Poetry").unwrap().clone()),
            relation_label: "write".to_string(),
            ..Default::default()
        },
        Relation {
            relation: RelationType::Entity as i32,
            source: Some(relation_nodes.get("Becquer").unwrap().clone()),
            to: Some(relation_nodes.get("Poetry").unwrap().clone()),
            relation_label: "like".to_string(),
            ..Default::default()
        },
        Relation {
            relation: RelationType::About as i32,
            source: Some(relation_nodes.get("Poetry").unwrap().clone()),
            to: Some(relation_nodes.get("Swallow").unwrap().clone()),
            relation_label: "about".to_string(),
            metadata: Some(RelationMetadata {
                paragraph_id: Some("myresource/0/myresource/100-200".to_string()),
                source_start: Some(0),
                source_end: Some(10),
                to_start: Some(11),
                to_end: Some(20),
                data_augmentation_task_id: Some("mytask".to_string()),
            }),
        },
        Relation {
            relation: RelationType::Other as i32,
            source: Some(relation_nodes.get(&rid.to_string()).unwrap().clone()),
            to: Some(relation_nodes.get("Poetry").unwrap().clone()),
            relation_label: "subject".to_string(),
            ..Default::default()
        },
        Relation {
            relation: RelationType::Entity as i32,
            source: Some(relation_nodes.get("Swallow").unwrap().clone()),
            to: Some(relation_nodes.get("Animal").unwrap().clone()),
            relation_label: "species".to_string(),
            ..Default::default()
        },
        Relation {
            relation: RelationType::Entity as i32,
            source: Some(relation_nodes.get("Swallow").unwrap().clone()),
            to: Some(relation_nodes.get("Fly").unwrap().clone()),
            relation_label: "can".to_string(),
            ..Default::default()
        },
        Relation {
            relation: RelationType::Entity as i32,
            source: Some(relation_nodes.get("Fly").unwrap().clone()),
            to: Some(relation_nodes.get("Gravity").unwrap().clone()),
            relation_label: "defy".to_string(),
            ..Default::default()
        },
    ];

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

    relation_nodes
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

#[sqlx::test]
async fn test_search_relations_prefixed(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
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
    let results = prefix_response.nodes.iter().map(|node| node.value.to_owned()).collect::<HashSet<_>>();
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
    let results = prefix_response.nodes.iter().map(|node| node.value.to_owned()).collect::<HashSet<_>>();
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
    let results = prefix_response.nodes.iter().map(|node| node.value.to_owned()).collect::<HashSet<_>>();
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
    let results = prefix_response.nodes.iter().map(|node| node.value.to_owned()).collect::<HashSet<_>>();
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

    let relation_nodes = create_knowledge_graph(&mut fixture, shard_id.clone()).await;
    fixture.wait_sync().await;

    fn extract_relations(response: RelationSearchResponse) -> HashSet<(String, String)> {
        response
            .subgraph
            .iter()
            .flat_map(|neighbours| neighbours.relations.iter())
            .flat_map(|node| {
                [(node.source.as_ref().unwrap().value.to_owned(), node.to.as_ref().unwrap().value.to_owned())]
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

    assert_eq!(metadata.paragraph_id, Some("myresource/0/myresource/100-200".to_string()));
    assert_eq!(metadata.source_start, Some(0));
    assert_eq!(metadata.source_end, Some(10));
    assert_eq!(metadata.to_start, Some(11));
    assert_eq!(metadata.to_end, Some(20));
    assert_eq!(metadata.data_augmentation_task_id, Some("mytask".to_string()));

    let neighbour_relations = extract_relations(response);
    assert!(expected.is_subset(&neighbour_relations));

    Ok(())
}
