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

use common::{node_services, TestNodeWriter};
use nucliadb_core::protos::op_status::Status;
use nucliadb_core::protos::prost_types::Timestamp;
use nucliadb_core::protos::relation::RelationType;
use nucliadb_core::protos::relation_node::NodeType;
use nucliadb_core::protos::resource::ResourceStatus;
use nucliadb_core::protos::{
    EmptyQuery, EntitiesSubgraphRequest, IndexMetadata, Relation, RelationEdgeFilter, RelationNode,
    RelationNodeFilter, RelationPrefixSearchRequest, RelationSearchRequest, RelationSearchResponse,
    Resource, ResourceId,
};
use tonic::Request;
use uuid::Uuid;

async fn create_knowledge_graph(
    writer: &mut TestNodeWriter,
    shard_id: String,
) -> HashMap<String, RelationNode> {
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
            subtype: "".to_string(),
        },
    );
    relation_nodes.insert(
        "Batman".to_string(),
        RelationNode {
            value: "Batman".to_string(),
            ntype: NodeType::Entity as i32,
            subtype: "".to_string(),
        },
    );
    relation_nodes.insert(
        "Becquer".to_string(),
        RelationNode {
            value: "Becquer".to_string(),
            ntype: NodeType::Entity as i32,
            subtype: "".to_string(),
        },
    );
    relation_nodes.insert(
        "Cat".to_string(),
        RelationNode {
            value: "Cat".to_string(),
            ntype: NodeType::Entity as i32,
            subtype: "".to_string(),
        },
    );
    relation_nodes.insert(
        "Catwoman".to_string(),
        RelationNode {
            value: "Catwoman".to_string(),
            ntype: NodeType::Entity as i32,
            subtype: "".to_string(),
        },
    );
    relation_nodes.insert(
        "Eric".to_string(),
        RelationNode {
            value: "Eric".to_string(),
            ntype: NodeType::Entity as i32,
            subtype: "".to_string(),
        },
    );
    relation_nodes.insert(
        "Fly".to_string(),
        RelationNode {
            value: "Fly".to_string(),
            ntype: NodeType::Entity as i32,
            subtype: "".to_string(),
        },
    );
    relation_nodes.insert(
        "Gravity".to_string(),
        RelationNode {
            value: "Gravity".to_string(),
            ntype: NodeType::Entity as i32,
            subtype: "".to_string(),
        },
    );
    relation_nodes.insert(
        "Joan Antoni".to_string(),
        RelationNode {
            value: "Joan Antoni".to_string(),
            ntype: NodeType::Entity as i32,
            subtype: "".to_string(),
        },
    );
    relation_nodes.insert(
        "Joker".to_string(),
        RelationNode {
            value: "Joker".to_string(),
            ntype: NodeType::Entity as i32,
            subtype: "".to_string(),
        },
    );
    relation_nodes.insert(
        "Newton".to_string(),
        RelationNode {
            value: "Newton".to_string(),
            ntype: NodeType::Entity as i32,
            subtype: "".to_string(),
        },
    );
    relation_nodes.insert(
        "Physics".to_string(),
        RelationNode {
            value: "Physics".to_string(),
            ntype: NodeType::Entity as i32,
            subtype: "".to_string(),
        },
    );
    relation_nodes.insert(
        "Poetry".to_string(),
        RelationNode {
            value: "Poetry".to_string(),
            ntype: NodeType::Entity as i32,
            subtype: "".to_string(),
        },
    );
    relation_nodes.insert(
        "Swallow".to_string(),
        RelationNode {
            value: "Swallow".to_string(),
            ntype: NodeType::Entity as i32,
            subtype: "".to_string(),
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
            ..Default::default()
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

    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();
    let timestamp = Timestamp {
        seconds: now.as_secs() as i64,
        nanos: 0,
    };

    let r = writer
        .set_resource(Resource {
            shard_id: shard_id.clone(),
            resource: Some(ResourceId {
                shard_id: shard_id.clone(),
                uuid: rid.to_string(),
            }),
            status: ResourceStatus::Processed as i32,
            relations: relation_edges.clone(),
            metadata: Some(IndexMetadata {
                created: Some(timestamp.clone()),
                modified: Some(timestamp),
            }),
            texts: HashMap::new(),
            ..Default::default()
        })
        .await
        .unwrap();

    assert_eq!(r.get_ref().status(), Status::Ok);

    relation_nodes
}

#[tokio::test]
async fn test_search_relations_prefixed() -> Result<(), Box<dyn std::error::Error>> {
    let (mut reader, mut writer) = node_services().await;

    let new_shard_response = writer.new_shard(Request::new(EmptyQuery {})).await?;
    let shard_id = &new_shard_response.get_ref().id;

    create_knowledge_graph(&mut writer, shard_id.clone()).await;

    // --------------------------------------------------------------
    // Test: prefixed search with empty term. Results are limited
    // --------------------------------------------------------------

    let response = reader
        .relation_search(RelationSearchRequest {
            shard_id: shard_id.clone(),
            prefix: Some(RelationPrefixSearchRequest {
                prefix: "".to_string(),
            }),
            ..Default::default()
        })
        .await?;

    assert!(response.get_ref().prefix.is_some());
    let prefix_response = response.get_ref().prefix.as_ref().unwrap();
    let results = &prefix_response.nodes;
    // TODO: get constants from RelationsReaderService (.../relations/service/reader.rs)
    assert_eq!(results.len(), 10);

    // --------------------------------------------------------------
    // Test: prefixed search with "cat" term (some results)
    // --------------------------------------------------------------

    let response = reader
        .relation_search(RelationSearchRequest {
            shard_id: shard_id.clone(),
            prefix: Some(RelationPrefixSearchRequest {
                prefix: "cat".to_string(),
            }),
            ..Default::default()
        })
        .await?;

    let expected = HashSet::from_iter(vec!["Cat".to_string(), "Catwoman".to_string()]);
    assert!(response.get_ref().prefix.is_some());
    let prefix_response = response.get_ref().prefix.as_ref().unwrap();
    let results = prefix_response
        .nodes
        .iter()
        .map(|node| node.value.to_owned())
        .collect::<HashSet<_>>();
    assert_eq!(results, expected);

    // --------------------------------------------------------------
    // Test: prefixed search with "zzz" term (empty results)
    // --------------------------------------------------------------

    let response = reader
        .relation_search(RelationSearchRequest {
            shard_id: shard_id.clone(),
            prefix: Some(RelationPrefixSearchRequest {
                prefix: "zzz".to_string(),
            }),
            ..Default::default()
        })
        .await?;

    assert!(response.get_ref().prefix.is_some());
    let prefix_response = response.get_ref().prefix.as_ref().unwrap();
    let results = &prefix_response.nodes;
    assert!(results.is_empty());

    Ok(())
}

#[tokio::test]
async fn test_search_relations_neighbours() -> Result<(), Box<dyn std::error::Error>> {
    let (mut reader, mut writer) = node_services().await;

    let new_shard_response = writer.new_shard(Request::new(EmptyQuery {})).await?;
    let shard_id = &new_shard_response.get_ref().id;

    let relation_nodes = create_knowledge_graph(&mut writer, shard_id.clone()).await;

    fn extract_relations(response: &RelationSearchResponse) -> HashSet<(String, String)> {
        response
            .subgraph
            .iter()
            .flat_map(|neighbours| neighbours.relations.iter())
            .flat_map(|node| {
                vec![(
                    node.source.as_ref().unwrap().value.to_owned(),
                    node.to.as_ref().unwrap().value.to_owned(),
                )]
            })
            .collect::<HashSet<_>>()
    }

    // --------------------------------------------------------------
    // Test: neighbours search on existent node
    // --------------------------------------------------------------

    let response = reader
        .relation_search(RelationSearchRequest {
            shard_id: shard_id.clone(),
            subgraph: Some(EntitiesSubgraphRequest {
                entry_points: vec![relation_nodes.get("Swallow").unwrap().clone()],
                depth: Some(1),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await?;

    let expected = HashSet::from_iter(vec![
        ("Poetry".to_string(), "Swallow".to_string()),
        ("Swallow".to_string(), "Animal".to_string()),
        ("Swallow".to_string(), "Fly".to_string()),
    ]);
    let neighbour_relations = extract_relations(response.get_ref());
    assert_eq!(neighbour_relations, expected);

    // --------------------------------------------------------------
    // Test: neighbours search on multiple existent nodes
    // --------------------------------------------------------------

    let response = reader
        .relation_search(RelationSearchRequest {
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
        })
        .await?;

    let expected = HashSet::from_iter(vec![
        ("Newton".to_string(), "Physics".to_string()),
        ("Newton".to_string(), "Gravity".to_string()),
        ("Becquer".to_string(), "Poetry".to_string()),
        ("Joan Antoni".to_string(), "Becquer".to_string()),
    ]);
    let neighbour_relations = extract_relations(response.get_ref());
    assert_eq!(neighbour_relations, expected);

    // --------------------------------------------------------------
    // Test: neighbours search on non existent node
    // --------------------------------------------------------------

    let response = reader
        .relation_search(RelationSearchRequest {
            shard_id: shard_id.clone(),
            subgraph: Some(EntitiesSubgraphRequest {
                entry_points: vec![RelationNode {
                    value: "Fake".to_string(),
                    ntype: NodeType::Entity as i32,
                    subtype: "".to_string(),
                }],
                depth: Some(1),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await?;

    let neighbours = extract_relations(response.get_ref());
    assert!(neighbours.is_empty());

    // --------------------------------------------------------------
    // Test: neighbours search with filters
    // --------------------------------------------------------------

    let response = reader
        .relation_search(RelationSearchRequest {
            shard_id: shard_id.clone(),
            subgraph: Some(EntitiesSubgraphRequest {
                entry_points: vec![relation_nodes.get("Poetry").unwrap().clone()],
                node_filters: vec![RelationNodeFilter {
                    node_type: NodeType::Entity as i32,
                    ..Default::default()
                }],
                edge_filters: vec![RelationEdgeFilter {
                    relation_type: RelationType::About as i32,
                    ..Default::default()
                }],
                depth: Some(1),
            }),
            ..Default::default()
        })
        .await?;

    let expected = HashSet::from_iter(vec![("Poetry".to_string(), "Swallow".to_string())]);
    let neighbour_relations = extract_relations(response.get_ref());
    assert_eq!(neighbour_relations, expected);

    Ok(())
}
