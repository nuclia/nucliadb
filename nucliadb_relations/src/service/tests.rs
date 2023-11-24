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
use std::collections::HashMap;
use std::path::Path;

use lazy_static::lazy_static;
use nucliadb_core::prelude::*;
use nucliadb_core::protos::resource::ResourceStatus;
use nucliadb_core::protos::*;
use nucliadb_core::Channel;
use prost_types::Timestamp;
use relation::*;
use relation_node::NodeType;

use super::*;

lazy_static! {
    static ref SHARD_ID: String = "f56c58ac-b4f9-4d61-a077-ffccaadd0001".to_string();
    static ref E0: RelationNode = RelationNode {
        value: "E0".to_string(),
        ntype: NodeType::Entity as i32,
        subtype: "".to_string(),
    };
    static ref E1: RelationNode = RelationNode {
        value: "E1".to_string(),
        ntype: NodeType::Entity as i32,
        subtype: "Official".to_string(),
    };
    static ref E2: RelationNode = RelationNode {
        value: "E2".to_string(),
        ntype: NodeType::Entity as i32,
        subtype: "Propaganda".to_string(),
    };
    static ref NODE_TYPES: TypeList = TypeList {
        list: vec![
            RelationTypeListMember {
                with_type: NodeType::Entity as i32,
                with_subtype: "Official".to_string(),
            },
            RelationTypeListMember {
                with_type: NodeType::Entity as i32,
                with_subtype: "".to_string(),
            },
            RelationTypeListMember {
                with_type: NodeType::Entity as i32,
                with_subtype: "Propaganda".to_string(),
            },
        ]
    };
    static ref REQUEST_BONES: RelationSearchRequest = RelationSearchRequest {
        shard_id: SHARD_ID.clone(),
        prefix: None,
        subgraph: None,
        ..Default::default()
    };
    static ref REQUEST0: EntitiesSubgraphRequest = EntitiesSubgraphRequest {
        entry_points: vec![E0.clone()],
        depth: Some(1),
        ..Default::default()
    };
    static ref RESPONSE0: Vec<RelationNode> = vec![E0.clone(), E1.clone(), E2.clone()];
    static ref REQUEST1: EntitiesSubgraphRequest = EntitiesSubgraphRequest {
        entry_points: vec![E0.clone()],
        depth: Some(1),
        ..Default::default()
    };
    static ref RESPONSE1: Vec<RelationNode> = vec![E0.clone(), E1.clone()];
    static ref EDGE_LIST: EdgeList = EdgeList {
        list: vec![
            RelationEdge {
                edge_type: RelationType::Entity as i32,
                property: "".to_string()
            },
            RelationEdge {
                edge_type: RelationType::Child as i32,
                property: "".to_string()
            },
        ]
    };
}

fn create_empty_resource(shard_id: String) -> Resource {
    let resource_id = ResourceId {
        shard_id: SHARD_ID.clone(),
        uuid: SHARD_ID.clone(),
    };
    let timestamp = Timestamp {
        seconds: 0,
        nanos: 0,
    };

    let metadata = IndexMetadata {
        created: Some(timestamp.clone()),
        modified: Some(timestamp),
    };

    Resource {
        resource: Some(resource_id),
        metadata: Some(metadata),
        texts: HashMap::with_capacity(0),
        status: ResourceStatus::Processed as i32,
        labels: vec![],
        paragraphs: HashMap::with_capacity(0),
        paragraphs_to_delete: vec![],
        sentences_to_delete: vec![],
        relations: vec![],
        vectors: HashMap::default(),
        vectors_to_delete: HashMap::default(),
        shard_id,
    }
}

fn empty_graph() -> Vec<Relation> {
    vec![]
}

fn entities(mut edges: Vec<Relation>) -> Vec<Relation> {
    let metadata = RelationMetadata {
        paragraph_id: Some("r0".to_string()),
        ..Default::default()
    };
    let r0 = Relation {
        relation: RelationType::Child as i32,
        source: Some(E1.clone()),
        to: Some(E2.clone()),
        relation_label: "".to_string(),
        metadata: Some(metadata),
    };
    let metadata = RelationMetadata {
        paragraph_id: Some("r1".to_string()),
        ..Default::default()
    };
    let r1 = Relation {
        relation: RelationType::Entity as i32,
        source: Some(E0.clone()),
        to: Some(E2.clone()),
        relation_label: "".to_string(),
        metadata: Some(metadata),
    };
    let metadata = RelationMetadata {
        paragraph_id: Some("r2".to_string()),
        ..Default::default()
    };
    let r2 = Relation {
        relation: RelationType::Entity as i32,
        source: Some(E0.clone()),
        to: Some(E1.clone()),
        relation_label: "".to_string(),
        metadata: Some(metadata),
    };
    edges.append(&mut vec![r0, r1, r2]);
    edges
}

fn similatity_edges(mut edges: Vec<Relation>) -> Vec<Relation> {
    let r0 = Relation {
        relation: RelationType::Synonym as i32,
        source: Some(E0.clone()),
        to: Some(E1.clone()),
        relation_label: "".to_string(),
        metadata: None,
    };
    let r1 = Relation {
        relation: RelationType::Synonym as i32,
        source: Some(E1.clone()),
        to: Some(E2.clone()),
        relation_label: "".to_string(),
        metadata: None,
    };
    edges.append(&mut vec![r0, r1]);
    edges
}

fn simple_graph(at: &Path) -> (RelationsWriterService, RelationsReaderService) {
    let rsc = RelationConfig {
        path: at.join("relations"),
        channel: Channel::EXPERIMENTAL,
    };
    println!("Writer starts");
    let writer = RelationsWriterService::start(&rsc).unwrap();
    let reader = RelationsReaderService::start(&rsc).unwrap();
    (writer, reader)
}

#[test]
fn simple_request() -> NodeResult<()> {
    let dir = tempfile::tempdir().unwrap();
    let (mut writer, reader) = simple_graph(dir.path());
    let mut resource = create_empty_resource("f56c58ac-b4f9-4d61-a077-ffccaadd0001".to_string());
    let graph = entities(empty_graph());
    resource.relations = graph;
    writer.set_resource(&resource).unwrap();

    reader.reload();
    let mut request = REQUEST_BONES.clone();
    request.subgraph = Some(REQUEST0.clone());
    let got = reader.search(&request).unwrap();
    let Some(bfs_response) = got.subgraph else {
        unreachable!("Wrong variant")
    };
    let len = bfs_response
        .relations
        .into_iter()
        .flat_map(|v| v.to.zip(v.source))
        .filter(|v| RESPONSE0.contains(&v.0))
        .filter(|v| RESPONSE0.contains(&v.1))
        .count();
    assert_eq!(len + 1, RESPONSE0.len());
    Ok(())
}

#[test]
fn simple_request_with_similarity() -> NodeResult<()> {
    let dir = tempfile::tempdir().unwrap();
    let (mut writer, reader) = simple_graph(dir.path());
    let mut resource = create_empty_resource("f56c58ac-b4f9-4d61-a077-ffccaadd0001".to_string());
    let graph = similatity_edges(entities(empty_graph()));
    resource.relations = graph;
    writer.set_resource(&resource).unwrap();
    reader.reload();

    let mut request = REQUEST_BONES.clone();
    request.subgraph = Some(REQUEST0.clone());
    let got = reader.search(&request).unwrap();
    let Some(bfs_response) = got.subgraph else {
        unreachable!("Wrong variant")
    };
    let len = bfs_response
        .relations
        .into_iter()
        .flat_map(|v| v.to.zip(v.source))
        .filter(|v| RESPONSE0.contains(&v.0))
        .filter(|v| RESPONSE0.contains(&v.1))
        .count();
    assert_eq!(len, RESPONSE0.len() + 2);

    Ok(())
}

#[test]
fn typed_request() -> NodeResult<()> {
    let dir = tempfile::tempdir().unwrap();
    let (mut writer, reader) = simple_graph(dir.path());
    let mut resource = create_empty_resource("f56c58ac-b4f9-4d61-a077-ffccaadd0001".to_string());
    let graph = entities(empty_graph());
    resource.relations = graph;
    writer.set_resource(&resource).unwrap();
    reader.reload();

    let mut request = REQUEST_BONES.clone();
    request.subgraph = Some(REQUEST1.clone());
    let got = reader.search(&request).unwrap();
    let Some(bfs_response) = got.subgraph else {
        unreachable!("Wrong variant")
    };

    let len = bfs_response
        .relations
        .into_iter()
        .flat_map(|v| v.to.zip(v.source))
        .filter(|v| RESPONSE1.contains(&v.0))
        .filter(|v| RESPONSE1.contains(&v.1))
        .count();
    assert_eq!(len + 1, RESPONSE1.len());

    Ok(())
}

#[test]
fn just_prefix_querying() -> NodeResult<()> {
    let dir = tempfile::tempdir().unwrap();
    let (mut writer, reader) = simple_graph(dir.path());
    let mut resource = create_empty_resource("f56c58ac-b4f9-4d61-a077-ffccaadd0001".to_string());
    let graph = entities(empty_graph());
    resource.relations = graph;
    writer.set_resource(&resource).unwrap();

    reader.reload();
    let mut request = REQUEST_BONES.clone();
    request.prefix = Some(RelationPrefixSearchRequest {
        prefix: "E".to_string(),
        ..Default::default()
    });
    let got = reader.search(&request).unwrap();
    let Some(prefix_response) = got.prefix else {
        unreachable!("Wrong variant")
    };
    let is_permutation = prefix_response
        .nodes
        .iter()
        .all(|member| RESPONSE0.contains(member));
    assert!((prefix_response.nodes.len() == RESPONSE0.len()) && is_permutation);

    request.prefix = Some(RelationPrefixSearchRequest {
        prefix: "e".to_string(),
        ..Default::default()
    });
    let got = reader.search(&request).unwrap();
    let Some(prefix_response) = got.prefix else {
        unreachable!("Wrong variant")
    };
    let is_permutation = prefix_response
        .nodes
        .iter()
        .all(|member| RESPONSE0.contains(member));
    assert!((prefix_response.nodes.len() == RESPONSE0.len()) && is_permutation);

    request.prefix = Some(RelationPrefixSearchRequest {
        prefix: "not".to_string(),
        ..Default::default()
    });
    let got = reader.search(&request).unwrap();
    let Some(prefix_response) = got.prefix else {
        unreachable!("Wrong variant")
    };
    assert!(prefix_response.nodes.is_empty());

    Ok(())
}

#[test]
fn getting_node_types() -> NodeResult<()> {
    let dir = tempfile::tempdir().unwrap();
    let (mut writer, reader) = simple_graph(dir.path());
    let mut resource = create_empty_resource("f56c58ac-b4f9-4d61-a077-ffccaadd0001".to_string());
    let graph = entities(empty_graph());
    resource.relations = graph;
    writer.set_resource(&resource).unwrap();
    reader.reload();
    let node_types = reader.get_node_types().unwrap();
    assert_eq!(node_types.list.len(), NODE_TYPES.list.len());
    assert!(node_types
        .list
        .iter()
        .all(|member| NODE_TYPES.list.contains(member)));

    let edges = reader.get_edges().unwrap();
    assert_eq!(edges.list.len(), EDGE_LIST.list.len(),);
    assert!(edges
        .list
        .iter()
        .all(|member| EDGE_LIST.list.contains(member)));
    Ok(())
}

#[test]
fn getting_edges() -> NodeResult<()> {
    let dir = tempfile::tempdir().unwrap();
    let (mut writer, reader) = simple_graph(dir.path());
    let mut resource = create_empty_resource("f56c58ac-b4f9-4d61-a077-ffccaadd0001".to_string());
    let graph = entities(empty_graph());
    resource.relations = graph;
    writer.set_resource(&resource).unwrap();
    reader.reload();
    let edges = reader.get_edges().unwrap();
    assert_eq!(edges.list.len(), EDGE_LIST.list.len(),);
    assert!(edges
        .list
        .iter()
        .all(|member| EDGE_LIST.list.contains(member)));
    Ok(())
}
