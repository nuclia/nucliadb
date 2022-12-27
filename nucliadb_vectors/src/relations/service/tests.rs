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
use nucliadb_protos::*;
use nucliadb_service_interface::prelude::*;
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
    static ref REQUEST0: RelationSearchRequest = RelationSearchRequest {
        id: SHARD_ID.clone(),
        entry_points: vec![E0.clone()],
        type_filters: vec![
            RelationFilter {
                ntype: NodeType::Entity as i32,
                subtype: "".to_string()
            },
            RelationFilter {
                ntype: NodeType::Entity as i32,
                subtype: "Nonexisting".to_string()
            }
        ],
        depth: 1,
        prefix: "".to_string(),
        reload: false,
    };
    static ref RESPONSE0: RelationSearchResponse = RelationSearchResponse {
        neighbours: vec![E0.clone(), E1.clone(), E2.clone()]
    };
    static ref REQUEST1: RelationSearchRequest = RelationSearchRequest {
        id: SHARD_ID.clone(),
        entry_points: vec![E0.clone()],
        type_filters: vec![RelationFilter {
            ntype: NodeType::Entity as i32,
            subtype: "Official".to_string()
        },],
        depth: 1,
        prefix: "".to_string(),
        reload: false,
    };
    static ref RESPONSE1: RelationSearchResponse = RelationSearchResponse {
        neighbours: vec![E1.clone()]
    };
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

    let metadata = nucliadb_protos::IndexMetadata {
        created: Some(timestamp.clone()),
        modified: Some(timestamp),
    };

    Resource {
        resource: Some(resource_id),
        metadata: Some(metadata),
        texts: HashMap::with_capacity(0),
        status: nucliadb_protos::resource::ResourceStatus::Processed as i32,
        labels: vec![],
        paragraphs: HashMap::with_capacity(0),
        paragraphs_to_delete: vec![],
        sentences_to_delete: vec![],
        relations_to_delete: vec![],
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
    let r0 = Relation {
        relation: RelationType::Child as i32,
        source: Some(E1.clone()),
        to: Some(E2.clone()),
        relation_label: "".to_string(),
    };
    let r1 = Relation {
        relation: RelationType::Entity as i32,
        source: Some(E0.clone()),
        to: Some(E2.clone()),
        relation_label: "".to_string(),
    };
    let r2 = Relation {
        relation: RelationType::Entity as i32,
        source: Some(E0.clone()),
        to: Some(E1.clone()),
        relation_label: "".to_string(),
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
    };
    let r1 = Relation {
        relation: RelationType::Entity as i32,
        source: Some(E1.clone()),
        to: Some(E2.clone()),
        relation_label: "".to_string(),
    };
    edges.append(&mut vec![r0, r1]);
    edges
}

fn simple_graph(at: &Path) -> (RelationsWriterService, RelationsReaderService) {
    let rsc = RelationConfig {
        path: at.to_path_buf(),
    };
    println!("Writer starts");
    let writer = RelationsWriterService::start(&rsc).unwrap();
    let reader = RelationsReaderService::open(&rsc).unwrap();
    (writer, reader)
}

#[test]
fn simple_request() -> anyhow::Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let (mut writer, reader) = simple_graph(dir.path());
    let mut resource = create_empty_resource("f56c58ac-b4f9-4d61-a077-ffccaadd0001".to_string());
    let graph = entities(empty_graph());
    resource.relations = graph;
    writer.set_resource(&resource).unwrap();

    reader.reload();
    let got = reader.search(&REQUEST0).unwrap();
    assert_eq!(got.neighbours.len(), RESPONSE0.neighbours.len());
    assert!(got
        .neighbours
        .iter()
        .all(|member| RESPONSE0.neighbours.contains(member)));
    Ok(())
}

#[test]
fn join_graph_test() -> anyhow::Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let (mut writer, reader) = simple_graph(dir.path());
    let mut resource = create_empty_resource("f56c58ac-b4f9-4d61-a077-ffccaadd0001".to_string());
    let graph = empty_graph();
    resource.relations = graph;
    writer.set_resource(&resource).unwrap();
    let got = reader.count().unwrap();
    assert_eq!(got, 0);

    let graph = JoinGraph {
        nodes: HashMap::from([(0i32, E0.clone()), (1i32, E1.clone()), (2i32, E2.clone())]),
        edges: vec![
            JoinGraphCnx {
                source: 2,
                target: 1,
                rtype: RelationType::Child as i32,
                rsubtype: "".to_string(),
            },
            JoinGraphCnx {
                source: 0,
                target: 2,
                rtype: RelationType::Entity as i32,
                rsubtype: "".to_string(),
            },
            JoinGraphCnx {
                source: 0,
                target: 1,
                rtype: RelationType::Entity as i32,
                rsubtype: "".to_string(),
            },
        ],
    };
    writer.join_graph(&graph).unwrap();
    reader.reload();
    let got = reader.search(&REQUEST0).unwrap();
    assert_eq!(got.neighbours.len(), RESPONSE0.neighbours.len());
    assert!(got
        .neighbours
        .iter()
        .all(|member| RESPONSE0.neighbours.contains(member)));
    Ok(())
}

#[test]
fn simple_request_with_similarity() -> anyhow::Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let (mut writer, reader) = simple_graph(dir.path());
    let mut resource = create_empty_resource("f56c58ac-b4f9-4d61-a077-ffccaadd0001".to_string());
    let graph = similatity_edges(entities(empty_graph()));
    resource.relations = graph;
    writer.set_resource(&resource).unwrap();

    reader.reload();
    let mut request = REQUEST0.clone();
    request.depth = 0;
    let got = reader.search(&REQUEST0).unwrap();
    println!("{:?}", got.neighbours);
    assert_eq!(got.neighbours.len(), RESPONSE0.neighbours.len());
    assert!(got
        .neighbours
        .iter()
        .all(|member| RESPONSE0.neighbours.contains(member)));
    Ok(())
}

#[test]
fn typed_request() -> anyhow::Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let (mut writer, reader) = simple_graph(dir.path());
    let mut resource = create_empty_resource("f56c58ac-b4f9-4d61-a077-ffccaadd0001".to_string());
    let graph = entities(empty_graph());
    resource.relations = graph;
    writer.set_resource(&resource).unwrap();

    reader.reload();
    let got = reader.search(&REQUEST1).unwrap();
    assert_eq!(got.neighbours.len(), RESPONSE1.neighbours.len());
    assert!(got
        .neighbours
        .iter()
        .all(|member| RESPONSE1.neighbours.contains(member)));
    Ok(())
}

#[test]
fn just_prefix_querying() -> anyhow::Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let (mut writer, reader) = simple_graph(dir.path());
    let mut resource = create_empty_resource("f56c58ac-b4f9-4d61-a077-ffccaadd0001".to_string());
    let graph = entities(empty_graph());
    resource.relations = graph;
    writer.set_resource(&resource).unwrap();

    reader.reload();
    let mut request = REQUEST0.clone();
    request.entry_points.clear();
    request.prefix = "E".to_string();
    let got = reader.search(&request).unwrap();
    assert_eq!(got.neighbours.len(), RESPONSE0.neighbours.len());
    assert!(got
        .neighbours
        .iter()
        .all(|member| RESPONSE0.neighbours.contains(member)));

    request.prefix = "e".to_string();
    let got = reader.search(&request).unwrap();
    assert_eq!(got.neighbours.len(), RESPONSE0.neighbours.len());
    assert!(got
        .neighbours
        .iter()
        .all(|member| RESPONSE0.neighbours.contains(member)));
    request.prefix = "not".to_string();
    let got = reader.search(&request).unwrap();
    assert!(got.neighbours.is_empty());
    Ok(())
}

#[test]
fn getting_node_types() -> anyhow::Result<()> {
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
fn getting_edges() -> anyhow::Result<()> {
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
