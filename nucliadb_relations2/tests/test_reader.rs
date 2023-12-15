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
use nucliadb_core::prelude::*;
use nucliadb_core::protos::entities_subgraph_request::DeletedEntities;
use nucliadb_core::protos::relation::RelationType;
use nucliadb_core::protos::relation_node::NodeType;
use nucliadb_core::protos::{
    EntitiesSubgraphRequest, RelationNodeFilter, RelationPrefixSearchRequest,
    RelationSearchRequest, Resource, ResourceId,
};
use nucliadb_core::Channel;
use nucliadb_relations2::reader::RelationsReaderService;
use nucliadb_relations2::writer::RelationsWriterService;
use tempfile::TempDir;

fn create_reader() -> NodeResult<RelationsReaderService> {
    let dir = TempDir::new().unwrap();
    let config = RelationConfig {
        path: dir.path().join("relations"),
        channel: Channel::EXPERIMENTAL,
    };

    let mut writer = RelationsWriterService::start(&config)?;
    let reader = RelationsReaderService::start(&config)?;

    writer.set_resource(&Resource {
        resource: Some(ResourceId {
            uuid: "uuid".to_string(),
            shard_id: "shard_id".to_string(),
        }),
        relations: vec![
            common::create_relation(
                "cat".to_string(),
                NodeType::Entity,
                "ANIMALS".to_string(),
                "cat".to_string(),
                NodeType::Entity,
                "ANIMALS".to_string(),
                RelationType::Entity,
            ),
            common::create_relation(
                "dolphin".to_string(),
                NodeType::Entity,
                "ANIMALS".to_string(),
                "dolphin".to_string(),
                NodeType::Entity,
                "ANIMALS".to_string(),
                RelationType::Entity,
            ),
            common::create_relation(
                "01808bbd8e784552967a4fb0d8b6e584".to_string(),
                NodeType::Resource,
                "".to_string(),
                "dog".to_string(),
                NodeType::Entity,
                "ANIMALS".to_string(),
                RelationType::Entity,
            ),
            common::create_relation(
                "01808bbd8e784552967a4fb0d8b6e584".to_string(),
                NodeType::Resource,
                "".to_string(),
                "bird".to_string(),
                NodeType::Entity,
                "ANIMALS".to_string(),
                RelationType::Entity,
            ),
            common::create_relation(
                "Anastasia".to_string(),
                NodeType::Entity,
                "PEOPLE".to_string(),
                "Anna".to_string(),
                NodeType::Entity,
                "PEOPLE".to_string(),
                RelationType::Entity,
            ),
            common::create_relation(
                "Anthony".to_string(),
                NodeType::Entity,
                "PEOPLE".to_string(),
                "Netherlands".to_string(),
                NodeType::Entity,
                "PLACES".to_string(),
                RelationType::Entity,
            ),
            common::create_relation(
                "Anna".to_string(),
                NodeType::Entity,
                "PEOPLE".to_string(),
                "New York".to_string(),
                NodeType::Entity,
                "PLACES".to_string(),
                RelationType::Entity,
            ),
        ],
        ..Default::default()
    })?;

    reader.reader.reload()?;

    Ok(reader)
}

#[test]
fn test_start_new_reader_after_a_writer() -> NodeResult<()> {
    let dir = TempDir::new()?;
    let config = RelationConfig {
        path: dir.path().join("relations"),
        channel: Channel::EXPERIMENTAL,
    };

    let _writer = RelationsWriterService::start(&config)?;
    let reader: Result<RelationsReaderService, nucliadb_core::Error> =
        RelationsReaderService::start(&config);
    assert!(reader.is_ok());

    Ok(())
}

#[test]
fn test_stored_ids() -> NodeResult<()> {
    let reader = create_reader()?;
    let stored_ids = reader.stored_ids()?;

    assert_eq!(stored_ids.len(), 10);
    Ok(())
}

#[test]
fn test_search_with_deleted() -> NodeResult<()> {
    let reader = create_reader()?;

    let result = reader.search(&RelationSearchRequest {
        subgraph: Some(EntitiesSubgraphRequest {
            depth: Some(1_i32),
            entry_points: vec![common::create_relation_node(
                "01808bbd8e784552967a4fb0d8b6e584".to_string(),
                NodeType::Resource,
                "".to_string(),
            )],
            deleted_entities: vec![DeletedEntities {
                node_subtype: "ANIMALS".into(),
                node_values: vec!["bird".to_string()],
            }],
            ..Default::default()
        }),
        ..Default::default()
    })?;

    let subgraph = result.subgraph.unwrap();
    assert_eq!(subgraph.relations.len(), 1);

    let result = reader.search(&RelationSearchRequest {
        subgraph: Some(EntitiesSubgraphRequest {
            depth: Some(1_i32),
            entry_points: vec![common::create_relation_node(
                "01808bbd8e784552967a4fb0d8b6e584".to_string(),
                NodeType::Resource,
                "".to_string(),
            )],
            deleted_entities: vec![DeletedEntities {
                node_subtype: "ANIMALS".into(),
                node_values: vec!["bird".to_string(), "dog".to_string()],
            }],
            ..Default::default()
        }),
        ..Default::default()
    })?;

    let subgraph = result.subgraph.unwrap();
    assert_eq!(subgraph.relations.len(), 0);

    Ok(())
}

#[test]
fn test_search() -> NodeResult<()> {
    let reader = create_reader()?;

    let result = reader.search(&RelationSearchRequest {
        subgraph: Some(EntitiesSubgraphRequest {
            depth: Some(1_i32),
            entry_points: vec![
                common::create_relation_node(
                    "dog".to_string(),
                    NodeType::Entity,
                    "ANIMALS".to_string(),
                ),
                common::create_relation_node(
                    "bird".to_string(),
                    NodeType::Entity,
                    "ANIMALS".to_string(),
                ),
            ],
            ..Default::default()
        }),
        ..Default::default()
    })?;

    assert_eq!(result.subgraph.unwrap().relations.len(), 2);

    Ok(())
}

#[test]
fn test_prefix_search() -> NodeResult<()> {
    let reader = create_reader()?;

    let result = reader.search(&RelationSearchRequest {
        prefix: Some(RelationPrefixSearchRequest {
            prefix: "".to_string(),
            ..Default::default()
        }),
        ..Default::default()
    })?;

    assert_eq!(result.prefix.unwrap().nodes.len(), 10);

    let result = reader.search(&RelationSearchRequest {
        prefix: Some(RelationPrefixSearchRequest {
            prefix: "do".to_string(),
            ..Default::default()
        }),
        ..Default::default()
    })?;

    assert_eq!(result.prefix.unwrap().nodes.len(), 2);

    let result = reader.search(&RelationSearchRequest {
        prefix: Some(RelationPrefixSearchRequest {
            prefix: "ann".to_string(),
            ..Default::default()
        }),
        ..Default::default()
    })?;

    assert_eq!(result.prefix.unwrap().nodes.len(), 3);
    Ok(())
}

#[test]
fn test_prefix_search_with_filters() -> NodeResult<()> {
    let reader = create_reader()?;

    let result = reader.search(&RelationSearchRequest {
        prefix: Some(RelationPrefixSearchRequest {
            prefix: "".to_string(),
            node_filters: vec![RelationNodeFilter {
                node_type: NodeType::Entity as i32,
                node_subtype: Some("ANIMALS".to_string()),
            }],
        }),
        ..Default::default()
    })?;

    assert_eq!(result.prefix.unwrap().nodes.len(), 4);

    let result = reader.search(&RelationSearchRequest {
        prefix: Some(RelationPrefixSearchRequest {
            prefix: "".to_string(),
            node_filters: vec![RelationNodeFilter {
                node_type: NodeType::Resource as i32,
                node_subtype: None,
            }],
        }),
        ..Default::default()
    })?;

    assert_eq!(result.prefix.unwrap().nodes.len(), 1);

    let result = reader.search(&RelationSearchRequest {
        prefix: Some(RelationPrefixSearchRequest {
            prefix: "".to_string(),
            node_filters: vec![RelationNodeFilter {
                node_type: NodeType::Resource as i32,
                node_subtype: Some("foobarmissing".to_string()),
            }],
        }),
        ..Default::default()
    })?;

    // XXX WHY ISN'T THIS WORKING?
    assert_eq!(result.prefix.unwrap().nodes.len(), 0);

    Ok(())
}
