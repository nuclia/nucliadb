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
use nucliadb_core::protos::relation::RelationType;
use nucliadb_core::protos::relation_node::NodeType;
use nucliadb_core::protos::{RelationMetadata, Resource, ResourceId};
use nucliadb_core::relations::*;
use nucliadb_relations2::writer::RelationsWriterService;
use tempfile::TempDir;

#[test]
fn test_index_docs() -> NodeResult<()> {
    let dir = TempDir::new().unwrap();
    let config = RelationConfig {
        path: dir.path().join("relations"),
    };

    let mut writer = RelationsWriterService::create(config).unwrap();

    writer.set_resource(&Resource {
        resource: Some(ResourceId {
            uuid: "uuid".to_string(),
            shard_id: "shard_id".to_string(),
        }),
        relations: vec![
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
        ],
        ..Default::default()
    })?;
    writer.set_resource(&Resource {
        resource: Some(ResourceId {
            uuid: "uuid2".to_string(),
            shard_id: "shard_id".to_string(),
        }),
        relations: vec![
            common::create_relation_with_metadata(
                "cat".to_string(),
                NodeType::Entity,
                "ANIMALS".to_string(),
                "cat".to_string(),
                NodeType::Entity,
                "ANIMALS".to_string(),
                RelationType::Entity,
                RelationMetadata {
                    paragraph_id: Some("myresource/0/myresource/100-200".to_string()),
                    source_start: Some(0),
                    source_end: Some(10),
                    to_start: Some(11),
                    to_end: Some(20),
                    data_augmentation_task_id: Some("mytask".to_string()),
                    ..Default::default()
                },
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
        ],
        ..Default::default()
    })?;

    assert_eq!(writer.count()?, 4);

    Ok(())
}
