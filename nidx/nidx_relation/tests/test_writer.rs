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

use nidx_protos::{
    IndexRelations, RelationMetadata, Resource, ResourceId, relation::RelationType, relation_node::NodeType,
};
use nidx_relation::{RelationConfig, RelationIndexer};
use tempfile::TempDir;

#[test]
fn test_index_docs() -> anyhow::Result<()> {
    let dir = TempDir::new().unwrap();

    let resource = Resource {
        resource: Some(ResourceId {
            uuid: "01808bbd8e784552967a4fb0d8b6e584".to_string(),
            shard_id: "shard_id".to_string(),
        }),
        field_relations: HashMap::from([(
            "a/metadata".to_string(),
            IndexRelations {
                relations: vec![
                    common::create_relation(
                        "01808bbd8e784552967a4fb0d8b6e584".to_string(),
                        NodeType::Resource,
                        "".to_string(),
                        "dog".to_string(),
                        NodeType::Entity,
                        "ANIMALS".to_string(),
                        RelationType::Entity,
                        "IS".to_string(),
                    ),
                    common::create_relation_with_metadata(
                        "01808bbd8e784552967a4fb0d8b6e584".to_string(),
                        NodeType::Resource,
                        "".to_string(),
                        "bird".to_string(),
                        NodeType::Entity,
                        "ANIMALS".to_string(),
                        RelationType::Entity,
                        "IS".to_string(),
                        RelationMetadata {
                            paragraph_id: Some("myresource/0/myresource/100-200".to_string()),
                            source_start: Some(0),
                            source_end: Some(10),
                            to_start: Some(11),
                            to_end: Some(20),
                            data_augmentation_task_id: Some("mytask".to_string()),
                        },
                    ),
                ],
            },
        )]),
        ..Default::default()
    };

    let meta = RelationIndexer
        .index_resource(dir.path(), &RelationConfig::default(), &resource)?
        .unwrap();

    assert_eq!(meta.records, 2);

    Ok(())
}
