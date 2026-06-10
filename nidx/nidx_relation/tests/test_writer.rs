// Copyright 2021 Bosutech XXI S.L.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
mod common;

use std::collections::HashMap;

use nidx_protos::{
    IndexRelations, RelationMetadata, Resource, ResourceId, relation::RelationType, relation_node::NodeType,
};
use nidx_relation::{RelationConfig, RelationIndexer};
use tempfile::TempDir;

use crate::common::TestOpener;

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

#[test]
fn test_merge() -> anyhow::Result<()> {
    let dir1 = TempDir::new().unwrap();

    let resource = Resource {
        resource: Some(ResourceId {
            uuid: "01808bbd8e784552967a4fb0d8b6e584".to_string(),
            shard_id: "shard_id".to_string(),
        }),
        field_relations: HashMap::from([(
            "a/metadata".to_string(),
            IndexRelations {
                relations: vec![common::create_relation(
                    "01808bbd8e784552967a4fb0d8b6e584".to_string(),
                    NodeType::Resource,
                    "".to_string(),
                    "dog".to_string(),
                    NodeType::Entity,
                    "ANIMALS".to_string(),
                    RelationType::Entity,
                    "IS".to_string(),
                )],
            },
        )]),
        ..Default::default()
    };

    let meta1 = RelationIndexer
        .index_resource(dir1.path(), &RelationConfig::default(), &resource)?
        .unwrap();

    assert_eq!(meta1.records, 1);

    let dir2 = TempDir::new().unwrap();

    let resource = Resource {
        resource: Some(ResourceId {
            uuid: "81808bbd8e784552967a4fb0d8b6e584".to_string(),
            shard_id: "shard_id".to_string(),
        }),
        field_relations: HashMap::from([(
            "a/metadata".to_string(),
            IndexRelations {
                relations: vec![common::create_relation(
                    "81808bbd8e784552967a4fb0d8b6e584".to_string(),
                    NodeType::Resource,
                    "".to_string(),
                    "cat".to_string(),
                    NodeType::Entity,
                    "ANIMALS".to_string(),
                    RelationType::Entity,
                    "IS".to_string(),
                )],
            },
        )]),
        ..Default::default()
    };

    let meta2 = RelationIndexer
        .index_resource(dir2.path(), &RelationConfig::default(), &resource)?
        .unwrap();

    assert_eq!(meta2.records, 1);

    let dir_merge = TempDir::new().unwrap();
    let merged_meta = RelationIndexer.merge(
        dir_merge.path(),
        RelationConfig::default(),
        TestOpener::new(vec![(meta1.clone(), 1u64.into()), (meta2.clone(), 2u64.into())], vec![]),
    )?;

    assert_eq!(merged_meta.records, 2);

    let dir_merge2 = TempDir::new().unwrap();
    let merged_meta2 = RelationIndexer.merge(
        dir_merge2.path(),
        RelationConfig::default(),
        TestOpener::new(
            vec![(meta1, 1u64.into()), (meta2, 2u64.into())],
            vec![("01808bbd8e784552967a4fb0d8b6e584".to_string(), 3u64.into())],
        ),
    )?;

    assert_eq!(merged_meta2.records, 1);

    Ok(())
}
