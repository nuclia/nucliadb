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

use nidx_protos::entities_subgraph_request::DeletedEntities;
use nidx_protos::relation::RelationType;
use nidx_protos::relation_node::NodeType;
use nidx_protos::{
    EntitiesSubgraphRequest, RelationMetadata, RelationNodeFilter, RelationPrefixSearchRequest, RelationSearchRequest,
    Resource, ResourceId,
};
use nidx_relation::{RelationIndexer, RelationSearcher};
use nidx_tantivy::{TantivyMeta, TantivySegmentMetadata};
use nidx_types::{OpenIndexMetadata, Seq};
use tempfile::TempDir;

pub struct TestOpener {
    segments: Vec<(TantivySegmentMetadata, Seq)>,
    deletions: Vec<(String, Seq)>,
}

impl TestOpener {
    pub fn new(segments: Vec<(TantivySegmentMetadata, Seq)>, deletions: Vec<(String, Seq)>) -> Self {
        Self {
            segments,
            deletions,
        }
    }
}

impl OpenIndexMetadata<TantivyMeta> for TestOpener {
    fn segments(&self) -> impl Iterator<Item = (nidx_types::SegmentMetadata<TantivyMeta>, nidx_types::Seq)> {
        self.segments.iter().cloned()
    }

    fn deletions(&self) -> impl Iterator<Item = (&String, nidx_types::Seq)> {
        self.deletions.iter().map(|(key, seq)| (key, *seq))
    }
}

fn create_reader() -> anyhow::Result<RelationSearcher> {
    let dir = TempDir::new().unwrap();

    let resource = Resource {
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
            common::create_relation_with_metadata(
                "Anthony".to_string(),
                NodeType::Entity,
                "PEOPLE".to_string(),
                "Netherlands".to_string(),
                NodeType::Entity,
                "PLACES".to_string(),
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
                "Anna".to_string(),
                NodeType::Entity,
                "PEOPLE".to_string(),
                "New York".to_string(),
                NodeType::Entity,
                "PLACES".to_string(),
                RelationType::Entity,
            ),
            common::create_relation(
                "James Bond".to_string(),
                NodeType::Entity,
                "PEOPLE".to_string(),
                "Ian Fleming".to_string(),
                NodeType::Entity,
                "PEOPLE".to_string(),
                RelationType::Entity,
            ),
        ],
        ..Default::default()
    };

    let segment_meta = RelationIndexer.index_resource(dir.path(), &resource).unwrap().unwrap();
    RelationSearcher::open(TestOpener::new(vec![(segment_meta, 1i64.into())], vec![]))
}

#[test]
fn test_search_with_deleted() -> anyhow::Result<()> {
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
fn test_search() -> anyhow::Result<()> {
    let reader = create_reader()?;

    let result = reader.search(&RelationSearchRequest {
        subgraph: Some(EntitiesSubgraphRequest {
            depth: Some(1_i32),
            entry_points: vec![
                common::create_relation_node("dog".to_string(), NodeType::Entity, "ANIMALS".to_string()),
                common::create_relation_node("bird".to_string(), NodeType::Entity, "ANIMALS".to_string()),
            ],
            ..Default::default()
        }),
        ..Default::default()
    })?;

    assert_eq!(result.subgraph.unwrap().relations.len(), 2);

    Ok(())
}

#[test]
fn test_search_metadata() -> anyhow::Result<()> {
    let reader = create_reader()?;

    let result = reader.search(&RelationSearchRequest {
        subgraph: Some(EntitiesSubgraphRequest {
            depth: Some(1_i32),
            entry_points: vec![common::create_relation_node(
                "Anthony".to_string(),
                NodeType::Entity,
                "PEOPLE".to_string(),
            )],
            ..Default::default()
        }),
        ..Default::default()
    })?;

    let subgraph = result.subgraph.unwrap();
    assert_eq!(subgraph.relations.len(), 1);

    let relation = &subgraph.relations[0];
    let metadata = relation.metadata.as_ref().unwrap();
    assert_eq!(metadata.paragraph_id, Some("myresource/0/myresource/100-200".to_string()));
    assert_eq!(metadata.source_start, Some(0));
    assert_eq!(metadata.source_end, Some(10));
    assert_eq!(metadata.to_start, Some(11));
    assert_eq!(metadata.to_end, Some(20));
    assert_eq!(metadata.data_augmentation_task_id, Some("mytask".to_string()));

    Ok(())
}

#[test]
fn test_prefix_search() -> anyhow::Result<()> {
    let reader = create_reader()?;

    let result = reader.search(&RelationSearchRequest {
        prefix: Some(RelationPrefixSearchRequest {
            prefix: "".to_string(),
            ..Default::default()
        }),
        ..Default::default()
    })?;

    assert_eq!(result.prefix.unwrap().nodes.len(), 12);

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
fn test_prefix_query_search() -> anyhow::Result<()> {
    let reader = create_reader()?;

    let result = reader.search(&RelationSearchRequest {
        prefix: Some(RelationPrefixSearchRequest {
            query: Some("Films with James Bond played by Roger Moore".to_string()),
            ..Default::default()
        }),
        ..Default::default()
    })?;
    assert_eq!(result.prefix.unwrap().nodes.len(), 1);

    let result = reader.search(&RelationSearchRequest {
        prefix: Some(RelationPrefixSearchRequest {
            query: Some("Films with Jomes Bond played by Roger Moore".to_string()),
            ..Default::default()
        }),
        ..Default::default()
    })?;
    assert_eq!(result.prefix.unwrap().nodes.len(), 1);

    let result = reader.search(&RelationSearchRequest {
        prefix: Some(RelationPrefixSearchRequest {
            query: Some("Just James".to_string()),
            ..Default::default()
        }),
        ..Default::default()
    })?;
    assert_eq!(result.prefix.unwrap().nodes.len(), 0);

    let result = reader.search(&RelationSearchRequest {
        prefix: Some(RelationPrefixSearchRequest {
            query: Some("James Bond or Anastasia".to_string()),
            ..Default::default()
        }),
        ..Default::default()
    })?;
    assert_eq!(result.prefix.unwrap().nodes.len(), 2);

    Ok(())
}

#[test]
fn test_prefix_search_with_filters() -> anyhow::Result<()> {
    let reader = create_reader()?;

    let result = reader.search(&RelationSearchRequest {
        prefix: Some(RelationPrefixSearchRequest {
            prefix: "".to_string(),
            node_filters: vec![RelationNodeFilter {
                node_type: NodeType::Entity as i32,
                node_subtype: Some("ANIMALS".to_string()),
            }],
            ..Default::default()
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
            ..Default::default()
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
            ..Default::default()
        }),
        ..Default::default()
    })?;

    // XXX WHY ISN'T THIS WORKING?
    assert_eq!(result.prefix.unwrap().nodes.len(), 0);

    Ok(())
}
