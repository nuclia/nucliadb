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

use std::collections::HashSet;

use nidx_protos::{
    IndexRelation, IndexRelations, Relation, RelationNode, RelationNodeVector, Resource, ResourceId, VectorSentence,
    relation::RelationType, relation_node::NodeType,
};
use nidx_types::{
    prefilter::{FieldId, PrefilterResult},
    query_language::{BooleanExpression, BooleanOperation, Operator},
};
use nidx_vector::{VectorIndexer, VectorSearchRequest, VectorSearcher, config::*};
use rstest::rstest;
use tempfile::tempdir;

use crate::common::TestOpener;

fn sentence(index: usize) -> VectorSentence {
    let mut vector: Vec<f32> = [0.0; DIMENSION].into();
    vector[index] = 1.0;

    VectorSentence { vector, metadata: None }
}

const DIMENSION: usize = 4;

#[test]
fn test_indexer() -> anyhow::Result<()> {
    let config = VectorConfig {
        similarity: Similarity::Dot,
        vector_type: VectorType::DenseF32 { dimension: DIMENSION },
        normalize_vectors: false,
        flags: vec![],
        vector_cardinality: VectorCardinality::Single,
        disable_indexes: true,
    };

    let resource = Resource {
        resource: Some(ResourceId {
            uuid: "00112233445566778899aabbccddeeff".into(),
            ..Default::default()
        }),
        field_relations: [
            (
                "a/title".into(),
                IndexRelations {
                    relations: vec![IndexRelation {
                        relation: Some(Relation {
                            source: Some(RelationNode {
                                value: "dog".into(),
                                ntype: NodeType::Entity as i32,
                                subtype: "animal".into(),
                            }),
                            to: Some(RelationNode {
                                value: "cat".into(),
                                ntype: NodeType::Entity as i32,
                                subtype: "animal".into(),
                            }),
                            relation: RelationType::Entity as i32,
                            relation_label: "bigger than".into(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                },
            ),
            (
                "f/file".into(),
                IndexRelations {
                    relations: vec![IndexRelation {
                        relation: Some(Relation {
                            source: Some(RelationNode {
                                value: "dog".into(),
                                ntype: NodeType::Entity as i32,
                                subtype: "animal".into(),
                            }),
                            to: Some(RelationNode {
                                value: "fish".into(),
                                ntype: NodeType::Entity as i32,
                                subtype: "animal".into(),
                            }),
                            relation: RelationType::Entity as i32,
                            relation_label: "bigger than".into(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                },
            ),
        ]
        .into(),
        relation_node_vectors: vec![
            RelationNodeVector {
                node: Some(RelationNode {
                    value: "dog".into(),
                    ntype: NodeType::Entity as i32,
                    subtype: "animal".into(),
                }),
                vector: vec![1.0, 0.0, 0.0, 0.0],
            },
            RelationNodeVector {
                node: Some(RelationNode {
                    value: "cat".into(),
                    ntype: NodeType::Entity as i32,
                    subtype: "animal".into(),
                }),
                vector: vec![0.0, 1.0, 0.0, 0.0],
            },
            RelationNodeVector {
                node: Some(RelationNode {
                    value: "fish".into(),
                    ntype: NodeType::Entity as i32,
                    subtype: "animal".into(),
                }),
                vector: vec![0.0, 0.0, 1.0, 0.0],
            },
        ],
        ..Default::default()
    };

    let segment_dir = tempdir()?;
    let segment_meta = VectorIndexer
        .index_resource(segment_dir.path(), &config, &resource, "default", true)?
        .unwrap();

    // Search near one specific vector
    let searcher = VectorSearcher::open(
        config.clone(),
        TestOpener::new(vec![(segment_meta.clone(), 1i64.into())], vec![]),
    )?;
    let search_for = vec![1.0, 0.0, 0.0, 0.0];
    let results = searcher.search(
        &VectorSearchRequest {
            vector: search_for,
            result_per_page: 10,
            ..Default::default()
        },
        &PrefilterResult::All,
    )?;

    assert_eq!(results.documents.len(), 3);
    assert_eq!(results.documents[0].score, 1.0);
    assert_eq!(results.documents[1].score, 0.0);

    // Search with a deletion
    let searcher = VectorSearcher::open(
        config.clone(),
        TestOpener::new(
            vec![(segment_meta.clone(), 1i64.into())],
            vec![("00112233445566778899aabbccddeeff/a/title".into(), 2u64.into())],
        ),
    )?;
    let search_for = vec![1.0, 0.0, 0.0, 0.0];
    let results = searcher.search(
        &VectorSearchRequest {
            vector: search_for,
            result_per_page: 10,
            ..Default::default()
        },
        &PrefilterResult::All,
    )?;

    assert_eq!(results.documents.len(), 2);
    assert_eq!(results.documents[0].score, 1.0);
    assert_eq!(results.documents[1].score, 0.0);

    /////////////////////////////////

    let resource2 = Resource {
        resource: Some(ResourceId {
            uuid: "ffeeddccbbaa99887766554433221100".into(),
            ..Default::default()
        }),
        field_relations: [
            (
                "a/title".into(),
                IndexRelations {
                    relations: vec![IndexRelation {
                        relation: Some(Relation {
                            source: Some(RelationNode {
                                value: "dog".into(),
                                ntype: NodeType::Entity as i32,
                                subtype: "animal".into(),
                            }),
                            to: Some(RelationNode {
                                value: "cat".into(),
                                ntype: NodeType::Entity as i32,
                                subtype: "animal".into(),
                            }),
                            relation: RelationType::Entity as i32,
                            relation_label: "bigger than".into(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                },
            ),
            (
                "f/file".into(),
                IndexRelations {
                    relations: vec![IndexRelation {
                        relation: Some(Relation {
                            source: Some(RelationNode {
                                value: "sheep".into(),
                                ntype: NodeType::Entity as i32,
                                subtype: "animal".into(),
                            }),
                            to: Some(RelationNode {
                                value: "rat".into(),
                                ntype: NodeType::Entity as i32,
                                subtype: "animal".into(),
                            }),
                            relation: RelationType::Entity as i32,
                            relation_label: "bigger than".into(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                },
            ),
        ]
        .into(),
        relation_node_vectors: vec![
            RelationNodeVector {
                node: Some(RelationNode {
                    value: "dog".into(),
                    ntype: NodeType::Entity as i32,
                    subtype: "animal".into(),
                }),
                vector: vec![1.0, 0.0, 0.0, 0.0],
            },
            RelationNodeVector {
                node: Some(RelationNode {
                    value: "cat".into(),
                    ntype: NodeType::Entity as i32,
                    subtype: "animal".into(),
                }),
                vector: vec![0.0, 1.0, 0.0, 0.0],
            },
            RelationNodeVector {
                node: Some(RelationNode {
                    value: "sheep".into(),
                    ntype: NodeType::Entity as i32,
                    subtype: "animal".into(),
                }),
                vector: vec![
                    0.0,
                    0.0,
                    std::f32::consts::FRAC_1_SQRT_2,
                    std::f32::consts::FRAC_1_SQRT_2,
                ],
            },
            RelationNodeVector {
                node: Some(RelationNode {
                    value: "rat".into(),
                    ntype: NodeType::Entity as i32,
                    subtype: "animal".into(),
                }),
                vector: vec![0.0, 0.0, 0.0, 1.0],
            },
        ],
        ..Default::default()
    };

    let segment_dir2 = tempdir()?;
    let segment_meta2 = VectorIndexer
        .index_resource(segment_dir2.path(), &config, &resource2, "default", true)?
        .unwrap();

    let segment_dir_merge = tempdir()?;
    let merged_meta = VectorIndexer.merge(
        segment_dir_merge.path(),
        config.clone(),
        TestOpener::new(
            vec![
                (segment_meta.clone(), 1i64.into()),
                (segment_meta2.clone(), 2i64.into()),
            ],
            vec![],
        ),
    )?;

    let searcher = VectorSearcher::open(
        config.clone(),
        TestOpener::new(vec![(merged_meta.clone(), 1i64.into())], vec![]),
    )?;
    let search_for = vec![1.0, 0.0, 0.0, 0.0];
    let results = searcher.search(
        &VectorSearchRequest {
            vector: search_for,
            result_per_page: 10,
            ..Default::default()
        },
        &PrefilterResult::All,
    )?;

    assert_eq!(results.documents.len(), 5);
    assert_eq!(results.documents[0].score, 1.0);
    assert_eq!(results.documents[1].score, 0.0);

    // Deleting one title does nothing, as the other segment has the same entity in the same field
    let searcher = VectorSearcher::open(
        config.clone(),
        TestOpener::new(
            vec![(merged_meta.clone(), 1i64.into())],
            vec![("00112233445566778899aabbccddeeff/a/title".into(), 2u64.into())],
        ),
    )?;
    let search_for = vec![1.0, 0.0, 0.0, 0.0];
    let results = searcher.search(
        &VectorSearchRequest {
            vector: search_for,
            result_per_page: 10,
            ..Default::default()
        },
        &PrefilterResult::All,
    )?;

    assert_eq!(results.documents.len(), 5);
    assert_eq!(results.documents[0].score, 1.0);
    assert_eq!(results.documents[1].score, 0.0);

    // Deleting both titles eliminates cat
    let searcher = VectorSearcher::open(
        config.clone(),
        TestOpener::new(
            vec![(merged_meta.clone(), 1i64.into())],
            vec![
                ("00112233445566778899aabbccddeeff/a/title".into(), 2u64.into()),
                ("ffeeddccbbaa99887766554433221100/a/title".into(), 2u64.into()),
            ],
        ),
    )?;
    let search_for = vec![1.0, 0.0, 0.0, 0.0];
    let results = searcher.search(
        &VectorSearchRequest {
            vector: search_for,
            result_per_page: 10,
            ..Default::default()
        },
        &PrefilterResult::All,
    )?;

    assert_eq!(results.documents.len(), 4);
    assert_eq!(results.documents[0].score, 1.0);
    assert_eq!(results.documents[1].score, 0.0);

    // TODO: test deletions applied during merge

    Ok(())
}
