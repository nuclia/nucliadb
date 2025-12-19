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

use std::hash::{DefaultHasher, Hash, Hasher};

use nidx_protos::{
    IndexRelation, IndexRelations, Relation, RelationNode, RelationNodeVector, Resource, ResourceId,
    relation::RelationType, relation_node::NodeType,
};
use nidx_types::prefilter::PrefilterResult;
use nidx_vector::{VectorIndexer, VectorSearchRequest, VectorSearcher, config::*};
use tempfile::tempdir;

use crate::common::TestOpener;

const DIMENSION: usize = 8;

fn relation(from: &str, relation: &str, to: &str) -> IndexRelation {
    IndexRelation {
        relation: Some(Relation {
            source: Some(RelationNode {
                value: from.into(),
                ntype: NodeType::Entity as i32,
                subtype: "animal".into(),
            }),
            to: Some(RelationNode {
                value: to.into(),
                ntype: NodeType::Entity as i32,
                subtype: "animal".into(),
            }),
            relation: RelationType::Entity as i32,
            relation_label: relation.into(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn normalize(v: Vec<f32>) -> Vec<f32> {
    let mut modulus = 0.0;
    for w in &v {
        modulus += w * w;
    }
    modulus = modulus.powf(0.5);

    v.into_iter().map(|w| w / modulus).collect()
}

fn vector_for(value: &str) -> Vec<f32> {
    let mut s = DefaultHasher::new();
    value.hash(&mut s);
    let hash = s.finish();
    normalize(hash.to_le_bytes().map(|v| v as f32).to_vec())
}

fn node_vector(value: &str) -> RelationNodeVector {
    RelationNodeVector {
        node: Some(RelationNode {
            value: value.into(),
            ntype: NodeType::Entity as i32,
            subtype: "animal".into(),
        }),
        vector: vector_for(value),
    }
}

#[test]
fn test_relations_deletion() -> anyhow::Result<()> {
    let config = VectorConfig::for_relations(VectorType::DenseF32 { dimension: DIMENSION });

    let resource = Resource {
        resource: Some(ResourceId {
            uuid: "00112233445566778899aabbccddeeff".into(),
            ..Default::default()
        }),
        field_relations: [
            (
                "a/title".into(),
                IndexRelations {
                    relations: vec![relation("dog", "bigger than", "cat")],
                },
            ),
            (
                "f/file".into(),
                IndexRelations {
                    relations: vec![relation("dog", "bigger than", "fish")],
                },
            ),
            (
                "f/other".into(),
                IndexRelations {
                    relations: vec![relation("sheep", "bigger than", "fish")],
                },
            ),
        ]
        .into(),
        relation_node_vectors: vec![
            node_vector("dog"),
            node_vector("cat"),
            node_vector("fish"),
            node_vector("sheep"),
        ],
        ..Default::default()
    };

    let segment_dir = tempdir()?;
    let segment_meta = VectorIndexer
        .index_resource(segment_dir.path(), &config, &resource, "default", true)?
        .unwrap();
    assert_eq!(segment_meta.records, 4);

    // Search without deletions, all results
    let searcher = VectorSearcher::open(
        config.clone(),
        TestOpener::new(vec![(segment_meta.clone(), 1i64.into())], vec![]),
    )?;
    let search_for = vector_for("dog");
    let results = searcher.search(
        &VectorSearchRequest {
            vector: search_for.clone(),
            result_per_page: 10,
            with_duplicates: true,
            ..Default::default()
        },
        &PrefilterResult::All,
    )?;

    assert_eq!(results.documents.len(), 4);
    assert_eq!(results.documents[0].score, 1.0);
    assert!(results.documents[1].score < 1.0);

    // Search deleting title, cat disappears
    let searcher = VectorSearcher::open(
        config.clone(),
        TestOpener::new(
            vec![(segment_meta.clone(), 1i64.into())],
            vec![("00112233445566778899aabbccddeeff/a/title".into(), 2u64.into())],
        ),
    )?;
    let search_for = vector_for("cat");
    let results = searcher.search(
        &VectorSearchRequest {
            vector: search_for.clone(),
            result_per_page: 10,
            with_duplicates: true,
            ..Default::default()
        },
        &PrefilterResult::All,
    )?;

    assert_eq!(results.documents.len(), 3);
    assert!(results.documents[0].score < 1.0);

    // Search deleting title, cat disappears
    let searcher = VectorSearcher::open(
        config.clone(),
        TestOpener::new(
            vec![(segment_meta.clone(), 1i64.into())],
            vec![("00112233445566778899aabbccddeeff/a/title".into(), 2u64.into())],
        ),
    )?;
    let search_for = vector_for("cat");
    let results = searcher.search(
        &VectorSearchRequest {
            vector: search_for.clone(),
            result_per_page: 10,
            with_duplicates: true,
            ..Default::default()
        },
        &PrefilterResult::All,
    )?;

    assert_eq!(results.documents.len(), 3);
    assert!(results.documents[0].score < 1.0);

    // Search deleting title and file, dog and cat disappear
    let searcher = VectorSearcher::open(
        config.clone(),
        TestOpener::new(
            vec![(segment_meta.clone(), 1i64.into())],
            vec![
                ("00112233445566778899aabbccddeeff/a/title".into(), 2u64.into()),
                ("00112233445566778899aabbccddeeff/f/file".into(), 2u64.into()),
            ],
        ),
    )?;
    let search_for = vector_for("dog");
    let results = searcher.search(
        &VectorSearchRequest {
            vector: search_for.clone(),
            result_per_page: 10,
            with_duplicates: true,
            ..Default::default()
        },
        &PrefilterResult::All,
    )?;

    assert_eq!(results.documents.len(), 2);
    assert!(results.documents[0].score < 1.0);

    // Search with non-applied deletions (seq), everything present
    let searcher = VectorSearcher::open(
        config.clone(),
        TestOpener::new(
            vec![(segment_meta.clone(), 1i64.into())],
            vec![
                ("00112233445566778899aabbccddeeff/a/title".into(), 1u64.into()),
                ("00112233445566778899aabbccddeeff/f/file".into(), 1u64.into()),
            ],
        ),
    )?;
    let search_for = vector_for("dog");
    let results = searcher.search(
        &VectorSearchRequest {
            vector: search_for.clone(),
            result_per_page: 10,
            with_duplicates: true,
            ..Default::default()
        },
        &PrefilterResult::All,
    )?;

    assert_eq!(results.documents.len(), 4);
    assert!(results.documents[0].score > 0.9999);

    // Search with malformed deletions (seq), everything present and no crashes
    let searcher = VectorSearcher::open(
        config.clone(),
        TestOpener::new(
            vec![(segment_meta.clone(), 1i64.into())],
            vec![
                ("fake/a/title".into(), 2u64.into()),
                ("00112233445566778899aabbccddeeff/not/file".into(), 2u64.into()),
            ],
        ),
    )?;
    let search_for = vector_for("dog");
    let results = searcher.search(
        &VectorSearchRequest {
            vector: search_for.clone(),
            result_per_page: 10,
            with_duplicates: true,
            ..Default::default()
        },
        &PrefilterResult::All,
    )?;

    assert_eq!(results.documents.len(), 4);
    assert!(results.documents[0].score > 0.9999);

    Ok(())
}

#[test]
fn test_relations_merge() -> anyhow::Result<()> {
    let config = VectorConfig::for_relations(VectorType::DenseF32 { dimension: DIMENSION });

    let resource1 = Resource {
        resource: Some(ResourceId {
            uuid: "00112233445566778899aabbccddeeff".into(),
            ..Default::default()
        }),
        field_relations: [
            (
                "a/title".into(),
                IndexRelations {
                    relations: vec![relation("dog", "bigger than", "cat")],
                },
            ),
            (
                "f/file".into(),
                IndexRelations {
                    relations: vec![relation("dog", "bigger than", "fish")],
                },
            ),
        ]
        .into(),
        relation_node_vectors: vec![node_vector("dog"), node_vector("cat"), node_vector("fish")],
        ..Default::default()
    };

    let segment_dir1 = tempdir()?;
    let segment_meta1 = VectorIndexer
        .index_resource(segment_dir1.path(), &config, &resource1, "default", true)?
        .unwrap();
    assert_eq!(segment_meta1.records, 3);

    let resource2 = Resource {
        resource: Some(ResourceId {
            uuid: "ffeeddccbbaa99887766554433221100".into(),
            ..Default::default()
        }),
        field_relations: [
            (
                "a/title".into(),
                IndexRelations {
                    relations: vec![relation("dog", "bigger than", "cat")],
                },
            ),
            (
                "f/file".into(),
                IndexRelations {
                    relations: vec![relation("sheep", "bigger than", "rat")],
                },
            ),
        ]
        .into(),
        relation_node_vectors: vec![
            node_vector("dog"),
            node_vector("cat"),
            node_vector("sheep"),
            node_vector("rat"),
        ],
        ..Default::default()
    };

    let segment_dir2 = tempdir()?;
    let segment_meta2 = VectorIndexer
        .index_resource(segment_dir2.path(), &config, &resource2, "default", true)?
        .unwrap();
    assert_eq!(segment_meta2.records, 4);

    // Merge without deletions
    let segment_dir_merge = tempdir()?;
    let merged_meta = VectorIndexer.merge(
        segment_dir_merge.path(),
        config.clone(),
        TestOpener::new(
            vec![
                (segment_meta1.clone(), 1i64.into()),
                (segment_meta2.clone(), 2i64.into()),
            ],
            vec![],
        ),
    )?;
    // Vectors are deduplicated, make sure with search
    assert_eq!(merged_meta.records, 5);

    let searcher = VectorSearcher::open(
        config.clone(),
        TestOpener::new(vec![(merged_meta.clone(), 1i64.into())], vec![]),
    )?;

    let results = searcher.search(
        &VectorSearchRequest {
            vector: vector_for("dog"),
            result_per_page: 10,
            with_duplicates: true,
            ..Default::default()
        },
        &PrefilterResult::All,
    )?;

    assert_eq!(results.documents.len(), 5);
    assert_eq!(results.documents[0].score, 1.0);
    assert!(results.documents[1].score < 1.0);

    // Deleting one title does nothing, as the other segment has the same entity in the same field
    let searcher = VectorSearcher::open(
        config.clone(),
        TestOpener::new(
            vec![(merged_meta.clone(), 1i64.into())],
            vec![("00112233445566778899aabbccddeeff/a/title".into(), 2u64.into())],
        ),
    )?;

    let results = searcher.search(
        &VectorSearchRequest {
            vector: vector_for("cat"),
            result_per_page: 10,
            with_duplicates: true,
            ..Default::default()
        },
        &PrefilterResult::All,
    )?;

    assert_eq!(results.documents.len(), 5);
    assert_eq!(results.documents[0].score, 1.0);
    assert!(results.documents[1].score < 1.0);

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

    let results = searcher.search(
        &VectorSearchRequest {
            vector: vector_for("cat"),
            result_per_page: 10,
            with_duplicates: true,
            ..Default::default()
        },
        &PrefilterResult::All,
    )?;

    assert_eq!(results.documents.len(), 4);
    assert!(results.documents[0].score < 1.0);

    Ok(())
}

#[test]
fn test_relations_merge_deletions() -> anyhow::Result<()> {
    let config = VectorConfig::for_relations(VectorType::DenseF32 { dimension: DIMENSION });

    let resource1 = Resource {
        resource: Some(ResourceId {
            uuid: "00112233445566778899aabbccddeeff".into(),
            ..Default::default()
        }),
        field_relations: [
            (
                "a/title".into(),
                IndexRelations {
                    relations: vec![relation("dog", "bigger than", "cat")],
                },
            ),
            (
                "f/file".into(),
                IndexRelations {
                    relations: vec![relation("dog", "bigger than", "fish")],
                },
            ),
        ]
        .into(),
        relation_node_vectors: vec![node_vector("dog"), node_vector("cat"), node_vector("fish")],
        ..Default::default()
    };

    let segment_dir1 = tempdir()?;
    let segment_meta1 = VectorIndexer
        .index_resource(segment_dir1.path(), &config, &resource1, "default", true)?
        .unwrap();
    assert_eq!(segment_meta1.records, 3);

    let resource2 = Resource {
        resource: Some(ResourceId {
            uuid: "ffeeddccbbaa99887766554433221100".into(),
            ..Default::default()
        }),
        field_relations: [
            (
                "a/title".into(),
                IndexRelations {
                    relations: vec![relation("dog", "bigger than", "cat")],
                },
            ),
            (
                "f/file".into(),
                IndexRelations {
                    relations: vec![relation("sheep", "bigger than", "rat")],
                },
            ),
        ]
        .into(),
        relation_node_vectors: vec![
            node_vector("dog"),
            node_vector("cat"),
            node_vector("sheep"),
            node_vector("rat"),
        ],
        ..Default::default()
    };

    let segment_dir2 = tempdir()?;
    let segment_meta2 = VectorIndexer
        .index_resource(segment_dir2.path(), &config, &resource2, "default", true)?
        .unwrap();
    assert_eq!(segment_meta2.records, 4);

    // Merge without deletions
    let segment_dir_merge = tempdir()?;
    let merged_meta = VectorIndexer.merge(
        segment_dir_merge.path(),
        config.clone(),
        TestOpener::new(
            vec![
                (segment_meta1.clone(), 1i64.into()),
                (segment_meta2.clone(), 2i64.into()),
            ],
            vec![],
        ),
    )?;
    assert_eq!(merged_meta.records, 5);

    // Delete one titles during merge, all entities remain
    let segment_dir_merge = tempdir()?;
    let merged_meta = VectorIndexer.merge(
        segment_dir_merge.path(),
        config.clone(),
        TestOpener::new(
            vec![
                (segment_meta1.clone(), 1i64.into()),
                (segment_meta2.clone(), 2i64.into()),
            ],
            vec![("00112233445566778899aabbccddeeff/a/title".into(), 3u64.into())],
        ),
    )?;
    assert_eq!(merged_meta.records, 5);

    // Delete both titles during merge, cat disappears
    let segment_dir_merge = tempdir()?;
    let merged_meta = VectorIndexer.merge(
        segment_dir_merge.path(),
        config.clone(),
        TestOpener::new(
            vec![
                (segment_meta1.clone(), 1i64.into()),
                (segment_meta2.clone(), 2i64.into()),
            ],
            vec![
                ("00112233445566778899aabbccddeeff/a/title".into(), 3u64.into()),
                ("ffeeddccbbaa99887766554433221100/a/title".into(), 3u64.into()),
            ],
        ),
    )?;
    assert_eq!(merged_meta.records, 4);

    // Delete both titles during merge with exact Seqs, cat disappears
    let segment_dir_merge = tempdir()?;
    let merged_meta = VectorIndexer.merge(
        segment_dir_merge.path(),
        config.clone(),
        TestOpener::new(
            vec![
                (segment_meta1.clone(), 1i64.into()),
                (segment_meta2.clone(), 2i64.into()),
            ],
            vec![
                ("00112233445566778899aabbccddeeff/a/title".into(), 2u64.into()),
                ("ffeeddccbbaa99887766554433221100/a/title".into(), 3u64.into()),
            ],
        ),
    )?;
    assert_eq!(merged_meta.records, 4);

    // Delete both titles during merge with old Seqs, all remain
    let segment_dir_merge = tempdir()?;
    let merged_meta = VectorIndexer.merge(
        segment_dir_merge.path(),
        config.clone(),
        TestOpener::new(
            vec![
                (segment_meta1.clone(), 1i64.into()),
                (segment_meta2.clone(), 2i64.into()),
            ],
            vec![
                ("00112233445566778899aabbccddeeff/a/title".into(), 2u64.into()),
                ("ffeeddccbbaa99887766554433221100/a/title".into(), 2u64.into()),
            ],
        ),
    )?;
    assert_eq!(merged_meta.records, 5);
    Ok(())
}

#[test]
fn test_relations_merge_updates() -> anyhow::Result<()> {
    let config = VectorConfig::for_relations(VectorType::DenseF32 { dimension: DIMENSION });

    let resource1 = Resource {
        resource: Some(ResourceId {
            uuid: "00112233445566778899aabbccddeeff".into(),
            ..Default::default()
        }),
        field_relations: [
            (
                "a/title".into(),
                IndexRelations {
                    relations: vec![relation("dog", "bigger than", "cat")],
                },
            ),
            (
                "f/file".into(),
                IndexRelations {
                    relations: vec![relation("dog", "bigger than", "fish")],
                },
            ),
        ]
        .into(),
        relation_node_vectors: vec![node_vector("dog"), node_vector("cat"), node_vector("fish")],
        ..Default::default()
    };

    let segment_dir1 = tempdir()?;
    let segment_meta1 = VectorIndexer
        .index_resource(segment_dir1.path(), &config, &resource1, "default", true)?
        .unwrap();
    assert_eq!(segment_meta1.records, 3);

    let resource1u = Resource {
        resource: Some(ResourceId {
            uuid: "00112233445566778899aabbccddeeff".into(),
            ..Default::default()
        }),
        field_relations: [
            (
                "a/title".into(),
                IndexRelations {
                    relations: vec![relation("my dog", "bigger than", "my cat")],
                },
            ),
            (
                "f/file".into(),
                IndexRelations {
                    relations: vec![relation("my dog", "bigger than", "my fish")],
                },
            ),
        ]
        .into(),
        relation_node_vectors: vec![node_vector("my dog"), node_vector("my cat"), node_vector("my fish")],
        ..Default::default()
    };

    let segment_dir1u = tempdir()?;
    let segment_meta1u = VectorIndexer
        .index_resource(segment_dir1u.path(), &config, &resource1u, "default", true)?
        .unwrap();
    assert_eq!(segment_meta1.records, 3);

    // Simulate updates, merge the same segment twice, with deletions on the second one
    let segment_dir_merge = tempdir()?;
    let merged_meta = VectorIndexer.merge(
        segment_dir_merge.path(),
        config.clone(),
        TestOpener::new(
            vec![
                (segment_meta1.clone(), 1i64.into()),
                (segment_meta1u.clone(), 2i64.into()),
            ],
            vec![
                ("00112233445566778899aabbccddeeff/a/title".into(), 2u64.into()),
                ("00112233445566778899aabbccddeeff/f/file".into(), 2u64.into()),
            ],
        ),
    )?;
    assert_eq!(merged_meta.records, 3);

    let searcher = VectorSearcher::open(
        config.clone(),
        TestOpener::new(vec![(merged_meta.clone(), 1i64.into())], vec![]),
    )?;
    let search_for = vector_for("my dog");
    let results = searcher.search(
        &VectorSearchRequest {
            vector: search_for.clone(),
            result_per_page: 10,
            with_duplicates: true,
            ..Default::default()
        },
        &PrefilterResult::All,
    )?;

    assert_eq!(results.documents.len(), 3);
    assert!(results.documents[0].score > 0.9999);
    let mut results_names = results
        .documents
        .iter()
        .map(|d| &d.doc_id.as_ref().unwrap().id)
        .collect::<Vec<_>>();
    results_names.sort();
    assert_eq!(results_names, vec!["my cat", "my dog", "my fish"]);

    // Bad merge without deletion, all data from both segment appears
    let segment_dir_merge = tempdir()?;
    let merged_meta = VectorIndexer.merge(
        segment_dir_merge.path(),
        config.clone(),
        TestOpener::new(
            vec![
                (segment_meta1.clone(), 1i64.into()),
                (segment_meta1u.clone(), 2i64.into()),
            ],
            vec![],
        ),
    )?;
    assert_eq!(merged_meta.records, 6);

    let searcher = VectorSearcher::open(
        config.clone(),
        TestOpener::new(vec![(merged_meta.clone(), 1i64.into())], vec![]),
    )?;
    let search_for = vector_for("my dog");
    let results = searcher.search(
        &VectorSearchRequest {
            vector: search_for.clone(),
            result_per_page: 10,
            with_duplicates: true,
            ..Default::default()
        },
        &PrefilterResult::All,
    )?;

    assert_eq!(results.documents.len(), 6);
    assert!(results.documents[0].score > 0.9999);
    let mut results_names = results
        .documents
        .iter()
        .map(|d| &d.doc_id.as_ref().unwrap().id)
        .collect::<Vec<_>>();
    results_names.sort();
    assert_eq!(results_names, vec!["cat", "dog", "fish", "my cat", "my dog", "my fish"]);

    // One field updated, other deleted
    let segment_dir_merge = tempdir()?;
    let merged_meta = VectorIndexer.merge(
        segment_dir_merge.path(),
        config.clone(),
        TestOpener::new(
            vec![
                (segment_meta1.clone(), 1i64.into()),
                (segment_meta1u.clone(), 2i64.into()),
            ],
            vec![
                ("00112233445566778899aabbccddeeff/a/title".into(), 2u64.into()),
                ("00112233445566778899aabbccddeeff/f/file".into(), 2u64.into()),
                ("00112233445566778899aabbccddeeff/f/file".into(), 3u64.into()),
            ],
        ),
    )?;
    assert_eq!(merged_meta.records, 2);

    let searcher = VectorSearcher::open(
        config.clone(),
        TestOpener::new(vec![(merged_meta.clone(), 1i64.into())], vec![]),
    )?;
    let search_for = vector_for("my dog");
    let results = searcher.search(
        &VectorSearchRequest {
            vector: search_for.clone(),
            result_per_page: 10,
            with_duplicates: true,
            ..Default::default()
        },
        &PrefilterResult::All,
    )?;

    assert_eq!(results.documents.len(), 2);
    assert!(results.documents[0].score > 0.9999);
    let mut results_names = results
        .documents
        .iter()
        .map(|d| &d.doc_id.as_ref().unwrap().id)
        .collect::<Vec<_>>();
    results_names.sort();
    assert_eq!(results_names, vec!["my cat", "my dog"]);

    Ok(())
}
