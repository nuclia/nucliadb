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
    IndexRelation, IndexRelations, Relation, RelationEdgeVector, RelationEdgeVectors, RelationNode, RelationNodeVector,
    RelationNodeVectors, Resource, ResourceId, relation::RelationType, relation_node::NodeType,
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

fn node_vector(value: &str, field_id: &str) -> RelationNodeVector {
    RelationNodeVector {
        node_value: value.into(),
        vector: vector_for(value),
        field_id: field_id.into(),
    }
}

#[test]
fn test_relations_deletion() -> anyhow::Result<()> {
    let config = VectorConfig::for_relation_nodes(VectorType::DenseF32 { dimension: DIMENSION });

    // Each node gets a separate vector per field it appears in
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
        relation_node_vectors: [(
            "default".to_string(),
            RelationNodeVectors {
                vectors: vec![
                    // a/title nodes
                    node_vector("dog", "a/title"),
                    node_vector("cat", "a/title"),
                    // f/file nodes
                    node_vector("dog", "f/file"),
                    node_vector("fish", "f/file"),
                    // f/other nodes
                    node_vector("sheep", "f/other"),
                    node_vector("fish", "f/other"),
                ],
            },
        )]
        .into(),
        ..Default::default()
    };

    let segment_dir = tempdir()?;
    let segment_meta = VectorIndexer
        .index_resource(segment_dir.path(), &config, &resource, "default", true)?
        .unwrap();
    // 6 vectors total (one per field occurrence)
    assert_eq!(segment_meta.records, 6);

    // Search without deletions - results are deduplicated by node_value key
    // Unique node values: dog, cat, fish, sheep
    let searcher = VectorSearcher::open(
        config.clone(),
        TestOpener::new(vec![(segment_meta.clone(), 1i64.into())], vec![]),
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
    assert_eq!(results.documents.len(), 4);
    assert!(results.documents[0].score > 0.9999);

    // Delete a/title: removes dog(a/title) and cat(a/title)
    // Remaining: dog(f/file), fish(f/file), sheep(f/other), fish(f/other)
    // Unique node values: dog, fish, sheep → 3 results
    let searcher = VectorSearcher::open(
        config.clone(),
        TestOpener::new(
            vec![(segment_meta.clone(), 1i64.into())],
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
    assert_eq!(results.documents.len(), 3);
    // cat is gone
    assert!(results.documents[0].score < 1.0);

    // Delete a/title and f/file: removes dog(a/title), cat(a/title), dog(f/file), fish(f/file)
    // Remaining: sheep(f/other), fish(f/other)
    // Unique node values: sheep, fish → 2 results
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
    let results = searcher.search(
        &VectorSearchRequest {
            vector: vector_for("dog"),
            result_per_page: 10,
            with_duplicates: true,
            ..Default::default()
        },
        &PrefilterResult::All,
    )?;
    assert_eq!(results.documents.len(), 2);
    // dog is gone
    assert!(results.documents[0].score < 1.0);

    // Non-applied deletions (seq too old), everything present
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
    let results = searcher.search(
        &VectorSearchRequest {
            vector: vector_for("dog"),
            result_per_page: 10,
            with_duplicates: true,
            ..Default::default()
        },
        &PrefilterResult::All,
    )?;
    assert_eq!(results.documents.len(), 4);
    assert!(results.documents[0].score > 0.9999);

    // Malformed deletions, everything present and no crashes
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
    let results = searcher.search(
        &VectorSearchRequest {
            vector: vector_for("dog"),
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
    let config = VectorConfig::for_relation_nodes(VectorType::DenseF32 { dimension: DIMENSION });

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
        relation_node_vectors: [(
            "default".to_string(),
            RelationNodeVectors {
                vectors: vec![
                    node_vector("dog", "a/title"),
                    node_vector("cat", "a/title"),
                    node_vector("dog", "f/file"),
                    node_vector("fish", "f/file"),
                ],
            },
        )]
        .into(),
        ..Default::default()
    };

    let segment_dir1 = tempdir()?;
    let segment_meta1 = VectorIndexer
        .index_resource(segment_dir1.path(), &config, &resource1, "default", true)?
        .unwrap();
    assert_eq!(segment_meta1.records, 4);

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
        relation_node_vectors: [(
            "default".to_string(),
            RelationNodeVectors {
                vectors: vec![
                    node_vector("dog", "a/title"),
                    node_vector("cat", "a/title"),
                    node_vector("sheep", "f/file"),
                    node_vector("rat", "f/file"),
                ],
            },
        )]
        .into(),
        ..Default::default()
    };

    let segment_dir2 = tempdir()?;
    let segment_meta2 = VectorIndexer
        .index_resource(segment_dir2.path(), &config, &resource2, "default", true)?
        .unwrap();
    assert_eq!(segment_meta2.records, 4);

    // Merge without deletions - no deduplication, all 8 vectors kept
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
    assert_eq!(merged_meta.records, 8);

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
    // Unique node values: dog, cat, fish, sheep, rat → 5 results (deduped by key in search)
    assert_eq!(results.documents.len(), 5);
    assert!(results.documents[0].score > 0.9999);

    // Delete one resource's title: removes dog(uuid1/a/title) and cat(uuid1/a/title)
    // Other resource still has dog(uuid2/a/title), cat(uuid2/a/title)
    // All unique node values still present
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
    // cat still present via uuid2's a/title
    assert_eq!(results.documents.len(), 5);
    assert!(results.documents[0].score > 0.9999);

    // Delete both resources' titles: removes all a/title vectors
    // Remaining: dog(uuid1/f/file), fish(uuid1/f/file), sheep(uuid2/f/file), rat(uuid2/f/file)
    // Unique node values: dog, fish, sheep, rat → 4 results. cat is gone.
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
    let config = VectorConfig::for_relation_nodes(VectorType::DenseF32 { dimension: DIMENSION });

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
        relation_node_vectors: [(
            "default".to_string(),
            RelationNodeVectors {
                vectors: vec![
                    node_vector("dog", "a/title"),
                    node_vector("cat", "a/title"),
                    node_vector("dog", "f/file"),
                    node_vector("fish", "f/file"),
                ],
            },
        )]
        .into(),
        ..Default::default()
    };

    let segment_dir1 = tempdir()?;
    let segment_meta1 = VectorIndexer
        .index_resource(segment_dir1.path(), &config, &resource1, "default", true)?
        .unwrap();
    assert_eq!(segment_meta1.records, 4);

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
        relation_node_vectors: [(
            "default".to_string(),
            RelationNodeVectors {
                vectors: vec![
                    node_vector("dog", "a/title"),
                    node_vector("cat", "a/title"),
                    node_vector("sheep", "f/file"),
                    node_vector("rat", "f/file"),
                ],
            },
        )]
        .into(),
        ..Default::default()
    };

    let segment_dir2 = tempdir()?;
    let segment_meta2 = VectorIndexer
        .index_resource(segment_dir2.path(), &config, &resource2, "default", true)?
        .unwrap();
    assert_eq!(segment_meta2.records, 4);

    // Merge without deletions - 8 vectors total
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
    assert_eq!(merged_meta.records, 8);

    // Delete one title during merge
    // Removes uuid1's a/title vectors (dog, cat from uuid1)
    // Remaining: 6 vectors
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
    assert_eq!(merged_meta.records, 6);

    // Delete both titles during merge
    // Removes all a/title vectors from both resources (4 vectors removed)
    // Remaining: dog(uuid1/f/file), fish(uuid1/f/file), sheep(uuid2/f/file), rat(uuid2/f/file) → 4 vectors
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

    // Delete both titles with exact Seqs
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

    // Delete both titles with old Seqs — deletions don't apply, all remain
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
                ("00112233445566778899aabbccddeeff/a/title".into(), 1u64.into()),
                ("ffeeddccbbaa99887766554433221100/a/title".into(), 1u64.into()),
            ],
        ),
    )?;
    assert_eq!(merged_meta.records, 8);

    Ok(())
}

#[test]
fn test_relations_merge_updates() -> anyhow::Result<()> {
    let config = VectorConfig::for_relation_nodes(VectorType::DenseF32 { dimension: DIMENSION });

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
        relation_node_vectors: [(
            "default".to_string(),
            RelationNodeVectors {
                vectors: vec![
                    node_vector("dog", "a/title"),
                    node_vector("cat", "a/title"),
                    node_vector("dog", "f/file"),
                    node_vector("fish", "f/file"),
                ],
            },
        )]
        .into(),
        ..Default::default()
    };

    let segment_dir1 = tempdir()?;
    let segment_meta1 = VectorIndexer
        .index_resource(segment_dir1.path(), &config, &resource1, "default", true)?
        .unwrap();
    assert_eq!(segment_meta1.records, 4);

    // Updated version of the same resource
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
        relation_node_vectors: [(
            "default".to_string(),
            RelationNodeVectors {
                vectors: vec![
                    node_vector("my dog", "a/title"),
                    node_vector("my cat", "a/title"),
                    node_vector("my dog", "f/file"),
                    node_vector("my fish", "f/file"),
                ],
            },
        )]
        .into(),
        ..Default::default()
    };

    let segment_dir1u = tempdir()?;
    let segment_meta1u = VectorIndexer
        .index_resource(segment_dir1u.path(), &config, &resource1u, "default", true)?
        .unwrap();
    assert_eq!(segment_meta1u.records, 4);

    // Simulate update: merge both versions with deletions on old fields
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
    // Old segment's 4 vectors deleted, new segment's 4 remain
    assert_eq!(merged_meta.records, 4);

    let searcher = VectorSearcher::open(
        config.clone(),
        TestOpener::new(vec![(merged_meta.clone(), 1i64.into())], vec![]),
    )?;
    let results = searcher.search(
        &VectorSearchRequest {
            vector: vector_for("my dog"),
            result_per_page: 10,
            with_duplicates: true,
            ..Default::default()
        },
        &PrefilterResult::All,
    )?;
    // Unique node values: my dog, my cat, my fish → 3
    assert_eq!(results.documents.len(), 3);
    assert!(results.documents[0].score > 0.9999);
    let mut results_names = results
        .documents
        .iter()
        .map(|d| &d.doc_id.as_ref().unwrap().id)
        .collect::<Vec<_>>();
    results_names.sort();
    assert_eq!(results_names, vec!["my cat", "my dog", "my fish"]);

    // Bad merge without deletion: all data from both segments
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
    assert_eq!(merged_meta.records, 8);

    let searcher = VectorSearcher::open(
        config.clone(),
        TestOpener::new(vec![(merged_meta.clone(), 1i64.into())], vec![]),
    )?;
    let results = searcher.search(
        &VectorSearchRequest {
            vector: vector_for("my dog"),
            result_per_page: 10,
            with_duplicates: true,
            ..Default::default()
        },
        &PrefilterResult::All,
    )?;
    // Unique node values: dog, cat, fish, my dog, my cat, my fish → 6
    assert_eq!(results.documents.len(), 6);
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
    // Old a/title and f/file deleted (4 removed from seg1)
    // New f/file deleted (2 removed from seg1u: my dog(f/file), my fish(f/file))
    // Remaining from seg1u: my dog(a/title), my cat(a/title) → 2
    assert_eq!(merged_meta.records, 2);

    let searcher = VectorSearcher::open(
        config.clone(),
        TestOpener::new(vec![(merged_meta.clone(), 1i64.into())], vec![]),
    )?;
    let results = searcher.search(
        &VectorSearchRequest {
            vector: vector_for("my dog"),
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

#[test]
fn test_relations_labels() -> anyhow::Result<()> {
    // Basic test for relation labels (edges)
    let config = VectorConfig::for_relation_edges(VectorType::DenseF32 { dimension: 4 });

    let resource = Resource {
        resource: Some(ResourceId {
            uuid: "00112233445566778899aabbccddeeff".into(),
            ..Default::default()
        }),
        field_relations: [
            (
                "a/title".into(),
                IndexRelations {
                    relations: vec![relation("dog", "faster than", "cat")],
                },
            ),
            (
                "f/file".into(),
                IndexRelations {
                    relations: vec![
                        relation("dog", "bigger than", "fish"),
                        relation("albatross", "bigger than", "dove"),
                        relation("shark", "bigger than", "fish"),
                        relation("leopard", "faster than", "turtle"),
                    ],
                },
            ),
        ]
        .into(),
        relation_edge_vectors: [(
            "default".to_string(),
            RelationEdgeVectors {
                vectors: vec![
                    RelationEdgeVector {
                        relation_label: "faster than".into(),
                        vector: vec![1.0, 0.0, 0.0, 0.0],
                        field_id: "a/title".into(),
                    },
                    RelationEdgeVector {
                        relation_label: "bigger than".into(),
                        vector: vec![0.0, 1.0, 0.0, 0.0],
                        field_id: "f/file".into(),
                    },
                    RelationEdgeVector {
                        relation_label: "faster than".into(),
                        vector: vec![1.0, 0.0, 0.0, 0.0],
                        field_id: "f/file".into(),
                    },
                ],
            },
        )]
        .into(),
        ..Default::default()
    };

    let segment_dir = tempdir()?;
    let segment_meta = VectorIndexer
        .index_resource(segment_dir.path(), &config, &resource, "default", true)?
        .unwrap();
    // 3 vectors (faster than from a/title, bigger than from f/file, faster than from f/file)
    assert_eq!(segment_meta.records, 3);

    // Search: 2 unique edge labels
    let searcher = VectorSearcher::open(
        config.clone(),
        TestOpener::new(vec![(segment_meta.clone(), 1i64.into())], vec![]),
    )?;
    let results = searcher.search(
        &VectorSearchRequest {
            vector: vec![1.0, 0.0, 0.0, 0.0],
            result_per_page: 10,
            with_duplicates: true,
            ..Default::default()
        },
        &PrefilterResult::All,
    )?;
    assert_eq!(results.documents.len(), 2);
    assert!(results.documents[0].score > 0.9999);

    Ok(())
}
