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
fn test_index_merge_relations() -> anyhow::Result<()> {
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
        ]
        .into(),
        relation_node_vectors: vec![node_vector("dog"), node_vector("cat"), node_vector("fish")],
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
    let search_for = vector_for("dog");
    let results = searcher.search(
        &VectorSearchRequest {
            vector: search_for.clone(),
            result_per_page: 10,
            ..Default::default()
        },
        &PrefilterResult::All,
    )?;

    assert_eq!(results.documents.len(), 3);
    assert_eq!(results.documents[0].score, 1.0);
    assert!(results.documents[1].score < 1.0);

    // Search with a deletion
    let searcher = VectorSearcher::open(
        config.clone(),
        TestOpener::new(
            vec![(segment_meta.clone(), 1i64.into())],
            vec![("00112233445566778899aabbccddeeff/a/title".into(), 2u64.into())],
        ),
    )?;

    let results = searcher.search(
        &VectorSearchRequest {
            vector: search_for.clone(),
            result_per_page: 10,
            ..Default::default()
        },
        &PrefilterResult::All,
    )?;

    println!("{:?}", results);
    assert_eq!(results.documents.len(), 2);
    assert_eq!(results.documents[0].score, 1.0);
    assert!(results.documents[1].score < 1.0);

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

    // Search with a deletion that does not apply
    let searcher = VectorSearcher::open(
        config.clone(),
        TestOpener::new(
            vec![
                (segment_meta.clone(), 1i64.into()),
                (segment_meta2.clone(), 2i64.into()),
            ],
            vec![("ffeeddccbbaa99887766554433221100/f/file".into(), 2u64.into())],
        ),
    )?;

    let results = searcher.search(
        &VectorSearchRequest {
            vector: search_for.clone(),
            result_per_page: 10,
            ..Default::default()
        },
        &PrefilterResult::All,
    )?;

    assert_eq!(results.documents.len(), 5);

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

    let results = searcher.search(
        &VectorSearchRequest {
            vector: search_for.clone(),
            result_per_page: 10,
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
            vector: search_for.clone(),
            result_per_page: 10,
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
            ..Default::default()
        },
        &PrefilterResult::All,
    )?;

    assert_eq!(results.documents.len(), 4);
    assert!(results.documents[0].score < 1.0);

    // TODO: test deletions applied during merge
    let segment_dir_merge = tempdir()?;
    let merged_meta = VectorIndexer.merge(
        segment_dir_merge.path(),
        config.clone(),
        TestOpener::new(
            vec![
                (segment_meta.clone(), 1i64.into()),
                (segment_meta2.clone(), 2i64.into()),
            ],
            vec![
                ("00112233445566778899aabbccddeeff/a/fake".into(), 1u64.into()),
                ("00112233445566778899aabbccddeeff/a/title".into(), 2u64.into()),
                ("ffeeddccbbaa99887766554433221100/a/title".into(), 2u64.into()),
            ],
        ),
    )?;

    let searcher = VectorSearcher::open(
        config.clone(),
        TestOpener::new(vec![(merged_meta.clone(), 1i64.into())], vec![]),
    )?;

    let results = searcher.search(
        &VectorSearchRequest {
            vector: search_for.clone(),
            result_per_page: 10,
            ..Default::default()
        },
        &PrefilterResult::All,
    )?;

    assert_eq!(results.documents.len(), 4);
    assert_eq!(results.documents[0].score, 1.0);
    assert!(results.documents[1].score < 1.0);

    Ok(())
}
