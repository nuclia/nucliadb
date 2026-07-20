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

use nidx_protos::{IndexParagraph, IndexParagraphs, Resource, ResourceId, VectorSentence};
use nidx_types::prefilter::PrefilterResult;
use nidx_vector::{VectorIndexer, VectorSearchRequest, VectorSearcher, config::*};
use tempfile::tempdir;

use crate::common::TestOpener;

const DIMENSION: usize = 4;

fn make_vector(index: usize) -> Vec<f32> {
    let mut v = vec![0.0f32; DIMENSION];
    v[index] = 1.0;
    v
}

/// Builds a minimal Resource with two fields (`a/title` and `t/body`), each
/// containing a single paragraph sentence.
fn make_field(uuid: &str, field_key: &str, vector: Vec<f32>) -> (String, IndexParagraphs) {
    let para_key = format!("{uuid}/{field_key}/0-10");
    (
        format!("{uuid}/{field_key}"),
        IndexParagraphs {
            paragraphs: [(
                para_key.clone(),
                IndexParagraph {
                    start: 0,
                    end: 10,
                    sentences: [(para_key, VectorSentence { vector, metadata: None })].into(),
                    ..Default::default()
                },
            )]
            .into(),
        },
    )
}

fn build_resource(uuid: &str, vector0: Vec<f32>, vector1: Vec<f32>) -> Resource {
    Resource {
        resource: Some(ResourceId {
            shard_id: String::new(),
            uuid: uuid.into(),
        }),
        paragraphs: [
            make_field(uuid, "a/title", vector0),
            make_field(uuid, "t/body", vector1),
        ]
        .into(),
        ..Default::default()
    }
}

/// Verifies that merging two segments that update a resource each are applied correctly.
/// This requires that deletions are applied in the correct order during merging.
#[test]
fn test_paragraph_merge_with_deletions() -> anyhow::Result<()> {
    let config = VectorConfig::for_paragraphs(VectorType::DenseF32 { dimension: DIMENSION });

    let uuid1 = "00112233445566778899aabbccddeeff";
    let uuid2 = "ffeeddccbbaa99887766554433221100";

    // --- Segment 1 (seq 2): resource1 / a/title, vector pointing along axis 0 ---
    // Deletion at seq 2 (same seq) for resource1/a/title simulates an atomic
    // update: delete the old field version and write the new one together.
    let resource1 = build_resource(uuid1, make_vector(0), make_vector(1));
    let segment_dir1 = tempdir()?;
    let segment_meta1 = VectorIndexer
        .index_resource(segment_dir1.path(), &config, &resource1, "default", true)?
        .unwrap();
    assert_eq!(segment_meta1.records, 2, "segment 1 should have exactly two vectors");

    // --- Segment 2 (seq 4): resource2 / a/title, vector pointing along axis 1 ---
    // Deletion at seq 4 (same seq), same rationale.
    let resource2 = build_resource(uuid2, make_vector(2), make_vector(3));
    let segment_dir2 = tempdir()?;
    let segment_meta2 = VectorIndexer
        .index_resource(segment_dir2.path(), &config, &resource2, "default", true)?
        .unwrap();
    assert_eq!(segment_meta2.records, 2, "segment 2 should have exactly two vectors");

    // --- Merge with deletions ---
    // Segments are at seq 2 and 4.  Deletions share the same seq as their
    // respective segments, so they remove any vectors for that field indexed
    // *before* that seq—none exist here.  The merged result must retain both.
    let segment_dir_merge = tempdir()?;
    let merged_meta = VectorIndexer.merge(
        segment_dir_merge.path(),
        config.clone(),
        TestOpener::new(
            vec![
                (segment_meta1.clone(), 2i64.into()),
                (segment_meta2.clone(), 4i64.into()),
            ],
            vec![
                (format!("{uuid1}/a/title"), 2u64.into()),
                (format!("{uuid1}/t/body"), 2u64.into()),
                (format!("{uuid2}/a/title"), 4u64.into()),
                (format!("{uuid2}/t/body"), 4u64.into()),
            ],
        ),
    )?;
    assert_eq!(merged_meta.records, 4, "merged segment must contain all four vectors");

    // --- Verify both vectors are searchable in the merged segment ---
    let searcher = VectorSearcher::open(
        config.clone(),
        TestOpener::new(vec![(merged_meta, 5i64.into())], vec![]),
    )?;

    // Search near vector 0 (resource1) – should be the top hit.
    let results = searcher.search(
        &VectorSearchRequest {
            vector: make_vector(0),
            result_per_page: 10,
            with_duplicates: true,
            ..Default::default()
        },
        &PrefilterResult::All,
    )?;
    assert_eq!(results.documents.len(), 4, "all four vectors should be returned");
    assert!(
        results.documents[0].score > 0.9999,
        "top result should be an exact match for vector 0"
    );

    // Search near vector 2 (resource2/a/title) – should be the top hit.
    let results = searcher.search(
        &VectorSearchRequest {
            vector: make_vector(2),
            result_per_page: 10,
            with_duplicates: true,
            ..Default::default()
        },
        &PrefilterResult::All,
    )?;
    assert_eq!(results.documents.len(), 4, "all four vectors should be returned");
    assert!(
        results.documents[0].score > 0.9999,
        "top result should be an exact match for vector 2"
    );

    // Search near vector 3 (resource2/t/body) – should be the top hit.
    let results = searcher.search(
        &VectorSearchRequest {
            vector: make_vector(3),
            result_per_page: 10,
            with_duplicates: true,
            ..Default::default()
        },
        &PrefilterResult::All,
    )?;
    assert_eq!(results.documents.len(), 4, "all four vectors should be returned");
    assert!(
        results.documents[0].score > 0.9999,
        "top result should be an exact match for vector 3"
    );

    Ok(())
}
