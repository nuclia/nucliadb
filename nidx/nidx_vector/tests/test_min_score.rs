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

//! Tests for `min_score` filtering correctness.
//!
//! # Background
//!
//! When RabitQ quantization is active (Dot similarity + dimension that is a multiple of 64,
//! stored as DataStoreV2), the brute-force search path filters candidates using an *estimated*
//! upper-bound score before passing them to `rerank_top`, which computes the exact similarity.
//! The upper-bound is intentionally higher than the real score (score + quantization error), so
//! that borderline candidates are not discarded too early.
//!
//! The bug: `rerank_top` returned the top-k by real score without re-applying `min_score`.
//! Vectors whose `upper_bound >= min_score` but whose real score is `< min_score` would leak
//! into the response.
//!
//! The fix applies a `min_score` filter on the reranked results before returning them.

mod common;

use nidx_protos::VectorSentence;
use nidx_types::prefilter::PrefilterResult;
use nidx_vector::{
    VectorIndexer, VectorSearchRequest, VectorSearcher,
    config::{Similarity, VectorConfig, VectorType},
};
use tempfile::tempdir;

/// Dimension must be a multiple of 64 and similarity must be Dot to activate RabitQ quantization.
const DIMENSION: usize = 64;

/// Build a unit vector with a `1.0` at position `index` and `0.0` elsewhere.
/// These are all orthogonal, so:
///   - dot_similarity(v_i, v_i) == 1.0
///   - dot_similarity(v_i, v_j) == 0.0  for i != j
fn unit_vector(index: usize) -> Vec<f32> {
    let mut v = vec![0.0f32; DIMENSION];
    v[index] = 1.0;
    v
}

/// Verify that `min_score` is respected when the RabitQ brute-force path is used.
///
/// With orthogonal unit vectors, each candidate has an exact similarity of either 1.0 (the
/// matching vector) or 0.0 (every other vector).  By requesting `min_score = 0.5` only the
/// exact match should be returned — no zero-score vectors may slip through even if their
/// RabitQ `upper_bound` estimate happens to exceed the threshold.
#[test]
fn test_min_score_respected_with_rabitq_brute_force() -> anyhow::Result<()> {
    use common::{TestOpener, resource};

    // Dot + dimension % 64 == 0 → RabitQ is active on DataStoreV2
    let config = VectorConfig::for_paragraphs(VectorType::DenseF32 { dimension: DIMENSION });
    // `for_paragraphs` already defaults to Dot similarity, but be explicit for clarity.
    assert!(matches!(config.similarity, Similarity::Dot));
    assert!(
        config.quantizable_vectors(),
        "RabitQ must be active for this test to be meaningful"
    );

    // Index a small number of orthogonal unit vectors in a single segment.
    // Keeping the vector count small ensures the brute-force path is chosen over HNSW.
    let num_vectors: i32 = 5;
    let mut resource = resource(vec![], vec![]);
    let sentences = &mut resource
        .paragraphs
        .values_mut()
        .next()
        .unwrap()
        .paragraphs
        .values_mut()
        .next()
        .unwrap()
        .sentences;
    sentences.clear();

    let id = resource.resource.as_ref().unwrap().uuid.clone();
    for i in 0..num_vectors {
        sentences.insert(
            format!("{id}/a/title/0-{i}"),
            VectorSentence {
                vector: unit_vector(i as usize),
                metadata: None,
            },
        );
    }

    let segment_dir = tempdir()?;
    let segment_meta = VectorIndexer
        .index_resource(segment_dir.path(), &config, &resource, "default", true)?
        .unwrap();

    let searcher = VectorSearcher::open(config, TestOpener::new(vec![(segment_meta, 1i64.into())], vec![]))?;

    // Query with unit_vector(0): exact match scores 1.0, all others score 0.0.
    // A min_score of 0.5 should return only the exact match.
    let results = searcher.search(
        &VectorSearchRequest {
            vector: unit_vector(0),
            result_per_page: num_vectors,
            min_score: 0.5,
            ..Default::default()
        },
        &PrefilterResult::All,
    )?;

    assert_eq!(
        results.documents.len(),
        1,
        "Only the exact match (score 1.0) should pass min_score=0.5; got {} results",
        results.documents.len()
    );

    let score = results.documents[0].score;
    assert!(
        score >= 0.5,
        "Returned document has score {score} which is below min_score=0.5"
    );
    assert!(
        score > 0.99,
        "The only returned document should be the exact match (score ≈ 1.0), got {score}"
    );

    // Sanity check: requesting min_score=-1.0 returns all vectors.
    let results_no_filter = searcher.search(
        &VectorSearchRequest {
            vector: unit_vector(0),
            result_per_page: num_vectors,
            min_score: -1.0,
            ..Default::default()
        },
        &PrefilterResult::All,
    )?;
    assert_eq!(
        results_no_filter.documents.len(),
        num_vectors as usize,
        "Without a min_score filter all {num_vectors} vectors should be returned"
    );

    // All results from any search must respect the requested min_score.
    for doc in &results.documents {
        assert!(
            doc.score >= 0.5,
            "Document {} has score {} which is below min_score=0.5",
            doc.doc_id.as_ref().map(|d| d.id.as_str()).unwrap_or("?"),
            doc.score,
        );
    }

    Ok(())
}
