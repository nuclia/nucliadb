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

use nidx_protos::VectorSentence;
use nidx_vector::config::{VectorCardinality, VectorConfig, VectorType};
use tempfile::tempdir;

mod common;

#[test]
fn test_maxsim() -> anyhow::Result<()> {
    let query = [[1.0, 0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 1.0, 0.0]]; // 0, 3
    let d0 = [
        [0.0, 1.0, 0.0, 0.0, 0.0],
        [0.0, 0.0, 1.0, 0.0, 0.0],
        [0.0, 0.0, 0.0, 0.0, 1.0],
    ]; // 1, 2, 4 (score 0)
    let d1 = [
        [1.0, 0.0, 0.0, 0.0, 0.0],
        [0.0, 1.0, 0.0, 0.0, 0.0],
        [0.0, 0.0, 1.0, 0.0, 0.0],
    ]; // 0, 1, 2 (score 1)
    let d2 = [
        [1.0, 0.0, 0.0, 0.0, 0.0],
        [0.0, 0.0, 1.0, 0.0, 0.0],
        [0.0, 0.0, 0.0, 1.0, 0.0],
    ]; // 0, 2, 3 (score 2)

    use common::{TestOpener, resource};
    use nidx_types::prefilter::PrefilterResult;
    use nidx_vector::{VectorIndexer, VectorSearchRequest, VectorSearcher};

    let mut config = VectorConfig::for_paragraphs(VectorType::DenseF32 { dimension: 5 });
    config.vector_cardinality = VectorCardinality::Multi;

    // Creates a resource with some orthogonal vectors, to test search
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
    let id = &resource.resource.as_ref().unwrap().uuid;
    sentences.insert(
        format!("{id}/f/d0/0-123"),
        VectorSentence {
            vector: d0.as_flattened().to_vec(),
            metadata: None,
        },
    );
    sentences.insert(
        format!("{id}/f/d1/0-123"),
        VectorSentence {
            vector: d1.as_flattened().to_vec(),
            metadata: None,
        },
    );
    sentences.insert(
        format!("{id}/f/d2/0-123"),
        VectorSentence {
            vector: d2.as_flattened().to_vec(),
            metadata: None,
        },
    );

    let segment_dir = tempdir()?;
    let segment_meta = VectorIndexer
        .index_resource(segment_dir.path(), &config, &resource, "default", true)?
        .unwrap();

    // Get best match
    let searcher = VectorSearcher::open(
        config.clone(),
        TestOpener::new(vec![(segment_meta, 1i64.into())], vec![]),
    )?;
    let search_for = query.as_flattened().to_vec();
    let results = searcher.search(
        &VectorSearchRequest {
            vector: search_for.clone(),
            result_per_page: 1,
            min_score: -10.0,
            ..Default::default()
        },
        &PrefilterResult::All,
    )?;
    assert_eq!(results.documents.len(), 1);
    assert_eq!(
        results.documents[0].doc_id.as_ref().unwrap().id,
        format!("{id}/f/d2/0-123")
    );
    assert_eq!(results.documents[0].score, 2.0);

    // Get best match by min_score (this makes sure min_score is not applied to individual vectors)
    let results = searcher.search(
        &VectorSearchRequest {
            vector: search_for.clone(),
            result_per_page: 10,
            min_score: 1.5,
            ..Default::default()
        },
        &PrefilterResult::All,
    )?;
    assert_eq!(results.documents.len(), 1);
    assert_eq!(
        results.documents[0].doc_id.as_ref().unwrap().id,
        format!("{id}/f/d2/0-123")
    );
    assert_eq!(results.documents[0].score, 2.0);

    // Get top matches
    let results = searcher.search(
        &VectorSearchRequest {
            vector: search_for.clone(),
            result_per_page: 2,
            min_score: -10.0,
            ..Default::default()
        },
        &PrefilterResult::All,
    )?;
    assert_eq!(results.documents.len(), 2);
    assert_eq!(
        results.documents[0].doc_id.as_ref().unwrap().id,
        format!("{id}/f/d2/0-123")
    );
    assert_eq!(results.documents[0].score, 2.0);
    assert_eq!(
        results.documents[1].doc_id.as_ref().unwrap().id,
        format!("{id}/f/d1/0-123")
    );
    assert_eq!(results.documents[1].score, 1.0);

    Ok(())
}
