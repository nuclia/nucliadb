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

use nidx_protos::VectorSentence;
use nidx_vector::config::{Similarity, VectorCardinality, VectorConfig, VectorType};
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

    let config = VectorConfig {
        similarity: Similarity::Dot,
        vector_type: VectorType::DenseF32 { dimension: 5 },
        normalize_vectors: false,
        vector_cardinality: VectorCardinality::Multi,
        flags: vec![],
    };

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
