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

use nidx_protos::{VectorSearchRequest, VectorSentence};
use nidx_vector::{config::*, VectorsContext};
use rstest::rstest;
use tempfile::tempdir;

fn sentence(index: usize) -> VectorSentence {
    let mut vector: Vec<f32> = [0.0; DIMENSION].into();
    vector[index] = 1.0;

    VectorSentence {
        vector,
        metadata: None,
    }
}

const DIMENSION: usize = 64;

#[rstest]
fn test_basic_search(
    #[values(Similarity::Dot, Similarity::Cosine)] similarity: Similarity,
    #[values(VectorType::DenseF32Unaligned, VectorType::DenseF32 { dimension: DIMENSION })] vector_type: VectorType,
) -> anyhow::Result<()> {
    use common::{resource, TestOpener};
    use nidx_vector::{VectorIndexer, VectorSearcher};

    let config = VectorConfig {
        similarity,
        vector_type,
        ..Default::default()
    };

    // Creates a resource with some orthogonal vectors, to test search
    let mut resource = resource(vec![]);
    let sentences =
        &mut resource.paragraphs.values_mut().next().unwrap().paragraphs.values_mut().next().unwrap().sentences;
    sentences.clear();
    let id = &resource.resource.as_ref().unwrap().uuid;
    for i in 0..DIMENSION {
        sentences.insert(format!("{id}/a/title/0-{i}"), sentence(i));
    }

    let segment_dir = tempdir()?;
    let segment_meta = VectorIndexer.index_resource(segment_dir.path(), &config, &resource)?.unwrap();

    // Search near one specific vector
    let reader = VectorSearcher::open(config.clone(), TestOpener::new(vec![(segment_meta, 1i64.into())], vec![]))?;
    let search_for = sentence(5);
    let results = reader.search(
        &VectorSearchRequest {
            vector: search_for.vector,
            result_per_page: 10,
            ..Default::default()
        },
        &VectorsContext::default(),
    )?;

    assert_eq!(results.documents.len(), 10);
    assert_eq!(results.documents[0].doc_id.as_ref().unwrap().id, format!("{id}/a/title/0-5"));
    assert_eq!(results.documents[0].score, 1.0);
    assert_eq!(results.documents[1].score, 0.0);

    // Search near a few elements
    let mut vector: Vec<f32> = [0.0; DIMENSION].into();
    vector[42] = 0.9;
    vector[43] = 0.7;
    vector[44] = 0.5;
    vector[45] = 0.3;
    let results = reader.search(
        &VectorSearchRequest {
            vector,
            result_per_page: 10,
            ..Default::default()
        },
        &VectorsContext::default(),
    )?;

    assert_eq!(results.documents.len(), 10);
    assert_eq!(results.documents[0].doc_id.as_ref().unwrap().id, format!("{id}/a/title/0-42"));
    assert!(results.documents[0].score > 0.2);
    assert_eq!(results.documents[1].doc_id.as_ref().unwrap().id, format!("{id}/a/title/0-43"));
    assert!(results.documents[1].score > 0.2);
    assert_eq!(results.documents[2].doc_id.as_ref().unwrap().id, format!("{id}/a/title/0-44"));
    assert!(results.documents[2].score > 0.2);
    assert_eq!(results.documents[3].doc_id.as_ref().unwrap().id, format!("{id}/a/title/0-45"));
    assert!(results.documents[3].score > 0.2);
    assert_eq!(results.documents[5].score, 0.0);

    Ok(())
}
