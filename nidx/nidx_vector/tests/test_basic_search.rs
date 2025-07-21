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

use nidx_protos::VectorSentence;
use nidx_types::{
    prefilter::FieldId,
    query_language::{BooleanExpression, BooleanOperation, Operator},
};
use nidx_vector::config::*;
use rstest::rstest;
use tempfile::tempdir;

fn sentence(index: usize) -> VectorSentence {
    let mut vector: Vec<f32> = [0.0; DIMENSION].into();
    vector[index] = 1.0;

    VectorSentence { vector, metadata: None }
}

const DIMENSION: usize = 64;

#[rstest]
fn test_basic_search(
    #[values(Similarity::Dot, Similarity::Cosine)] similarity: Similarity,
    #[values(VectorType::DenseF32 { dimension: DIMENSION })] vector_type: VectorType,
    #[values(false, true)] data_store_v2: bool,
) -> anyhow::Result<()> {
    use common::{TestOpener, resource};
    use nidx_types::prefilter::PrefilterResult;
    use nidx_vector::{VectorIndexer, VectorSearchRequest, VectorSearcher};

    let flags = if data_store_v2 {
        vec![]
    } else {
        vec![flags::FORCE_DATA_STORE_V1.to_string()]
    };

    let config = VectorConfig {
        similarity,
        vector_type,
        normalize_vectors: false,
        flags,
        vector_cardinality: VectorCardinality::Single,
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
    for i in 0..DIMENSION {
        sentences.insert(format!("{id}/a/title/0-{i}"), sentence(i));
    }

    let segment_dir = tempdir()?;
    let segment_meta = VectorIndexer
        .index_resource(segment_dir.path(), &config, &resource, "default", true)?
        .unwrap();

    // Search near one specific vector
    let searcher = VectorSearcher::open(
        config.clone(),
        TestOpener::new(vec![(segment_meta, 1i64.into())], vec![]),
    )?;
    let search_for = sentence(5);
    let results = searcher.search(
        &VectorSearchRequest {
            vector: search_for.vector,
            result_per_page: 10,
            ..Default::default()
        },
        &PrefilterResult::All,
    )?;

    assert_eq!(results.documents.len(), 10);
    assert_eq!(
        results.documents[0].doc_id.as_ref().unwrap().id,
        format!("{id}/a/title/0-5")
    );
    assert_eq!(results.documents[0].score, 1.0);
    assert_eq!(results.documents[1].score, 0.0);

    // Search near a few elements
    let mut vector: Vec<f32> = [0.0; DIMENSION].into();
    vector[42] = 0.9;
    vector[43] = 0.7;
    vector[44] = 0.5;
    vector[45] = 0.3;
    let results = searcher.search(
        &VectorSearchRequest {
            vector,
            result_per_page: 10,
            ..Default::default()
        },
        &PrefilterResult::All,
    )?;

    assert_eq!(results.documents.len(), 10);
    assert_eq!(
        results.documents[0].doc_id.as_ref().unwrap().id,
        format!("{id}/a/title/0-42")
    );
    assert!(results.documents[0].score > 0.2);
    assert_eq!(
        results.documents[1].doc_id.as_ref().unwrap().id,
        format!("{id}/a/title/0-43")
    );
    assert!(results.documents[1].score > 0.2);
    assert_eq!(
        results.documents[2].doc_id.as_ref().unwrap().id,
        format!("{id}/a/title/0-44")
    );
    assert!(results.documents[2].score > 0.2);
    assert_eq!(
        results.documents[3].doc_id.as_ref().unwrap().id,
        format!("{id}/a/title/0-45")
    );
    assert!(results.documents[3].score > 0.2);
    assert_eq!(results.documents[5].score, 0.0);

    Ok(())
}

#[test]
fn test_deletions() -> anyhow::Result<()> {
    use common::{TestOpener, resource};
    use nidx_types::prefilter::PrefilterResult;
    use nidx_vector::{VectorIndexer, VectorSearchRequest, VectorSearcher};

    let config = VectorConfig {
        similarity: Similarity::Dot,
        vector_type: VectorType::DenseF32 { dimension: 4 },
        normalize_vectors: false,
        flags: vec![],
        vector_cardinality: VectorCardinality::Single,
    };

    // Creates a couple of resources
    let resource1 = resource(vec![], vec![]);
    let resource2 = resource(vec![], vec![]);

    let segment1_dir = tempdir()?;
    let segment2_dir = tempdir()?;
    let segment1 = VectorIndexer
        .index_resource(segment1_dir.path(), &config, &resource1, "default", true)?
        .unwrap();
    let segment2 = VectorIndexer
        .index_resource(segment2_dir.path(), &config, &resource2, "default", true)?
        .unwrap();

    // Search initially returns both resources
    let segments = vec![(segment1, 1i64.into()), (segment2, 2i64.into())];
    let open_config = TestOpener::new(segments.clone(), vec![]);
    let searcher = VectorSearcher::open(config.clone(), open_config)?;
    let search_request = &VectorSearchRequest {
        vector: [0.0; 4].to_vec(),
        result_per_page: 10,
        min_score: -1.0,
        ..Default::default()
    };
    let results = searcher.search(search_request, &PrefilterResult::All)?;
    assert_eq!(results.documents.len(), 2);

    // Delete a full resource, it does not appear in search
    let open_config = TestOpener::new(
        segments.clone(),
        vec![(resource1.resource.clone().unwrap().uuid, 3i64.into())],
    );
    let searcher = VectorSearcher::open(config.clone(), open_config)?;
    let results = searcher.search(search_request, &PrefilterResult::All)?;
    assert_eq!(results.documents.len(), 1);
    assert!(
        results.documents[0]
            .doc_id
            .as_ref()
            .unwrap()
            .id
            .starts_with(&resource2.resource.as_ref().unwrap().uuid)
    );

    // Delete a field, it does not appear in search
    let open_config = TestOpener::new(
        segments,
        vec![(format!("{}/a/title", resource2.resource.unwrap().uuid), 3i64.into())],
    );
    let searcher = VectorSearcher::open(config.clone(), open_config)?;
    let results = searcher.search(search_request, &PrefilterResult::All)?;
    assert_eq!(results.documents.len(), 1);
    assert!(
        results.documents[0]
            .doc_id
            .as_ref()
            .unwrap()
            .id
            .starts_with(&resource1.resource.as_ref().unwrap().uuid)
    );

    Ok(())
}

#[test]
fn test_filtered_search() -> anyhow::Result<()> {
    use common::{TestOpener, resource};
    use nidx_types::prefilter::PrefilterResult;
    use nidx_vector::{VectorIndexer, VectorSearchRequest, VectorSearcher};

    let config = VectorConfig {
        similarity: Similarity::Dot,
        vector_type: VectorType::DenseF32 { dimension: 4 },
        normalize_vectors: false,
        flags: vec![],
        vector_cardinality: VectorCardinality::Single,
    };

    // Create 4 resources
    // 0 has label 0, 8
    // 1 has labels 1, 9
    // 2 has labels 2, 8
    // 3 has labels 3, 9
    let work_path = tempdir()?;
    let resources: Vec<_> = (0..4)
        .map(|i| {
            resource(
                vec![],
                vec![
                    format!("/l/labelset/label_{}", i),
                    format!("/l/labelset/label_{}", (i % 2) + 8),
                ],
            )
        })
        .collect();
    let segments = resources
        .iter()
        .enumerate()
        .map(|(i, r)| {
            let segment_path = &work_path.path().join(i.to_string());
            std::fs::create_dir(segment_path).unwrap();
            (
                VectorIndexer
                    .index_resource(segment_path, &config, r, "default", true)
                    .unwrap()
                    .unwrap(),
                (i as i64).into(),
            )
        })
        .collect();

    let open_config = TestOpener::new(segments, vec![]);
    let searcher = VectorSearcher::open(config, open_config)?;

    let search = |filtering_formula| -> HashSet<u32> {
        // Search initially returns both resources
        let search_request = &VectorSearchRequest {
            vector: [0.0; 4].to_vec(),
            result_per_page: 10,
            min_score: -1.0,
            filtering_formula,
            ..Default::default()
        };
        let results = searcher.search(search_request, &PrefilterResult::All).unwrap();
        results
            .documents
            .iter()
            .map(|d| {
                d.labels
                    .iter()
                    .map(|l| l.split("_").last().unwrap().parse().unwrap())
                    .min()
                    .unwrap()
            })
            .collect()
    };

    // All results
    assert_eq!(search(None), [0, 1, 2, 3].into());

    // Literal: Even
    assert_eq!(
        search(Some(BooleanExpression::Literal("/l/labelset/label_8".into()))),
        [0, 2].into()
    );

    // OR: Even or 3
    assert_eq!(
        search(Some(BooleanExpression::Operation(BooleanOperation {
            operator: Operator::Or,
            operands: vec![
                BooleanExpression::Literal("/l/labelset/label_8".into()),
                BooleanExpression::Literal("/l/labelset/label_3".into()),
            ]
        }))),
        [0, 2, 3].into()
    );

    // AND: Even and 3
    assert_eq!(
        search(Some(BooleanExpression::Operation(BooleanOperation {
            operator: Operator::And,
            operands: vec![
                BooleanExpression::Literal("/l/labelset/label_8".into()),
                BooleanExpression::Literal("/l/labelset/label_3".into()),
            ]
        }))),
        [].into()
    );

    // NOT: Not 3
    assert_eq!(
        search(Some(BooleanExpression::Not(Box::new(BooleanExpression::Literal(
            "/l/labelset/label_3".into()
        ))))),
        [0, 1, 2].into()
    );

    // Combination: (even AND NOT 2) OR odd
    assert_eq!(
        search(Some(BooleanExpression::Operation(BooleanOperation {
            operator: Operator::Or,
            operands: vec![
                BooleanExpression::Operation(BooleanOperation {
                    operator: Operator::And,
                    operands: vec![
                        BooleanExpression::Literal("/l/labelset/label_8".into()),
                        BooleanExpression::Not(Box::new(BooleanExpression::Literal("/l/labelset/label_2".into()))),
                    ],
                }),
                BooleanExpression::Literal("/l/labelset/label_9".into()),
            ],
        }))),
        [0, 1, 3].into()
    );

    // Combinations with prefilter
    let search = |filtering_formula, prefilter, filter_or| -> HashSet<u32> {
        let search_request = &VectorSearchRequest {
            vector: [0.0; 4].to_vec(),
            result_per_page: 10,
            min_score: -1.0,
            filtering_formula,
            filter_or,
            ..Default::default()
        };
        let results = searcher.search(search_request, &prefilter).unwrap();
        results
            .documents
            .iter()
            .map(|d| {
                d.labels
                    .iter()
                    .map(|l| l.split("_").last().unwrap().parse().unwrap())
                    .min()
                    .unwrap()
            })
            .collect()
    };

    let resource = |r: usize| FieldId {
        resource_id: resources[r].resource.as_ref().unwrap().uuid.parse().unwrap(),
        field_id: "/a/title".into(),
    };

    // Prefilter only
    assert_eq!(
        search(None, PrefilterResult::Some(vec![resource(0)]), false),
        [0].into()
    );

    // Prefilter AND labels
    assert_eq!(
        search(
            Some(BooleanExpression::Literal("/l/labelset/label_1".into())),
            PrefilterResult::Some(vec![resource(0), resource(1)]),
            false
        ),
        [1].into()
    );

    // Prefilter OR labels
    assert_eq!(
        search(
            Some(BooleanExpression::Literal("/l/labelset/label_3".into())),
            PrefilterResult::Some(vec![resource(1)]),
            true
        ),
        [1, 3].into()
    );

    Ok(())
}
