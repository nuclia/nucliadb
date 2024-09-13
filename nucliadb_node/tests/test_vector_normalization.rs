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

use std::collections::HashMap;
use std::time::SystemTime;
use uuid::Uuid;

use common::NodeFixture;
use nucliadb_core::protos::prost_types::Timestamp;
use nucliadb_core::protos::NewShardRequest;
use nucliadb_protos::{nodereader, noderesources, nodewriter};
use rstest::*;
use tonic::Request;

const VECTOR_DIMENSION: usize = 10;

#[rstest]
#[tokio::test]
async fn test_vector_normalization_shard() -> Result<(), Box<dyn std::error::Error>> {
    use nucliadb_core::protos::VectorIndexConfig;

    let mut fixture = NodeFixture::new();
    fixture.with_writer().await?.with_reader().await?;
    let mut writer = fixture.writer_client();
    let mut reader = fixture.reader_client();

    // Create a shard with vector normalization

    const KBID: &str = "vector-normalization-kbid";
    #[allow(deprecated)]
    let shard = writer
        .new_shard(Request::new(NewShardRequest {
            kbid: KBID.to_string(),
            config: Some(VectorIndexConfig {
                normalize_vectors: true,
                ..Default::default()
            }),
            ..Default::default()
        }))
        .await?
        .into_inner();

    // Add a resource with vectors

    let resource = build_resource(shard.id.clone());
    let result = writer.set_resource(resource).await?;

    assert_eq!(result.get_ref().status(), nodewriter::op_status::Status::Ok);

    // Search and validate normalization
    //
    // Normalization is validated using DOT product (so no normalization is done
    // during similarity computation) and checking all vectors give the same
    // result and score 1.0

    let magnitude = f32::sqrt((17.0_f32).powi(2) * VECTOR_DIMENSION as f32);
    let query_vector = vec![17.0 / magnitude; VECTOR_DIMENSION];

    let search_request = nodereader::SearchRequest {
        shard: shard.id.clone(),
        vector: query_vector,
        with_duplicates: true,
        page_number: 0,
        result_per_page: 30,
        min_score_semantic: 0.9,
        ..Default::default()
    };
    let results = reader.search(Request::new(search_request)).await?.into_inner();

    assert!(results.vector.is_some());
    let vector_results = results.vector.unwrap();
    assert_eq!(vector_results.documents.len(), 20);
    let scores = vector_results.documents.iter().map(|result| result.score).collect::<Vec<f32>>();
    assert!(scores.iter().all(|score| *score == scores[0]));
    assert!(vector_results.documents.iter().all(|result| trunc(result.score, 5) == 1.0));

    Ok(())
}

fn build_resource(shard_id: String) -> noderesources::Resource {
    let rid = Uuid::new_v4().to_string();
    let field_id = Uuid::new_v4().to_string();

    let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap();
    let timestamp = Timestamp {
        seconds: now.as_secs() as i64,
        nanos: 0,
    };

    let mut texts = HashMap::new();
    texts.insert(
        field_id.clone(),
        noderesources::TextInformation {
            text: "Dummy text".to_string(),
            labels: vec![],
        },
    );

    let mut field_paragraphs = HashMap::new();
    for i in 1..=20 {
        let mut sentences = HashMap::new();
        sentences.insert(
            format!("{rid}/{field_id}/{i}/paragraph-{i}"),
            noderesources::VectorSentence {
                vector: vec![i as f32; VECTOR_DIMENSION],
                ..Default::default()
            },
        );
        field_paragraphs.insert(
            format!("{rid}/{field_id}/paragraph-{i}"),
            noderesources::IndexParagraph {
                start: i,
                end: i + 1,
                sentences,
                ..Default::default()
            },
        );
    }
    let mut resource_paragraphs = HashMap::new();
    resource_paragraphs.insert(
        format!("{rid}/{field_id}"),
        noderesources::IndexParagraphs {
            paragraphs: field_paragraphs,
        },
    );

    noderesources::Resource {
        shard_id: shard_id.clone(),
        resource: Some(noderesources::ResourceId {
            shard_id: shard_id.clone(),
            uuid: rid,
        }),
        status: noderesources::resource::ResourceStatus::Processed as i32,
        metadata: Some(noderesources::IndexMetadata {
            created: Some(timestamp.clone()),
            modified: Some(timestamp),
        }),
        texts,
        paragraphs: resource_paragraphs,
        ..Default::default()
    }
}

fn trunc(value: f32, digits: usize) -> f32 {
    let factor = (10 * digits) as f32;
    f32::trunc(value * factor) / factor
}
