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

use common::services::NidxFixture;
use sqlx::PgPool;
use std::collections::HashMap;
use std::time::SystemTime;
use uuid::Uuid;

use nidx_protos::NewShardRequest;
use nidx_protos::VectorIndexConfig;
use nidx_protos::prost_types::Timestamp;
use nidx_protos::{nodereader, noderesources};

use tonic::Request;

const VECTOR_DIMENSION: usize = 10;

#[sqlx::test]
async fn test_vector_normalization_shard(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let mut fixture = NidxFixture::new(pool).await?;

    // Create a shard with vector normalization

    const KBID: &str = "aabbccddeeff11223344556677889900";
    #[allow(deprecated)]
    let shard = fixture
        .api_client
        .new_shard(Request::new(NewShardRequest {
            kbid: KBID.to_string(),
            config: Some(VectorIndexConfig {
                normalize_vectors: true,
                ..Default::default()
            }),
            vectorsets_configs: HashMap::from([(
                "english".to_string(),
                VectorIndexConfig {
                    vector_dimension: Some(VECTOR_DIMENSION as u32),
                    ..Default::default()
                },
            )]),
            ..Default::default()
        }))
        .await?
        .into_inner();

    // Add a resource with vectors

    let resource = build_resource(shard.id.clone());
    fixture.index_resource(&shard.id, resource).await?;
    fixture.wait_sync().await;

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
        vectorset: "english".to_string(),
        with_duplicates: true,
        result_per_page: 30,
        min_score_semantic: 0.9,
        ..Default::default()
    };
    let results = fixture
        .searcher_client
        .search(Request::new(search_request))
        .await?
        .into_inner();

    assert!(results.vector.is_some());
    let vector_results = results.vector.unwrap();
    assert_eq!(vector_results.documents.len(), 20);
    assert!(vector_results.documents.iter().all(|result| result.score >= 0.999));

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
            created: Some(timestamp),
            modified: Some(timestamp),
        }),
        texts,
        paragraphs: resource_paragraphs,
        ..Default::default()
    }
}
