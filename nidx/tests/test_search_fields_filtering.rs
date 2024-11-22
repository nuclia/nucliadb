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
use nidx_protos::prost_types::Timestamp;
use nidx_protos::{nodereader, noderesources};
use nidx_protos::{NewShardRequest, VectorIndexConfig};
use sqlx::PgPool;
use std::collections::HashMap;
use std::time::SystemTime;
use tonic::Request;
use uuid::Uuid;

const VECTOR_DIMENSION: usize = 10;

#[sqlx::test]
async fn test_search_fields_filtering(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let mut fixture = NidxFixture::new(pool).await?;

    let new_shard_response = fixture
        .api_client
        .new_shard(Request::new(NewShardRequest {
            kbid: "aabbccddeeff11223344556677889900".to_string(),
            vectorsets_configs: HashMap::from([("english".to_string(), VectorIndexConfig::default())]),
            ..Default::default()
        }))
        .await?;
    let shard_id = &new_shard_response.get_ref().id;

    // Add a resource with vectors
    let resource1 = build_resource_with_field(shard_id.to_string(), "f/field1".to_string());
    let resource2 = build_resource_with_field(shard_id.to_string(), "f/field2".to_string());
    fixture.index_resource(shard_id, resource1).await?;
    fixture.index_resource(shard_id, resource2).await?;

    fixture.wait_sync().await;

    // Search filtering with an unexisting field, to check that no vector are returned
    let magnitude = f32::sqrt((17.0_f32).powi(2) * VECTOR_DIMENSION as f32);
    let query_vector = vec![17.0 / magnitude; VECTOR_DIMENSION];

    let search_request = nodereader::SearchRequest {
        shard: shard_id.clone(),
        fields: ["f/foobar".to_string()].to_vec(),
        vector: query_vector.clone(),
        vectorset: "english".to_string(),
        page_number: 0,
        result_per_page: 1,
        min_score_semantic: -1.0,
        paragraph: false,
        document: false,
        ..Default::default()
    };
    let results = fixture.searcher_client.search(Request::new(search_request)).await?.into_inner();

    assert!(results.vector.is_some());
    let vector_results = results.vector.unwrap();
    assert_eq!(vector_results.documents.len(), 0);

    // Search filtering with a real field, to check that the vector is returned
    let search_request = nodereader::SearchRequest {
        shard: shard_id.clone(),
        vector: query_vector.clone(),
        vectorset: "english".to_string(),
        fields: ["f/field1".to_string(), "f/unexisting".to_string()].to_vec(),
        page_number: 0,
        result_per_page: 1,
        min_score_semantic: -1.0,
        paragraph: false,
        document: false,
        ..Default::default()
    };
    let results = fixture.searcher_client.search(Request::new(search_request)).await?.into_inner();

    assert!(results.vector.is_some());
    let vector_results = results.vector.unwrap();
    assert_eq!(vector_results.documents.len(), 1);

    // Search filtering with multiple fields, to check that the OR is applied
    let search_request = nodereader::SearchRequest {
        shard: shard_id.clone(),
        vector: query_vector.clone(),
        vectorset: "english".to_string(),
        fields: ["f/field1".to_string(), "f/field2".to_string()].to_vec(),
        page_number: 0,
        result_per_page: 2,
        min_score_semantic: -1.0,
        paragraph: false,
        document: false,
        ..Default::default()
    };
    let results = fixture.searcher_client.search(Request::new(search_request)).await?.into_inner();

    assert!(results.vector.is_some());
    let vector_results = results.vector.unwrap();
    assert_eq!(vector_results.documents.len(), 2);

    Ok(())
}

fn build_resource_with_field(shard_id: String, field_id: String) -> noderesources::Resource {
    let rid = Uuid::new_v4().to_string();

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
        let start = i;
        let end = i + 1;
        sentences.insert(
            format!("{rid}/{field_id}/{i}/{start}-{end}"),
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
