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

use common::services::NidxFixture;
use nidx_protos::filter_expression::date_range_filter::DateField;
use nidx_protos::filter_expression::{DateRangeFilter, Expr, FilterExpressionList};
use nidx_protos::prost_types::Timestamp;
use nidx_protos::resource::ResourceStatus;
use nidx_protos::{
    FilterExpression, IndexMetadata, IndexParagraph, IndexParagraphs, NewShardRequest, Resource, ResourceId,
    SearchRequest, TextInformation, VectorIndexConfig, VectorSentence, VectorsetSentences,
};
use sqlx::PgPool;
use tonic::Request;
use uuid::Uuid;

async fn populate(fixture: &mut NidxFixture, shard_id: String, metadata: IndexMetadata) {
    let raw_resource_id = Uuid::new_v4().simple().to_string();
    let field_id = "f/body".to_string();

    let resource_id = ResourceId {
        shard_id: shard_id.clone(),
        uuid: raw_resource_id.clone(),
    };

    let mut sentences = HashMap::new();
    let sentence_blueprint = VectorSentence::default();

    let mut sentence = sentence_blueprint.clone();
    sentence.vector = vec![1.0, 3.0, 4.0];
    sentences.insert(format!("{raw_resource_id}/{field_id}/1/1"), sentence);

    let mut sentence = sentence_blueprint.clone();
    sentence.vector = vec![2.0, 4.0, 5.0];
    sentences.insert(format!("{raw_resource_id}/{field_id}/1/2"), sentence);

    let mut sentence = sentence_blueprint.clone();
    sentence.vector = vec![3.0, 5.0, 6.0];
    sentences.insert(format!("{raw_resource_id}/{field_id}/1/3"), sentence);

    let mut sentence = sentence_blueprint.clone();
    sentence.vector = vec![6.0, 9.0, 6.0];
    sentences.insert(format!("{raw_resource_id}/{field_id}/1/4"), sentence);

    let paragraph = IndexParagraph {
        start: 0,
        end: 0,
        sentences: sentences.clone(),
        vectorsets_sentences: HashMap::from([("__default__".to_string(), VectorsetSentences { sentences })]),
        field: field_id.clone(),
        labels: vec![],
        index: 3,
        split: "".to_string(),
        repeated_in_field: false,
        metadata: None,
    };
    let paragraphs = IndexParagraphs {
        paragraphs: HashMap::from([(format!("{raw_resource_id}/{field_id}/1"), paragraph)]),
    };

    let text_content = TextInformation {
        text: "Dummy text".to_string(),
        labels: vec![],
    };

    let resource = Resource {
        shard_id: shard_id.clone(),
        resource: Some(resource_id),
        metadata: Some(metadata),
        status: ResourceStatus::Processed as i32,
        texts: HashMap::from([(field_id.clone(), text_content)]),
        paragraphs: HashMap::from([(format!("{raw_resource_id}/{field_id}"), paragraphs)]),
        ..Default::default()
    };

    fixture.index_resource(&shard_id, resource).await.unwrap();
}

#[sqlx::test]
async fn test_date_range_search(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let base_time = Timestamp::default();
    let mut fixture = NidxFixture::new(pool).await?;

    let new_shard_response = fixture
        .api_client
        .new_shard(Request::new(NewShardRequest {
            kbid: "aabbccddeeff11223344556677889900".to_string(),
            vectorsets_configs: HashMap::from([(
                "english".to_string(),
                VectorIndexConfig {
                    vector_dimension: Some(3),
                    ..Default::default()
                },
            )]),
            ..Default::default()
        }))
        .await?;
    let shard_id = &new_shard_response.get_ref().id;

    let metadata = IndexMetadata {
        created: Some(base_time),
        modified: Some(base_time),
    };
    populate(&mut fixture, shard_id.clone(), metadata).await;

    let mut base_time_plus_one = base_time;
    base_time_plus_one.seconds += 1;
    let metadata = IndexMetadata {
        created: Some(base_time_plus_one),
        modified: Some(base_time_plus_one),
    };
    populate(&mut fixture, shard_id.clone(), metadata).await;

    let request = SearchRequest {
        shard: shard_id.clone(),
        order: None,
        vectorset: "english".to_string(),
        vector: vec![4.0, 6.0, 7.0],
        result_per_page: 20,
        with_duplicates: true,
        ..Default::default()
    };

    fixture.wait_sync().await;

    // No time filter
    let no_time_range = request.clone();
    let result = fixture.searcher_client.search(no_time_range).await.unwrap();
    let result = result.into_inner();
    let vectors = result.vector.unwrap();
    assert_eq!(vectors.documents.len(), 8);

    // Time range allows everything
    let mut request_all_range = request.clone();
    request_all_range.field_filter = Some(FilterExpression {
        expr: Some(Expr::BoolAnd(FilterExpressionList {
            operands: vec![
                FilterExpression {
                    expr: Some(Expr::Date(DateRangeFilter {
                        field: DateField::Modified.into(),
                        since: Some(base_time),
                        until: None,
                    })),
                },
                FilterExpression {
                    expr: Some(Expr::Date(DateRangeFilter {
                        field: DateField::Created.into(),
                        since: Some(base_time),
                        until: None,
                    })),
                },
            ],
        })),
    });
    let result = fixture.searcher_client.search(request_all_range).await.unwrap();
    let result = result.into_inner();
    let vectors = result.vector.unwrap();
    assert_eq!(vectors.documents.len(), 8);

    // Time range allows only second batch
    let mut request_second_batch = request.clone();
    request_second_batch.field_filter = Some(FilterExpression {
        expr: Some(Expr::BoolAnd(FilterExpressionList {
            operands: vec![
                FilterExpression {
                    expr: Some(Expr::Date(DateRangeFilter {
                        field: DateField::Modified.into(),
                        since: Some(base_time_plus_one),
                        until: None,
                    })),
                },
                FilterExpression {
                    expr: Some(Expr::Date(DateRangeFilter {
                        field: DateField::Created.into(),
                        since: Some(base_time_plus_one),
                        until: None,
                    })),
                },
            ],
        })),
    });
    let result = fixture.searcher_client.search(request_second_batch).await.unwrap();
    let result = result.into_inner();
    let vectors = result.vector.unwrap();
    assert_eq!(vectors.documents.len(), 4);

    // Time range allows only second batch, but with modified only
    let mut request_second_batch = request.clone();
    request_second_batch.field_filter = Some(FilterExpression {
        expr: Some(Expr::BoolAnd(FilterExpressionList {
            operands: vec![FilterExpression {
                expr: Some(Expr::Date(DateRangeFilter {
                    field: DateField::Modified.into(),
                    since: Some(base_time_plus_one),
                    until: None,
                })),
            }],
        })),
    });
    let result = fixture.searcher_client.search(request_second_batch).await.unwrap();
    let result = result.into_inner();
    let vectors = result.vector.unwrap();
    assert_eq!(vectors.documents.len(), 4);

    let mut base_time_plus_two = base_time_plus_one;
    base_time_plus_two.seconds += 1;

    // Time range does not match any field, therefore the response has no results
    let mut request_second_batch = request.clone();
    request_second_batch.field_filter = Some(FilterExpression {
        expr: Some(Expr::BoolAnd(FilterExpressionList {
            operands: vec![
                FilterExpression {
                    expr: Some(Expr::Date(DateRangeFilter {
                        field: DateField::Modified.into(),
                        since: Some(base_time_plus_two),
                        until: None,
                    })),
                },
                FilterExpression {
                    expr: Some(Expr::Date(DateRangeFilter {
                        field: DateField::Created.into(),
                        since: Some(base_time_plus_two),
                        until: None,
                    })),
                },
            ],
        })),
    });
    let result = fixture.searcher_client.search(request_second_batch).await.unwrap();
    let result = result.into_inner();
    assert_eq!(result.document, None);
    assert_eq!(result.paragraph, None);
    assert_eq!(result.vector, None);
    assert_eq!(result.graph, None);

    // Multiple timestamps are parsed as AND conditions
    let mut request_second_batch = request.clone();
    request_second_batch.field_filter = Some(FilterExpression {
        expr: Some(Expr::BoolAnd(FilterExpressionList {
            operands: vec![
                FilterExpression {
                    expr: Some(Expr::Date(DateRangeFilter {
                        field: DateField::Modified.into(),
                        since: Some(base_time_plus_one),
                        until: None,
                    })),
                },
                FilterExpression {
                    expr: Some(Expr::Date(DateRangeFilter {
                        field: DateField::Created.into(),
                        since: Some(base_time_plus_two),
                        until: None,
                    })),
                },
            ],
        })),
    });
    let result = fixture.searcher_client.search(request_second_batch).await.unwrap();
    let result = result.into_inner();
    assert_eq!(result.document, None);
    assert_eq!(result.paragraph, None);
    assert_eq!(result.vector, None);
    assert_eq!(result.graph, None);

    Ok(())
}
