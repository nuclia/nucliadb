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

use std::collections::HashMap;

use crate::common::services::NidxFixture;
use nidx_protos::nodereader::{
    self, JsonFieldPathFilter, JsonFilterExpression, json_field_path_filter, json_filter_expression,
};
use nidx_protos::noderesources::JsonInformation;
use nidx_protos::{
    IndexParagraph, IndexParagraphs, NewShardRequest, Resource, Security, TextInformation, VectorIndexConfig,
};
use nidx_tests::minimal_resource;
use sqlx::PgPool;
use tonic::Request;

const VECTOR_DIMENSION: usize = 3;

/// Build a resource that carries JSON data on a given field, plus a text field
/// and paragraph so that paragraph search has results to return.
/// The JSON blob is: { "price": <price>, "category": <category>, "available": <available> }
fn resource_with_json(shard_id: String, price: i64, category: &str, available: bool) -> Resource {
    resource_with_json_and_security(shard_id, price, category, available, vec![])
}

fn resource_with_json_and_security(
    shard_id: String,
    price: i64,
    category: &str,
    available: bool,
    access_groups: Vec<String>,
) -> Resource {
    let mut resource = minimal_resource(shard_id);
    let rid = resource.resource.as_ref().unwrap().uuid.clone();

    if !access_groups.is_empty() {
        resource.security = Some(Security { access_groups });
    }

    // JSON field
    let json_value = serde_json::json!({
        "price": price,
        "category": category,
        "available": available,
    });
    resource.json_fields.insert(
        "t/product".to_string(),
        JsonInformation {
            value: json_value.to_string(),
        },
    );

    // Text field so the resource is findable via paragraph search
    resource.texts.insert(
        "a/title".to_string(),
        TextInformation {
            text: format!("{category} item"),
            ..Default::default()
        },
    );
    let mut paragraphs = HashMap::new();
    paragraphs.insert(
        format!("{rid}/a/title/0-20"),
        IndexParagraph {
            start: 0,
            end: 20,
            field: "a/title".to_string(),
            ..Default::default()
        },
    );
    resource
        .paragraphs
        .insert("a/title".to_string(), IndexParagraphs { paragraphs });

    resource
}

fn json_path_filter(
    field_id: &str,
    json_path: &str,
    predicate: json_field_path_filter::Predicate,
) -> JsonFilterExpression {
    JsonFilterExpression {
        expr: Some(json_filter_expression::Expr::Path(JsonFieldPathFilter {
            field_id: field_id.to_string(),
            json_path: json_path.to_string(),
            predicate: Some(predicate),
        })),
    }
}

fn json_and(operands: Vec<JsonFilterExpression>) -> JsonFilterExpression {
    JsonFilterExpression {
        expr: Some(json_filter_expression::Expr::BoolAnd(
            nodereader::json_filter_expression::List { operands },
        )),
    }
}

fn json_or(operands: Vec<JsonFilterExpression>) -> JsonFilterExpression {
    JsonFilterExpression {
        expr: Some(json_filter_expression::Expr::BoolOr(
            nodereader::json_filter_expression::List { operands },
        )),
    }
}

fn json_not(operand: JsonFilterExpression) -> JsonFilterExpression {
    JsonFilterExpression {
        expr: Some(json_filter_expression::Expr::BoolNot(Box::new(operand))),
    }
}

async fn setup_fixture(
    pool: PgPool,
) -> Result<(NidxFixture, String, String, String, String), Box<dyn std::error::Error>> {
    let mut fixture = NidxFixture::new(pool).await?;

    let new_shard_response = fixture
        .api_client
        .new_shard(Request::new(NewShardRequest {
            kbid: "aabbccddeeff11223344556677889900".to_string(),
            vectorsets_configs: HashMap::from([(
                "english".to_string(),
                VectorIndexConfig {
                    vector_dimension: Some(VECTOR_DIMENSION as u32),
                    ..Default::default()
                },
            )]),
            ..Default::default()
        }))
        .await?;
    let shard_id = new_shard_response.get_ref().id.clone();

    // apple: price=150, category="fruit", available=true
    let apple = resource_with_json(shard_id.clone(), 150, "fruit", true);
    let apple_uuid = apple.resource.as_ref().unwrap().uuid.clone();

    // banana: price=80, category="fruit", available=false
    let banana = resource_with_json(shard_id.clone(), 80, "fruit", false);
    let banana_uuid = banana.resource.as_ref().unwrap().uuid.clone();

    // hammer: price=200, category="tool", available=true
    let hammer = resource_with_json(shard_id.clone(), 200, "tool", true);
    let hammer_uuid = hammer.resource.as_ref().unwrap().uuid.clone();

    fixture.index_resource(&shard_id, apple).await?;
    fixture.index_resource(&shard_id, banana).await?;
    fixture.index_resource(&shard_id, hammer).await?;

    fixture.wait_sync().await;

    Ok((fixture, shard_id, apple_uuid, banana_uuid, hammer_uuid))
}

fn uuids_from_paragraph_results(results: &nodereader::SearchResponse) -> Vec<String> {
    results
        .paragraph
        .as_ref()
        .map(|p| p.results.iter().map(|r| r.uuid.clone()).collect())
        .unwrap_or_default()
}

#[sqlx::test]
async fn test_json_exact_match(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let (mut fixture, shard_id, apple_uuid, banana_uuid, _hammer_uuid) = setup_fixture(pool).await?;

    // "fruit" matches apple and banana
    let results = fixture
        .searcher_client
        .search(Request::new(nodereader::SearchRequest {
            shard: shard_id.clone(),
            paragraph: true,
            body: String::new(),
            result_per_page: 10,
            json_filter: Some(json_path_filter(
                "t/product",
                "category",
                json_field_path_filter::Predicate::Text("fruit".to_string()),
            )),
            ..Default::default()
        }))
        .await?
        .into_inner();

    let uuids = uuids_from_paragraph_results(&results);
    assert!(uuids.contains(&apple_uuid), "apple should match category=fruit");
    assert!(uuids.contains(&banana_uuid), "banana should match category=fruit");
    assert_eq!(uuids.len(), 2);

    Ok(())
}

#[sqlx::test]
async fn test_json_no_match(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let (mut fixture, shard_id, ..) = setup_fixture(pool).await?;

    // "vegetable" matches nothing
    let results = fixture
        .searcher_client
        .search(Request::new(nodereader::SearchRequest {
            shard: shard_id.clone(),
            paragraph: true,
            body: String::new(),
            result_per_page: 10,
            json_filter: Some(json_path_filter(
                "t/product",
                "category",
                json_field_path_filter::Predicate::Text("vegetable".to_string()),
            )),
            ..Default::default()
        }))
        .await?
        .into_inner();

    let uuids = uuids_from_paragraph_results(&results);
    assert!(uuids.is_empty(), "no resource should match category=vegetable");

    Ok(())
}

#[sqlx::test]
async fn test_json_int_range(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let (mut fixture, shard_id, apple_uuid, banana_uuid, _hammer_uuid) = setup_fixture(pool).await?;

    // price in [80, 150] matches apple (150) and banana (80), not hammer (200)
    let results = fixture
        .searcher_client
        .search(Request::new(nodereader::SearchRequest {
            shard: shard_id.clone(),
            paragraph: true,
            body: String::new(),
            result_per_page: 10,
            json_filter: Some(json_path_filter(
                "t/product",
                "price",
                json_field_path_filter::Predicate::IntRange(
                    nodereader::json_field_path_filter::IntegerRangePredicate {
                        lower: Some(80),
                        upper: Some(150),
                    },
                ),
            )),
            ..Default::default()
        }))
        .await?
        .into_inner();

    let uuids = uuids_from_paragraph_results(&results);
    assert!(uuids.contains(&apple_uuid), "apple (price=150) should match");
    assert!(uuids.contains(&banana_uuid), "banana (price=80) should match");
    assert_eq!(uuids.len(), 2, "hammer (price=200) should not match");

    Ok(())
}

#[sqlx::test]
async fn test_json_bool_match(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let (mut fixture, shard_id, apple_uuid, _banana_uuid, hammer_uuid) = setup_fixture(pool).await?;

    // available=true matches apple and hammer
    let results = fixture
        .searcher_client
        .search(Request::new(nodereader::SearchRequest {
            shard: shard_id.clone(),
            paragraph: true,
            body: String::new(),
            result_per_page: 10,
            json_filter: Some(json_path_filter(
                "t/product",
                "available",
                json_field_path_filter::Predicate::Boolean(true),
            )),
            ..Default::default()
        }))
        .await?
        .into_inner();

    let uuids = uuids_from_paragraph_results(&results);
    assert!(uuids.contains(&apple_uuid), "apple (available=true) should match");
    assert!(uuids.contains(&hammer_uuid), "hammer (available=true) should match");
    assert_eq!(uuids.len(), 2, "banana (available=false) should not match");

    Ok(())
}

#[sqlx::test]
async fn test_json_and_filter(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let (mut fixture, shard_id, apple_uuid, _banana_uuid, _hammer_uuid) = setup_fixture(pool).await?;

    // category="fruit" AND available=true → only apple
    let results = fixture
        .searcher_client
        .search(Request::new(nodereader::SearchRequest {
            shard: shard_id.clone(),
            paragraph: true,
            body: String::new(),
            result_per_page: 10,
            json_filter: Some(json_and(vec![
                json_path_filter(
                    "t/product",
                    "category",
                    json_field_path_filter::Predicate::Text("fruit".to_string()),
                ),
                json_path_filter(
                    "t/product",
                    "available",
                    json_field_path_filter::Predicate::Boolean(true),
                ),
            ])),
            ..Default::default()
        }))
        .await?
        .into_inner();

    let uuids = uuids_from_paragraph_results(&results);
    assert_eq!(uuids, vec![apple_uuid], "only apple matches fruit AND available=true");

    Ok(())
}

#[sqlx::test]
async fn test_json_or_filter(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let (mut fixture, shard_id, _apple_uuid, banana_uuid, hammer_uuid) = setup_fixture(pool).await?;

    // available=false OR category="tool" → banana and hammer
    let results = fixture
        .searcher_client
        .search(Request::new(nodereader::SearchRequest {
            shard: shard_id.clone(),
            paragraph: true,
            body: String::new(),
            result_per_page: 10,
            json_filter: Some(json_or(vec![
                json_path_filter(
                    "t/product",
                    "available",
                    json_field_path_filter::Predicate::Boolean(false),
                ),
                json_path_filter(
                    "t/product",
                    "category",
                    json_field_path_filter::Predicate::Text("tool".to_string()),
                ),
            ])),
            ..Default::default()
        }))
        .await?
        .into_inner();

    let uuids = uuids_from_paragraph_results(&results);
    assert!(uuids.contains(&banana_uuid), "banana (available=false) should match");
    assert!(uuids.contains(&hammer_uuid), "hammer (category=tool) should match");
    assert_eq!(uuids.len(), 2);

    Ok(())
}

#[sqlx::test]
async fn test_json_filter_combined_with_security(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let mut fixture = NidxFixture::new(pool).await?;

    let new_shard_response = fixture
        .api_client
        .new_shard(Request::new(NewShardRequest {
            kbid: "aabbccddeeff11223344556677889900".to_string(),
            vectorsets_configs: HashMap::from([(
                "english".to_string(),
                VectorIndexConfig {
                    vector_dimension: Some(VECTOR_DIMENSION as u32),
                    ..Default::default()
                },
            )]),
            ..Default::default()
        }))
        .await?;
    let shard_id = new_shard_response.get_ref().id.clone();

    // apple: accessible to "engineering", category="fruit"  → should match both filters
    let apple = resource_with_json_and_security(shard_id.clone(), 150, "fruit", true, vec!["engineering".to_string()]);
    let apple_uuid = apple.resource.as_ref().unwrap().uuid.clone();

    // banana: accessible to "other" only, category="fruit"  → matches JSON but NOT security
    let banana = resource_with_json_and_security(shard_id.clone(), 80, "fruit", false, vec!["other".to_string()]);

    // hammer: accessible to "engineering", category="tool"  → matches security but NOT JSON
    let hammer = resource_with_json_and_security(shard_id.clone(), 200, "tool", true, vec!["engineering".to_string()]);
    let hammer_uuid = hammer.resource.as_ref().unwrap().uuid.clone();

    fixture.index_resource(&shard_id, apple).await?;
    fixture.index_resource(&shard_id, banana).await?;
    fixture.index_resource(&shard_id, hammer).await?;
    fixture.wait_sync().await;

    // Search with BOTH security=["engineering"] AND json_filter=category="fruit".
    // Only apple satisfies both constraints.
    let results = fixture
        .searcher_client
        .search(Request::new(nodereader::SearchRequest {
            shard: shard_id.clone(),
            paragraph: true,
            body: String::new(),
            result_per_page: 10,
            security: Some(Security {
                access_groups: vec!["engineering".to_string()],
            }),
            json_filter: Some(json_path_filter(
                "t/product",
                "category",
                json_field_path_filter::Predicate::Text("fruit".to_string()),
            )),
            ..Default::default()
        }))
        .await?
        .into_inner();

    let uuids = uuids_from_paragraph_results(&results);

    // hammer is accessible but its category is "tool", not "fruit" — JSON filter must exclude it.
    assert!(
        !uuids.contains(&hammer_uuid),
        "hammer matches security but not JSON filter; it must be excluded (got: {uuids:?})"
    );
    assert!(
        uuids.contains(&apple_uuid),
        "apple matches both security and JSON filter; it must be included (got: {uuids:?})"
    );
    assert_eq!(uuids.len(), 1, "only apple should match both filters (got: {uuids:?})");

    Ok(())
}

#[sqlx::test]
async fn test_json_not_filter(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let (mut fixture, shard_id, apple_uuid, _banana_uuid, hammer_uuid) = setup_fixture(pool).await?;

    // NOT available=false → apple and hammer
    let results = fixture
        .searcher_client
        .search(Request::new(nodereader::SearchRequest {
            shard: shard_id.clone(),
            paragraph: true,
            body: String::new(),
            result_per_page: 10,
            json_filter: Some(json_not(json_path_filter(
                "t/product",
                "available",
                json_field_path_filter::Predicate::Boolean(false),
            ))),
            ..Default::default()
        }))
        .await?
        .into_inner();

    let uuids = uuids_from_paragraph_results(&results);
    assert!(
        uuids.contains(&apple_uuid),
        "apple (available=true) should match NOT available=false"
    );
    assert!(
        uuids.contains(&hammer_uuid),
        "hammer (available=true) should match NOT available=false"
    );
    assert_eq!(uuids.len(), 2);

    Ok(())
}
