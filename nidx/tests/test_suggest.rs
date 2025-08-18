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
use itertools::Itertools;
use nidx_protos::{
    FilterExpression, NewShardRequest, SuggestFeatures, SuggestRequest, SuggestResponse, VectorIndexConfig,
    filter_expression::{Expr, FacetFilter, FieldFilter},
};
use nidx_tests::*;
use sqlx::PgPool;
use std::collections::HashMap;
use tonic::Request;

#[sqlx::test]
async fn test_suggest_paragraphs(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let (mut fixture, shard) = suggest_shard(pool).await?;

    // exact match
    expect_paragraphs(
        &suggest_paragraphs(&mut fixture, &shard.id, "Nietzche").await,
        &[(&shard.resources["zarathustra"], "/a/summary")],
    );
    expect_paragraphs(
        &suggest_paragraphs(&mut fixture, &shard.id, "story").await,
        &[(&shard.resources["little prince"], "/a/summary")],
    );

    // typo tolerant search
    expect_paragraphs(
        &suggest_paragraphs(&mut fixture, &shard.id, "princes").await,
        &[
            (&shard.resources["little prince"], "/a/title"),
            (&shard.resources["little prince"], "/a/summary"),
        ],
    );

    // we won't match anything, as 'z' is too short to do fuzzy
    expect_paragraphs(&suggest_paragraphs(&mut fixture, &shard.id, "z").await, &[]);
    // however, 'a' will match exactly and match one resource
    expect_paragraphs(
        &suggest_paragraphs(&mut fixture, &shard.id, "a").await,
        &[(&shard.resources["little prince"], "/a/summary")],
    );

    // nonexistent term
    expect_paragraphs(&suggest_paragraphs(&mut fixture, &shard.id, "Hanna Adrent").await, &[]);

    // filter by field
    let response = fixture
        .searcher_client
        .suggest(Request::new(SuggestRequest {
            shard: shard.id.clone(),
            body: "prince".to_string(),
            features: vec![SuggestFeatures::Paragraphs as i32],
            field_filter: Some(FilterExpression {
                expr: Some(Expr::Field(FieldFilter {
                    field_type: "a".into(),
                    field_id: Some("title".into()),
                })),
            }),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    expect_paragraphs(&response, &[(&shard.resources["little prince"], "/a/title")]);

    // filter by language (set as a label) - "prince" appears in english sources
    // but not in german ones
    let request = SuggestRequest {
        shard: shard.id.clone(),
        body: "prince".to_string(),
        features: vec![SuggestFeatures::Paragraphs as i32],
        ..Default::default()
    };

    let response = fixture
        .searcher_client
        .suggest(Request::new(SuggestRequest {
            field_filter: Some(FilterExpression {
                expr: Some(Expr::Facet(FacetFilter {
                    facet: "/s/p/en".into(),
                })),
            }),
            ..request.clone()
        }))
        .await
        .unwrap()
        .into_inner();

    expect_paragraphs(
        &response,
        &[
            (&shard.resources["little prince"], "/a/title"),
            (&shard.resources["little prince"], "/a/summary"),
        ],
    );

    let response = fixture
        .searcher_client
        .suggest(Request::new(SuggestRequest {
            field_filter: Some(FilterExpression {
                expr: Some(Expr::Facet(FacetFilter {
                    facet: "/s/p/de".into(),
                })),
            }),
            ..request.clone()
        }))
        .await
        .unwrap()
        .into_inner();

    expect_paragraphs(&response, &[]);

    // Inverted filter
    let response = fixture
        .searcher_client
        .suggest(Request::new(SuggestRequest {
            field_filter: Some(FilterExpression {
                expr: Some(Expr::BoolNot(Box::new(FilterExpression {
                    expr: Some(Expr::Facet(FacetFilter {
                        facet: "/s/p/de".into(),
                    })),
                }))),
            }),
            ..request.clone()
        }))
        .await
        .unwrap()
        .into_inner();

    expect_paragraphs(
        &response,
        &[
            (&shard.resources["little prince"], "/a/title"),
            (&shard.resources["little prince"], "/a/summary"),
        ],
    );

    let response = fixture
        .searcher_client
        .suggest(Request::new(SuggestRequest {
            field_filter: Some(FilterExpression {
                expr: Some(Expr::BoolNot(Box::new(FilterExpression {
                    expr: Some(Expr::Facet(FacetFilter {
                        facet: "/s/p/en".into(),
                    })),
                }))),
            }),
            ..request
        }))
        .await
        .unwrap()
        .into_inner();

    expect_paragraphs(&response, &[]);

    Ok(())
}

#[sqlx::test]
async fn test_suggest_entities(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let (mut fixture, shard) = suggest_shard(pool).await?;

    // basic suggests
    expect_entities(
        &suggest_entities(&mut fixture, &shard.id, "Ann").await,
        &["Anna", "Anthony"],
    );

    expect_entities(&suggest_entities(&mut fixture, &shard.id, "joh").await, &["John"]);

    expect_entities(&suggest_entities(&mut fixture, &shard.id, "anyth").await, &["Anthony"]);

    expect_entities(&suggest_entities(&mut fixture, &shard.id, "anything").await, &[]);

    // validate tokenization
    expect_entities(
        &suggest_entities(&mut fixture, &shard.id, "barc").await,
        &["Barcelona", "Bárcenas"],
    );

    expect_entities(
        &suggest_entities(&mut fixture, &shard.id, "Barc").await,
        &["Barcelona", "Bárcenas"],
    );

    expect_entities(
        &suggest_entities(&mut fixture, &shard.id, "BARC").await,
        &["Barcelona", "Bárcenas"],
    );

    expect_entities(
        &suggest_entities(&mut fixture, &shard.id, "BÄRĈ").await,
        &["Barcelona", "Bárcenas"],
    );

    expect_entities(
        &suggest_entities(&mut fixture, &shard.id, "BáRc").await,
        &["Barcelona", "Bárcenas"],
    );

    // multiple words
    let response = suggest_entities(&mut fixture, &shard.id, "Solomon Isa").await;
    assert!(response.entity_results.is_some());
    assert_eq!(response.entity_results.as_ref().unwrap().nodes.len(), 2);
    // Due to tantivy limitations, scoring with fuzzy doesn't work as expected
    // and the order of results is random
    expect_entities(&response, &["Solomon Islands", "Israel"]);

    // Does not find resources by UUID prefix
    let pap_uuid = &shard.resources["pap"];
    expect_entities(&suggest_entities(&mut fixture, &shard.id, &pap_uuid[0..6]).await, &[]);

    Ok(())
}

#[sqlx::test]
async fn test_suggest_features(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
    // Test: search for "ann" with paragraph and entities features and validate
    // we search only for what we request.
    //
    // "ann" should match entities starting with this prefix and the "and" word
    // from the little prince text

    let (mut fixture, shard) = suggest_shard(pool).await?;

    let response = suggest_paragraphs(&mut fixture, &shard.id, "ann").await;
    assert!(response.entity_results.is_none());
    expect_paragraphs(&response, &[(&shard.resources["little prince"], "/a/summary")]);

    let response = suggest_entities(&mut fixture, &shard.id, "ann").await;
    assert_eq!(response.total, 0);
    assert!(response.results.is_empty());
    expect_entities(&response, &["Anna", "Anthony"]);

    Ok(())
}

struct ShardDetails {
    id: String,
    resources: HashMap<&'static str, String>,
}

async fn suggest_shard(pool: PgPool) -> anyhow::Result<(NidxFixture, ShardDetails)> {
    let mut fixture = NidxFixture::new(pool).await?;
    let new_shard_response = fixture
        .api_client
        .new_shard(Request::new(NewShardRequest {
            kbid: "aabbccdd-eeff-1122-3344-556677889900".to_string(),
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

    let resources = [
        ("little prince", little_prince(shard_id, None)),
        ("zarathustra", thus_spoke_zarathustra(shard_id)),
        ("pap", people_and_places(shard_id)),
    ];

    let mut resource_uuids = HashMap::new();
    for (name, resource) in resources.into_iter() {
        resource_uuids.insert(name, resource.resource.as_ref().unwrap().uuid.clone());
        fixture.index_resource(shard_id, resource).await.unwrap();
    }
    fixture.wait_sync().await;

    Ok((
        fixture,
        ShardDetails {
            id: shard_id.to_owned(),
            resources: resource_uuids,
        },
    ))
}

async fn suggest_paragraphs(
    fixture: &mut NidxFixture,
    shard_id: impl Into<String>,
    query: impl Into<String>,
) -> SuggestResponse {
    let request = Request::new(SuggestRequest {
        shard: shard_id.into(),
        body: query.into(),
        features: vec![SuggestFeatures::Paragraphs as i32],
        ..Default::default()
    });
    let response = fixture.searcher_client.suggest(request).await.unwrap();
    response.into_inner()
}

fn expect_paragraphs(response: &SuggestResponse, expected: &[(&str, &str)]) {
    assert_eq!(
        response.total as usize,
        expected.len(),
        "\nfailed assert for \n'{:#?}'",
        response
    );

    let results = response
        .results
        .iter()
        .map(|result| (result.uuid.as_str(), result.field.as_str()))
        .sorted();
    results
        .zip(expected.iter().sorted())
        .for_each(|(item, expected)| assert_eq!(item, *expected, "\nfailed assert for \n'{:#?}'", response));
}

async fn suggest_entities(
    fixture: &mut NidxFixture,
    shard_id: impl Into<String>,
    query: impl Into<String>,
) -> SuggestResponse {
    let request = Request::new(SuggestRequest {
        shard: shard_id.into(),
        body: query.into(),
        features: vec![SuggestFeatures::Entities as i32],
        ..Default::default()
    });
    let response = fixture.searcher_client.suggest(request).await.unwrap();
    response.into_inner()
}

fn expect_entities(response: &SuggestResponse, expected: &[&str]) {
    assert!(response.entity_results.is_some());
    assert_eq!(
        response.entity_results.as_ref().unwrap().nodes.len(),
        expected.len(),
        "Response entities don't match expected ones: {:?} != {:?}",
        response.entity_results,
        expected,
    );
    for entity in expected {
        assert!(
            response
                .entity_results
                .as_ref()
                .unwrap()
                .nodes
                .iter()
                .any(|e| &e.value == entity)
        );
    }
}
