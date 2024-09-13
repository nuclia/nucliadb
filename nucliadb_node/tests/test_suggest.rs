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

// disable clippy lint caused by rstest proc macro
#![allow(clippy::items_after_test_module)]

mod common;

use std::collections::HashMap;

use common::{resources, NodeFixture, TestNodeReader};
use itertools::Itertools;
use nucliadb_core::protos::{op_status, Filter, NewShardRequest, SuggestFeatures, SuggestRequest, SuggestResponse};
use rstest::*;
use tonic::Request;

#[rstest]
#[awt]
#[tokio::test]
async fn test_suggest_paragraphs(
    #[future] suggest_shard: (NodeFixture, ShardDetails),
) -> Result<(), Box<dyn std::error::Error>> {
    let (node, shard) = suggest_shard;
    let mut reader = node.reader_client();

    // exact match
    expect_paragraphs(
        &suggest_paragraphs(&mut reader, &shard.id, "Nietzche").await,
        &[(&shard.resources["zarathustra"], "/a/summary")],
    );
    expect_paragraphs(
        &suggest_paragraphs(&mut reader, &shard.id, "story").await,
        &[(&shard.resources["little prince"], "/a/summary")],
    );

    // typo tolerant search
    expect_paragraphs(
        &suggest_paragraphs(&mut reader, &shard.id, "princes").await,
        &[(&shard.resources["little prince"], "/a/title"), (&shard.resources["little prince"], "/a/summary")],
    );

    // fuzzy search with distance 1 will only match 'a' from resource 2
    expect_paragraphs(
        &suggest_paragraphs(&mut reader, &shard.id, "z").await,
        &[(&shard.resources["little prince"], "/a/summary")],
    );

    // nonexistent term
    expect_paragraphs(&suggest_paragraphs(&mut reader, &shard.id, "Hanna Adrent").await, &[]);

    // filter by field
    let response = reader
        .suggest(Request::new(SuggestRequest {
            shard: shard.id.clone(),
            body: "prince".to_string(),
            fields: vec!["a/title".to_string()],
            features: vec![SuggestFeatures::Paragraphs as i32],
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

    let response = reader
        .suggest(Request::new(SuggestRequest {
            filter: Some(Filter {
                field_labels: vec!["/s/p/en".to_string()],
                paragraph_labels: vec![],
                ..Default::default()
            }),
            ..request.clone()
        }))
        .await
        .unwrap()
        .into_inner();

    expect_paragraphs(
        &response,
        &[(&shard.resources["little prince"], "/a/title"), (&shard.resources["little prince"], "/a/summary")],
    );

    let response = reader
        .suggest(Request::new(SuggestRequest {
            filter: Some(Filter {
                field_labels: vec!["/s/p/de".to_string()],
                paragraph_labels: vec![],
                ..Default::default()
            }),
            ..request
        }))
        .await
        .unwrap()
        .into_inner();

    expect_paragraphs(&response, &[]);

    Ok(())
}

#[rstest]
#[awt]
#[tokio::test]
async fn test_suggest_entities(
    #[future] suggest_shard: (NodeFixture, ShardDetails),
) -> Result<(), Box<dyn std::error::Error>> {
    let (node, shard) = suggest_shard;
    let mut reader = node.reader_client();

    // basic suggests
    expect_entities(&suggest_entities(&mut reader, &shard.id, "Ann").await, &["Anna", "Anthony"]);

    expect_entities(&suggest_entities(&mut reader, &shard.id, "joh").await, &["John"]);

    expect_entities(&suggest_entities(&mut reader, &shard.id, "anyth").await, &["Anthony"]);

    expect_entities(&suggest_entities(&mut reader, &shard.id, "anything").await, &[]);

    // validate tokenization
    expect_entities(&suggest_entities(&mut reader, &shard.id, "barc").await, &["Barcelona", "Bárcenas"]);

    expect_entities(&suggest_entities(&mut reader, &shard.id, "Barc").await, &["Barcelona", "Bárcenas"]);

    expect_entities(&suggest_entities(&mut reader, &shard.id, "BARC").await, &["Barcelona", "Bárcenas"]);

    expect_entities(&suggest_entities(&mut reader, &shard.id, "BÄRĈ").await, &["Barcelona", "Bárcenas"]);

    expect_entities(&suggest_entities(&mut reader, &shard.id, "BáRc").await, &["Barcelona", "Bárcenas"]);

    // multiple words and result ordering
    let response = suggest_entities(&mut reader, &shard.id, "Solomon Isa").await;
    assert!(response.entity_results.is_some());
    assert_eq!(response.entity_results.as_ref().unwrap().nodes.len(), 2);
    assert!(response.entity_results.as_ref().unwrap().nodes[0].value == *"Solomon Islands");
    assert!(response.entity_results.as_ref().unwrap().nodes[1].value == *"Israel");

    // Does not find resources by UUID prefix
    let pap_uuid = &shard.resources["pap"];
    expect_entities(&suggest_entities(&mut reader, &shard.id, &pap_uuid[0..6]).await, &[]);

    Ok(())
}

#[rstest]
#[awt]
#[tokio::test]
async fn test_suggest_features(
    #[future] suggest_shard: (NodeFixture, ShardDetails),
) -> Result<(), Box<dyn std::error::Error>> {
    // Test: search for "ann" with paragraph and entities features and validate
    // we search only for what we request.
    //
    // "an" should match entities starting with this prefix and the "and" word
    // from the little prince text

    let (node, shard) = suggest_shard;
    let mut reader = node.reader_client();

    let response = suggest_paragraphs(&mut reader, &shard.id, "ann").await;
    assert!(response.entity_results.is_none());
    expect_paragraphs(&response, &[(&shard.resources["little prince"], "/a/summary")]);

    let response = suggest_entities(&mut reader, &shard.id, "ann").await;
    assert_eq!(response.total, 0);
    assert!(response.results.is_empty());
    expect_entities(&response, &["Anna", "Anthony"]);

    Ok(())
}

struct ShardDetails {
    id: String,
    resources: HashMap<&'static str, String>,
}

#[fixture]
async fn suggest_shard() -> (NodeFixture, ShardDetails) {
    let mut fixture = NodeFixture::new();
    fixture.with_writer().await.unwrap();
    fixture.with_reader().await.unwrap();

    let mut writer = fixture.writer_client();

    let request = Request::new(NewShardRequest::default());
    let new_shard_response = writer.new_shard(request).await.expect("Unable to create new shard");
    let shard_id = &new_shard_response.get_ref().id;

    let resources = [
        ("little prince", resources::little_prince(shard_id)),
        ("zarathustra", resources::thus_spoke_zarathustra(shard_id)),
        ("pap", resources::people_and_places(shard_id)),
    ];

    let mut resource_uuids = HashMap::new();
    for (name, resource) in resources.into_iter() {
        resource_uuids.insert(name, resource.resource.as_ref().unwrap().uuid.clone());
        let request = Request::new(resource);
        let response = writer.set_resource(request).await.unwrap();
        assert_eq!(response.get_ref().status, op_status::Status::Ok as i32);
    }

    (
        fixture,
        ShardDetails {
            id: shard_id.to_owned(),
            resources: resource_uuids,
        },
    )
}

async fn suggest_paragraphs(
    reader: &mut TestNodeReader,
    shard_id: impl Into<String>,
    query: impl Into<String>,
) -> SuggestResponse {
    let request = Request::new(SuggestRequest {
        shard: shard_id.into(),
        body: query.into(),
        features: vec![SuggestFeatures::Paragraphs as i32],
        ..Default::default()
    });
    let response = reader.suggest(request).await.unwrap();
    response.into_inner()
}

fn expect_paragraphs(response: &SuggestResponse, expected: &[(&str, &str)]) {
    assert_eq!(response.total as usize, expected.len(), "\nfailed assert for \n'{:#?}'", response);

    let results = response.results.iter().map(|result| (result.uuid.as_str(), result.field.as_str())).sorted();
    results
        .zip(expected.iter().sorted())
        .for_each(|(item, expected)| assert_eq!(item, *expected, "\nfailed assert for \n'{:#?}'", response));
}

async fn suggest_entities(
    reader: &mut TestNodeReader,
    shard_id: impl Into<String>,
    query: impl Into<String>,
) -> SuggestResponse {
    let request = Request::new(SuggestRequest {
        shard: shard_id.into(),
        body: query.into(),
        features: vec![SuggestFeatures::Entities as i32],
        ..Default::default()
    });
    let response = reader.suggest(request).await.unwrap();
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
        assert!(response.entity_results.as_ref().unwrap().nodes.iter().any(|e| &e.value == entity));
    }
}
