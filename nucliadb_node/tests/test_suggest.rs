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

use common::{resources, NodeFixture, TestNodeReader, TestNodeWriter};
use itertools::Itertools;
use nucliadb_core::protos::{
    op_status, Filter, NewShardRequest, SuggestFeatures, SuggestRequest, SuggestResponse,
};
use tonic::Request;

#[tokio::test]
async fn test_suggest_paragraphs() -> Result<(), Box<dyn std::error::Error>> {
    let mut fixture = NodeFixture::new();
    fixture.with_writer().await?.with_reader().await?;
    let mut writer = fixture.writer_client();
    let mut reader = fixture.reader_client();

    let shard = create_suggest_shard(&mut writer).await;

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
        &[
            (&shard.resources["little prince"], "/a/title"),
            (&shard.resources["little prince"], "/a/summary"),
        ],
    );

    // fuzzy search with distance 1 will only match 'a' from resource 2
    expect_paragraphs(
        &suggest_paragraphs(&mut reader, &shard.id, "z").await,
        &[(&shard.resources["little prince"], "/a/summary")],
    );

    // nonexistent term
    expect_paragraphs(
        &suggest_paragraphs(&mut reader, &shard.id, "Hanna Adrent").await,
        &[],
    );

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
    expect_paragraphs(
        &response,
        &[(&shard.resources["little prince"], "/a/title")],
    );

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

    let response = reader
        .suggest(Request::new(SuggestRequest {
            filter: Some(Filter {
                field_labels: vec!["/s/p/de".to_string()],
                paragraph_labels: vec![],
            }),
            ..request
        }))
        .await
        .unwrap()
        .into_inner();

    expect_paragraphs(&response, &[]);

    Ok(())
}

#[tokio::test]
async fn test_suggest_entities() -> Result<(), Box<dyn std::error::Error>> {
    let mut fixture = NodeFixture::new();
    fixture.with_writer().await?.with_reader().await?;
    let mut writer = fixture.writer_client();
    let mut reader = fixture.reader_client();

    let shard = create_suggest_shard(&mut writer).await;

    // basic suggests
    expect_entities(
        &suggest_entities(&mut reader, &shard.id, "An").await,
        &["Anastasia", "Anna", "Anthony"],
    );

    expect_entities(
        &suggest_entities(&mut reader, &shard.id, "ann").await,
        // TODO: add "Anastasia" when typo correction is implemented
        &["Anna"],
    );

    expect_entities(
        &suggest_entities(&mut reader, &shard.id, "jo").await,
        &["John"],
    );

    expect_entities(&suggest_entities(&mut reader, &shard.id, "any").await, &[]);

    // validate tokenization
    expect_entities(
        &suggest_entities(&mut reader, &shard.id, "bar").await,
        &["Barcelona", "Bárcenas"],
    );

    expect_entities(
        &suggest_entities(&mut reader, &shard.id, "Bar").await,
        &["Barcelona", "Bárcenas"],
    );

    expect_entities(
        &suggest_entities(&mut reader, &shard.id, "BAR").await,
        &["Barcelona", "Bárcenas"],
    );

    expect_entities(
        &suggest_entities(&mut reader, &shard.id, "BÄR").await,
        &["Barcelona", "Bárcenas"],
    );

    expect_entities(
        &suggest_entities(&mut reader, &shard.id, "BáR").await,
        &["Barcelona", "Bárcenas"],
    );

    // multiple words and result ordering
    let response = suggest_entities(&mut reader, &shard.id, "Solomon Is").await;
    assert!(response.entities.is_some());
    assert_eq!(response.entities.as_ref().unwrap().total, 2);
    assert!(response.entities.as_ref().unwrap().entities[0] == *"Solomon Islands");
    assert!(response.entities.as_ref().unwrap().entities[1] == *"Israel");

    Ok(())
}

#[tokio::test]
async fn test_suggest_features() -> Result<(), Box<dyn std::error::Error>> {
    // Test: search for "an" with paragraph and entities features and validate
    // we search only for what we request.
    //
    // "an" should match entities starting with this prefix and the "and" word
    // from the little prince text

    let mut fixture = NodeFixture::new();
    fixture.with_writer().await?.with_reader().await?;
    let mut writer = fixture.writer_client();
    let mut reader = fixture.reader_client();

    let shard = create_suggest_shard(&mut writer).await;

    let response = suggest_paragraphs(&mut reader, &shard.id, "an").await;
    assert!(response.entities.is_none());
    expect_paragraphs(
        &response,
        &[(&shard.resources["little prince"], "/a/summary")],
    );

    let response = suggest_entities(&mut reader, &shard.id, "an").await;
    assert_eq!(response.total, 0);
    assert!(response.results.is_empty());
    expect_entities(&response, &["Anastasia", "Anna", "Anthony"]);

    Ok(())
}

struct ShardDetails<'a> {
    id: String,
    resources: HashMap<&'a str, String>,
}

async fn create_suggest_shard(writer: &mut TestNodeWriter) -> ShardDetails {
    let request = Request::new(NewShardRequest::default());
    let new_shard_response = writer
        .new_shard(request)
        .await
        .expect("Unable to create new shard");
    let shard_id = &new_shard_response.get_ref().id;
    let resource_uuids = create_test_resources(writer, shard_id.clone()).await;
    ShardDetails {
        id: shard_id.to_owned(),
        resources: resource_uuids,
    }
}

async fn create_test_resources(
    writer: &mut TestNodeWriter,
    shard_id: String,
) -> HashMap<&str, String> {
    let resources = [
        ("little prince", resources::little_prince(&shard_id)),
        ("zarathustra", resources::thus_spoke_zarathustra(&shard_id)),
        ("pap", resources::people_and_places(&shard_id)),
    ];
    let mut resource_uuids = HashMap::new();

    for (name, resource) in resources.into_iter() {
        resource_uuids.insert(name, resource.resource.as_ref().unwrap().uuid.clone());
        let request = Request::new(resource);
        let response = writer.set_resource(request).await.unwrap();
        assert_eq!(response.get_ref().status, op_status::Status::Ok as i32);
    }

    resource_uuids
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
        .for_each(|(item, expected)| {
            assert_eq!(item, *expected, "\nfailed assert for \n'{:#?}'", response)
        });
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
    assert!(response.entities.is_some());
    assert_eq!(
        response.entities.as_ref().unwrap().total as usize,
        expected.len()
    );
    for entity in expected {
        assert!(response
            .entities
            .as_ref()
            .unwrap()
            .entities
            .contains(&entity.to_string()));
    }
}
