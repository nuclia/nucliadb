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

use common::resources::minimal_resource;
use common::{node_reader, node_writer, TestNodeReader, TestNodeWriter};
use nucliadb_core::protos::relation::RelationType;
use nucliadb_core::protos::relation_node::NodeType;
use nucliadb_core::protos::{
    op_status, NewShardRequest, Relation, RelationNode, Resource, SuggestFeatures, SuggestRequest,
    SuggestResponse, TextInformation,
};
use tonic::Request;

#[tokio::test]
async fn test_suggest() -> Result<(), Box<dyn std::error::Error>> {
    let mut writer = node_writer().await;
    let mut reader = node_reader().await;

    let new_shard_response = writer
        .new_shard(Request::new(NewShardRequest::default()))
        .await?;
    let shard_id = &new_shard_response.get_ref().id;

    create_test_resources(&mut writer, shard_id.clone()).await;

    // --------------------------------------------------------------
    // Test: suggest paragraphs
    // --------------------------------------------------------------

    // let response = suggest_paragraphs(&mut reader, shard_id, "Nietzche").await;
    // println!("Response: {:#?}", response);

    // --------------------------------------------------------------
    // Test: suggest entities - basic suggests
    // --------------------------------------------------------------

    expect_entities(
        suggest_entities(&mut reader, shard_id, "An").await,
        &["Anastasia", "Anna", "Anthony"],
    );

    expect_entities(
        suggest_entities(&mut reader, shard_id, "ann").await,
        &["Anna"],
    );

    expect_entities(
        suggest_entities(&mut reader, shard_id, "jo").await,
        &["John"],
    );

    expect_entities(suggest_entities(&mut reader, shard_id, "any").await, &[]);

    // --------------------------------------------------------------
    // Test: suggest entities - validate tokenization
    // --------------------------------------------------------------
    expect_entities(
        suggest_entities(&mut reader, shard_id, "bar").await,
        &["Barcelona", "Bárcenas"],
    );

    expect_entities(
        suggest_entities(&mut reader, shard_id, "BAR").await,
        &["Barcelona", "Bárcenas"],
    );

    expect_entities(
        suggest_entities(&mut reader, shard_id, "BÄR").await,
        &["Barcelona", "Bárcenas"],
    );

    expect_entities(
        suggest_entities(&mut reader, shard_id, "BáR").await,
        &["Barcelona", "Bárcenas"],
    );

    // --------------------------------------------------------------
    // Test: suggest entities - multiple words and result ordering
    // --------------------------------------------------------------

    let response = suggest_entities(&mut reader, shard_id, "Solomon Is").await;
    assert!(response.entities.is_some());
    assert_eq!(response.entities.as_ref().unwrap().total, 2);
    assert!(response.entities.as_ref().unwrap().entities[0] == *"Solomon Islands");
    assert!(response.entities.as_ref().unwrap().entities[1] == *"Israel");

    Ok(())
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

fn expect_entities(response: SuggestResponse, expected: &[&str]) {
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

async fn suggest_paragraphs(
    reader: &mut TestNodeReader,
    shard_id: impl Into<String>,
    query: impl Into<String>,
) -> SuggestResponse {
    let request = Request::new(SuggestRequest {
        shard: shard_id.into(),
        body: query.into(),
        features: vec![SuggestFeatures::Paragraph as i32],
        ..Default::default()
    });
    let response = reader.suggest(request).await.unwrap();
    response.into_inner()
}

async fn create_test_resources(writer: &mut TestNodeWriter, shard_id: String) {
    let resources = [
        resource_with_little_prince(shard_id.clone()),
        resource_with_zarathustra(shard_id.clone()),
        resource_with_entities(shard_id.clone()),
    ];

    for resource in resources.into_iter() {
        let request = Request::new(resource);
        let response = writer.set_resource(request).await.unwrap();
        assert_eq!(response.get_ref().status, op_status::Status::Ok as i32);
    }
}

fn resource_with_little_prince(shard_id: String) -> Resource {
    let mut resource = minimal_resource(shard_id);

    resource.texts.insert(
        format!("{}/title", resource.resource.as_ref().unwrap().uuid),
        TextInformation {
            text: "The little prince".to_string(),
            ..Default::default()
        },
    );

    resource.texts.insert(
        format!("{}/summary", resource.resource.as_ref().unwrap().uuid),
        TextInformation {
            text: "The story follows a young prince who visits various planets in space, \
                   including Earth, and addresses themes of loneliness, friendship, love, and \
                   loss."
                .to_string(),
            ..Default::default()
        },
    );

    resource
}

fn resource_with_zarathustra(shard_id: String) -> Resource {
    let mut resource = minimal_resource(shard_id);

    resource.texts.insert(
        format!("{}/title", resource.resource.as_ref().unwrap().uuid),
        TextInformation {
            text: "Thus Spoke Zarathustra".to_string(),
            ..Default::default()
        },
    );

    resource.texts.insert(
        format!("{}/summary", resource.resource.as_ref().unwrap().uuid),
        TextInformation {
            text: "Philosophical book written by Frederich Nietzche".to_string(),
            ..Default::default()
        },
    );

    resource
}

fn resource_with_entities(shard_id: String) -> Resource {
    let mut resource = minimal_resource(shard_id);

    resource.texts.insert(
        format!("{}/title", resource.resource.as_ref().unwrap().uuid),
        TextInformation {
            text: "People and places".to_string(),
            ..Default::default()
        },
    );
    resource.texts.insert(
        format!("{}/summary", resource.resource.as_ref().unwrap().uuid),
        TextInformation {
            text: "Test entities to validate suggest on relations index".to_string(),
            ..Default::default()
        },
    );

    add_entities(&mut resource);

    resource
}

fn add_entities(resource: &mut Resource) {
    let resource_node = RelationNode {
        value: resource
            .resource
            .as_ref()
            .expect("Minimal resource")
            .uuid
            .clone(),
        ntype: NodeType::Resource as i32,
        subtype: String::new(),
    };

    let collaborators = ["Anastasia", "Irene"]
        .into_iter()
        .map(|collaborator| RelationNode {
            value: collaborator.to_string(),
            ntype: NodeType::User as i32,
            subtype: "".to_string(),
        });

    let people = ["Anna", "Anthony", "Bárcenas", "Ben", "John"]
        .into_iter()
        .map(|person| RelationNode {
            value: person.to_string(),
            ntype: NodeType::Entity as i32,
            subtype: "person".to_string(),
        });

    let cities = ["Barcelona", "New York", "York"]
        .into_iter()
        .map(|city| RelationNode {
            value: city.to_string(),
            ntype: NodeType::Entity as i32,
            subtype: "city".to_string(),
        });

    let countries = ["Israel", "Netherlands", "Solomon Islands"]
        .into_iter()
        .map(|country| RelationNode {
            value: country.to_string(),
            ntype: NodeType::Entity as i32,
            subtype: "country".to_string(),
        });

    let entities = people.chain(cities).chain(countries);

    let mut relations = vec![];
    relations.extend(collaborators.map(|node| Relation {
        relation: RelationType::Colab as i32,
        source: Some(resource_node.clone()),
        to: Some(node),
        ..Default::default()
    }));
    relations.extend(entities.map(|node| Relation {
        relation: RelationType::Entity as i32,
        source: Some(resource_node.clone()),
        to: Some(node),
        ..Default::default()
    }));

    resource.relations.extend(relations);
}
