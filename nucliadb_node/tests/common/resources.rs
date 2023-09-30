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
use std::time::SystemTime;

use nucliadb_core::protos::prost_types::Timestamp;
use nucliadb_core::protos::relation::RelationType;
use nucliadb_core::protos::relation_node::NodeType;
use nucliadb_core::protos::resource::ResourceStatus;
use nucliadb_core::protos::{
    IndexMetadata, IndexParagraph, IndexParagraphs, Relation, RelationNode, Resource, ResourceId,
    TextInformation,
};
use uuid::Uuid;

pub fn minimal_resource(shard_id: String) -> Resource {
    let resource_id = Uuid::new_v4().to_string();

    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();
    let timestamp = Timestamp {
        seconds: now.as_secs() as i64,
        nanos: 0,
    };

    let metadata = IndexMetadata {
        created: Some(timestamp.clone()),
        modified: Some(timestamp),
    };

    Resource {
        shard_id: shard_id.clone(),
        resource: Some(ResourceId {
            shard_id,
            uuid: resource_id,
        }),
        status: ResourceStatus::Processed as i32,
        metadata: Some(metadata),
        ..Default::default()
    }
}

pub fn little_prince(shard_id: impl Into<String>) -> Resource {
    let shard_id = shard_id.into();
    let mut resource = minimal_resource(shard_id);
    let rid = &resource.resource.as_ref().unwrap().uuid;

    resource.labels.push("/s/p/en".to_string()); // language=en

    resource.texts.insert(
        "a/title".to_string(),
        TextInformation {
            text: "The little prince".to_string(),
            ..Default::default()
        },
    );
    let mut title_paragraphs = HashMap::new();
    title_paragraphs.insert(
        format!("{rid}/a/title/0-17"),
        IndexParagraph {
            start: 0,
            end: 17,
            field: "a/title".to_string(),
            ..Default::default()
        },
    );
    resource.paragraphs.insert(
        "a/title".to_string(),
        IndexParagraphs {
            paragraphs: title_paragraphs,
        },
    );

    resource.texts.insert(
        "a/summary".to_string(),
        TextInformation {
            text: "The story follows a young prince who visits various planets in space, \
                   including Earth, and addresses themes of loneliness, friendship, love, and \
                   loss."
                .to_string(),
            ..Default::default()
        },
    );
    let mut summary_paragraphs = HashMap::new();
    summary_paragraphs.insert(
        format!("{rid}/a/summary/0-150"),
        IndexParagraph {
            start: 0,
            end: 150,
            field: "a/summary".to_string(),
            ..Default::default()
        },
    );
    resource.paragraphs.insert(
        "a/summary".to_string(),
        IndexParagraphs {
            paragraphs: summary_paragraphs,
        },
    );

    resource
}

pub fn thus_spoke_zarathustra(shard_id: impl Into<String>) -> Resource {
    let shard_id = shard_id.into();
    let mut resource = minimal_resource(shard_id);
    let rid = &resource.resource.as_ref().unwrap().uuid;

    resource.labels.push("/s/p/de".to_string()); // language=de

    resource.texts.insert(
        "a/title".to_string(),
        TextInformation {
            text: "Thus Spoke Zarathustra".to_string(),
            ..Default::default()
        },
    );
    let mut title_paragraphs = HashMap::new();
    title_paragraphs.insert(
        format!("{rid}/a/title/0-22"),
        IndexParagraph {
            start: 0,
            end: 22,
            field: "a/title".to_string(),
            ..Default::default()
        },
    );
    resource.paragraphs.insert(
        "a/title".to_string(),
        IndexParagraphs {
            paragraphs: title_paragraphs,
        },
    );

    resource.texts.insert(
        "a/summary".to_string(),
        TextInformation {
            text: "Philosophical book written by Frederich Nietzche".to_string(),
            ..Default::default()
        },
    );
    let mut summary_paragraphs = HashMap::new();
    summary_paragraphs.insert(
        format!("{rid}/a/summary/0-48"),
        IndexParagraph {
            start: 0,
            end: 48,
            field: "a/summary".to_string(),
            ..Default::default()
        },
    );
    resource.paragraphs.insert(
        "a/summary".to_string(),
        IndexParagraphs {
            paragraphs: summary_paragraphs,
        },
    );

    resource
}

pub fn people_and_places(shard_id: impl Into<String>) -> Resource {
    let shard_id = shard_id.into();

    let mut resource = minimal_resource(shard_id);
    let rid = &resource.resource.as_ref().unwrap().uuid;

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

    let resource_node = RelationNode {
        value: rid.clone(),
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

    resource
}
