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
use std::{collections::HashMap, time::SystemTime};

use nucliadb_core::protos::prost_types::Timestamp;
use nucliadb_core::protos::*;
use resource::ResourceStatus;
use uuid::Uuid;

pub fn minimal_resource(shard_id: String) -> Resource {
    let resource_id = Uuid::new_v4().to_string();

    let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap();
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
            sentences: HashMap::from([(
                format!("{rid}/a/summary/0-150"),
                VectorSentence {
                    vector: vec![0.5, 0.5, 0.5],
                    metadata: None,
                },
            )]),
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
