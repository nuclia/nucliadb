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
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::collections::HashMap;
use std::time::SystemTime;

use nucliadb_core::protos::prost_types::Timestamp;
use nucliadb_core::NodeResult;
use nucliadb_protos::nodereader;
use nucliadb_protos::noderesources;
use nucliadb_vectors::config::{Similarity, VectorConfig};
use tempfile;
use uuid::Uuid;

use crate::disk_structure;
use crate::settings::EnvSettings;
use crate::shards::indexes::DEFAULT_VECTORS_INDEX_NAME;
use crate::shards::reader::ShardReader;

use super::writer::ShardWriter;

#[test]
fn test_vectorsets() -> NodeResult<()> {
    let tempdir = tempfile::tempdir()?;
    let shards_path = tempdir.path().join("shards");
    std::fs::create_dir(&shards_path)?;
    let shard_id = "shard".to_string();
    let kbid = "kbid".to_string();

    let settings = EnvSettings::default().into();
    let (writer, _metadata) = ShardWriter::new(
        crate::shards::writer::NewShard {
            kbid: kbid.clone(),
            shard_id: shard_id.clone(),
            vector_configs: HashMap::from([(DEFAULT_VECTORS_INDEX_NAME.to_string(), VectorConfig::default())]),
        },
        &shards_path,
        &settings,
    )?;
    writer.create_vectors_index(
        "myvectorset".to_string(),
        VectorConfig {
            similarity: Similarity::Cosine,
            normalize_vectors: true,
            ..Default::default()
        },
    )?;

    let vectorsets = writer.list_vectors_indexes();
    assert_eq!(vectorsets.len(), 2);

    let resource = generate_resource(shard_id.clone());
    writer.set_resource(resource)?;

    let reader = ShardReader::new(shard_id.clone(), &disk_structure::shard_path_by_id(&shards_path, &shard_id))?;

    // Search in default vectorset (by omitting vectorset)
    let results = reader.search(nodereader::SearchRequest {
        shard: shard_id.clone(),
        vector: vec![0.7; 100],
        min_score_semantic: 0.0,
        result_per_page: 20,
        ..Default::default()
    })?;
    assert_eq!(results.vector.as_ref().unwrap().documents.len(), 2);
    assert!(results.vector.as_ref().unwrap().documents[0].doc_id.as_ref().unwrap().id.ends_with("#defaultvectors"));
    assert!(results.vector.as_ref().unwrap().documents[1].doc_id.as_ref().unwrap().id.ends_with("#defaultvectors"));

    // Search in default vectorset (using default vectorset string)
    let results = reader.search(nodereader::SearchRequest {
        shard: shard_id.clone(),
        vector: vec![0.7; 100],
        vectorset: DEFAULT_VECTORS_INDEX_NAME.to_string(),
        min_score_semantic: 0.0,
        result_per_page: 20,
        ..Default::default()
    })?;
    assert_eq!(results.vector.as_ref().unwrap().documents.len(), 2);
    assert!(results.vector.as_ref().unwrap().documents[0].doc_id.as_ref().unwrap().id.ends_with("#defaultvectors"));
    assert!(results.vector.as_ref().unwrap().documents[1].doc_id.as_ref().unwrap().id.ends_with("#defaultvectors"));

    // Search in a vectorset
    let results = reader.search(nodereader::SearchRequest {
        shard: shard_id.clone(),
        vector: vec![0.7; 100],
        vectorset: "myvectorset".to_string(),
        min_score_semantic: 0.0,
        result_per_page: 20,
        ..Default::default()
    })?;
    assert_eq!(results.vector.as_ref().unwrap().documents.len(), 1);
    assert!(results.vector.as_ref().unwrap().documents[0].doc_id.as_ref().unwrap().id.ends_with("#vectorset"));

    Ok(())
}

/// Generate a Resource to index in multiple vectorsets.
///
/// Vector keys are suffixed so we'll be able to recognize where are they
/// from in the tests
fn generate_resource(shard_id: String) -> noderesources::Resource {
    let mut resource = minimal_resource(shard_id);
    let rid = &resource.resource.as_ref().unwrap().uuid;

    resource.texts.insert(
        "a/title".to_string(),
        noderesources::TextInformation {
            text: "Testing vectorsets".to_string(),
            ..Default::default()
        },
    );

    resource.paragraphs.insert(
        "a/title".to_string(),
        noderesources::IndexParagraphs {
            paragraphs: HashMap::from([(
                format!("{rid}/a/title/0-18"),
                noderesources::IndexParagraph {
                    start: 0,
                    end: 18,
                    field: "a/title".to_string(),
                    sentences: HashMap::from([
                        (
                            format!("{rid}/a/title/0-9#defaultvectors"),
                            noderesources::VectorSentence {
                                vector: vec![0.5; 100],
                                ..Default::default()
                            },
                        ),
                        (
                            format!("{rid}/a/title/9-18#defaultvectors"),
                            noderesources::VectorSentence {
                                vector: vec![0.7; 100],
                                ..Default::default()
                            },
                        ),
                    ]),
                    vectorsets_sentences: HashMap::from([(
                        "myvectorset".to_string(),
                        noderesources::VectorsetSentences {
                            sentences: HashMap::from([(
                                format!("{rid}/a/title/0-18#vectorset"),
                                noderesources::VectorSentence {
                                    vector: vec![0.2; 100],
                                    ..Default::default()
                                },
                            )]),
                        },
                    )]),
                    ..Default::default()
                },
            )]),
        },
    );

    resource
}

fn minimal_resource(shard_id: String) -> noderesources::Resource {
    let resource_id = Uuid::new_v4().to_string();

    let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap();
    let timestamp = Timestamp {
        seconds: now.as_secs() as i64,
        nanos: 0,
    };

    let metadata = noderesources::IndexMetadata {
        created: Some(timestamp.clone()),
        modified: Some(timestamp),
    };

    noderesources::Resource {
        shard_id: shard_id.clone(),
        resource: Some(noderesources::ResourceId {
            shard_id,
            uuid: resource_id,
        }),
        status: noderesources::resource::ResourceStatus::Processed as i32,
        metadata: Some(metadata),
        ..Default::default()
    }
}
