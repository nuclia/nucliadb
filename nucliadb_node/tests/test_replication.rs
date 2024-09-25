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
use std::sync::Arc;

use common::{resources, NodeFixture, TestNodeReader, TestNodeWriter};
use nucliadb_core::protos::{
    op_status, IndexParagraphs, NewShardRequest, NewVectorSetRequest, SearchRequest, SearchResponse, ShardId,
    VectorSetId,
};
use nucliadb_node::replication::health::ReplicationHealthManager;
use rstest::*;
use tonic::Request;

#[rstest]
#[tokio::test]
async fn test_search_replicated_data() -> Result<(), Box<dyn std::error::Error>> {
    let mut fixture = NodeFixture::new();
    fixture.with_writer().await?.with_reader().await?.with_secondary_reader().await?;
    let mut writer = fixture.writer_client();
    let mut reader = fixture.reader_client();
    let mut secondary_reader = fixture.secondary_reader_client();

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let shard = create_shard(&mut writer).await;

    let mut query = create_search_request(&shard.id, "prince", None);
    query.vector = vec![0.5, 0.5, 0.5];
    let response = run_search(&mut reader, query.clone()).await;
    assert!(response.vector.is_some());
    assert!(response.document.is_some());
    assert!(response.paragraph.is_some());
    assert_eq!(response.document.unwrap().results.len(), 2);
    assert_eq!(response.paragraph.unwrap().results.len(), 2);
    assert_eq!(response.vector.unwrap().documents.len(), 1);

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let health_manager_settings = fixture.secondary_settings.clone();
    let repl_health_mng = ReplicationHealthManager::new(health_manager_settings);
    let healthy = repl_health_mng.healthy();
    assert!(healthy);

    // Validate search against secondary
    let response = run_search(&mut secondary_reader, query.clone()).await;
    assert!(response.vector.is_some());
    assert!(response.document.is_some());
    assert!(response.paragraph.is_some());
    assert_eq!(response.document.unwrap().results.len(), 2);
    assert_eq!(response.paragraph.unwrap().results.len(), 2);
    assert_eq!(response.vector.unwrap().documents.len(), 1);

    query.vector = vec![1.0, 2.0, 3.0];
    let response = run_search(&mut secondary_reader, query.clone()).await;
    assert!(response.vector.is_some());
    assert_eq!(response.vector.unwrap().documents.len(), 1);

    // Validate generation id is the same
    let primary_shard = Arc::downgrade(&fixture.primary_shard_cache().get(&shard.id)?);
    let secondary_shard = Arc::downgrade(&fixture.secondary_shard_cache().get(&shard.id)?);

    assert_eq!(
        primary_shard.upgrade().unwrap().metadata.get_generation_id(),
        secondary_shard.upgrade().unwrap().metadata.get_generation_id()
    );

    // Test deleting shard deletes it from secondary
    delete_shard(&mut writer, shard.id.clone()).await;

    // wait for the shard to be deleted
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    assert!(primary_shard.upgrade().is_none());
    assert!(secondary_shard.upgrade().is_none());

    let err_search_result = secondary_reader.search(query).await;

    let _err = err_search_result;

    Ok(())
}

struct ShardDetails {
    id: String,
}

async fn create_shard(writer: &mut TestNodeWriter) -> ShardDetails {
    let request = Request::new(NewShardRequest::default());
    let new_shard_response = writer.new_shard(request).await.expect("Unable to create new shard");
    let shard_id = &new_shard_response.get_ref().id;
    create_test_resources(writer, shard_id.clone(), None).await;
    ShardDetails {
        id: shard_id.to_owned(),
    }
}

async fn delete_shard(writer: &mut TestNodeWriter, shard_id: String) {
    let request = Request::new(ShardId {
        id: shard_id.clone(),
    });
    writer.delete_shard(request).await.expect("Unable to delete shard");
}

async fn create_test_resources(
    writer: &mut TestNodeWriter,
    shard_id: String,
    vectorset: Option<String>,
) -> HashMap<&str, String> {
    let resources = [
        ("little prince", resources::little_prince(&shard_id)),
        ("zarathustra", resources::thus_spoke_zarathustra(&shard_id)),
        ("pap", resources::people_and_places(&shard_id)),
    ];
    let mut resource_uuids = HashMap::new();
    for (name, mut resource) in resources.into_iter() {
        if let Some(ref vectorset) = vectorset {
            for IndexParagraphs {
                paragraphs,
            } in resource.paragraphs.values_mut()
            {
                for p in paragraphs.values_mut() {
                    p.vectorsets_sentences.insert(
                        vectorset.clone(),
                        nucliadb_core::protos::VectorsetSentences {
                            sentences: std::mem::take(&mut p.sentences),
                        },
                    );
                }
            }
        }
        resource_uuids.insert(name, resource.resource.as_ref().unwrap().uuid.clone());
        let response = writer.set_resource(resource).await.unwrap();
        assert_eq!(response.get_ref().status, op_status::Status::Ok as i32);
    }

    resource_uuids
}

fn create_search_request(
    shard_id: impl Into<String>,
    query: impl Into<String>,
    vectorset: Option<String>,
) -> SearchRequest {
    SearchRequest {
        shard: shard_id.into(),
        body: query.into(),
        paragraph: true,
        document: true,
        result_per_page: 10,
        vectorset: vectorset.unwrap_or_default(),
        ..Default::default()
    }
}

async fn run_search(reader: &mut TestNodeReader, request: SearchRequest) -> SearchResponse {
    let response = reader.search(request).await.unwrap();
    response.into_inner()
}

#[rstest]
#[tokio::test]
async fn test_replicate_vectorsets() -> Result<(), Box<dyn std::error::Error>> {
    let mut fixture = NodeFixture::new();
    fixture.with_writer().await?.with_reader().await?.with_secondary_reader().await?;
    let mut writer = fixture.writer_client();
    let mut reader = fixture.reader_client();
    let mut secondary_reader = fixture.secondary_reader_client();

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let shard = create_shard(&mut writer).await;

    // Create a vectorset and insert something
    let request = Request::new(NewVectorSetRequest {
        id: Some(VectorSetId {
            shard: Some(ShardId {
                id: shard.id.clone(),
            }),
            vectorset: "long_vectors".into(),
        }),
        ..Default::default()
    });
    writer.add_vector_set(request).await.expect("Unable to create vectorset");
    create_test_resources(&mut writer, shard.id.clone(), Some("long_vectors".into())).await;

    // Search against primary
    let mut query = create_search_request(&shard.id, "prince", Some("long_vectors".into()));
    query.vector = vec![0.5, 0.5, 0.5];
    let response = run_search(&mut reader, query.clone()).await;
    assert_eq!(response.vector.unwrap().documents.len(), 1);

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let health_manager_settings = fixture.secondary_settings.clone();
    let repl_health_mng = ReplicationHealthManager::new(health_manager_settings);
    let healthy = repl_health_mng.healthy();
    assert!(healthy);

    // Validate search against secondary
    let response = run_search(&mut secondary_reader, query.clone()).await;
    assert_eq!(response.vector.unwrap().documents.len(), 1);

    // Validate generation id is the same
    let primary_shard = Arc::downgrade(&fixture.primary_shard_cache().get(&shard.id)?);
    let secondary_shard = Arc::downgrade(&fixture.secondary_shard_cache().get(&shard.id)?);

    assert_eq!(
        primary_shard.upgrade().unwrap().metadata.get_generation_id(),
        secondary_shard.upgrade().unwrap().metadata.get_generation_id()
    );

    // Test deleting shard deletes it from secondary
    delete_shard(&mut writer, shard.id.clone()).await;

    // wait for the shard to be deleted
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    assert!(primary_shard.upgrade().is_none());
    assert!(secondary_shard.upgrade().is_none());

    let err_search_result = secondary_reader.search(query).await;

    let _err = err_search_result;

    Ok(())
}
