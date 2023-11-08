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
use nucliadb_core::protos::{
    op_status, NewShardRequest, NewVectorSetRequest, SearchRequest, SearchResponse, ShardId,
    UserVector, UserVectors, VectorSetId, VectorSimilarity,
};
use nucliadb_node::replication::health::ReplicationHealthManager;
use nucliadb_node::shards::providers::AsyncShardWriterProvider;
use tonic::Request;

#[tokio::test]
async fn test_search_replicated_data() -> Result<(), Box<dyn std::error::Error>> {
    let mut fixture = NodeFixture::new();
    fixture
        .with_writer()
        .await?
        .with_reader()
        .await?
        .with_secondary_reader()
        .await?;
    let mut writer = fixture.writer_client();
    let mut reader = fixture.reader_client();
    let mut secondary_reader = fixture.secondary_reader_client();

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let shard = create_shard(&mut writer).await;

    let mut query = create_search_request(&shard.id, "prince");
    query.vector = vec![1.0, 2.0, 3.0];
    let response = run_search(&mut reader, query.clone()).await;
    assert!(response.vector.is_some());
    assert!(response.document.is_some());
    assert!(response.paragraph.is_some());
    assert_eq!(response.document.unwrap().results.len(), 2);
    assert_eq!(response.paragraph.unwrap().results.len(), 2);
    assert_eq!(response.vector.unwrap().documents.len(), 0);

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let health_manager_settings = fixture.secondary_settings.clone();
    let repl_health_mng = ReplicationHealthManager::new(health_manager_settings);
    let healthy = repl_health_mng.healthy();
    assert!(healthy);

    let response = run_search(&mut secondary_reader, query.clone()).await;
    assert!(response.vector.is_some());
    assert!(response.document.is_some());
    assert!(response.paragraph.is_some());
    assert_eq!(response.document.unwrap().results.len(), 2);
    assert_eq!(response.paragraph.unwrap().results.len(), 2);
    assert_eq!(response.vector.unwrap().documents.len(), 0);

    query.vectorset = "test_vector_set".to_string();
    query.vector = vec![1.0, 2.0, 3.0];

    // Validate search against secondary
    let response = run_search(&mut secondary_reader, query.clone()).await;
    assert!(response.vector.is_some());
    assert_eq!(response.vector.unwrap().documents.len(), 1);

    // Validate generation id is the same
    let primary_shard = fixture.primary_shard_cache().load(shard.id.clone()).await?;
    let secondary_shard = fixture
        .secondary_shard_cache()
        .load(shard.id.clone())
        .await?;

    assert_eq!(
        primary_shard.get_generation_id(),
        secondary_shard.get_generation_id()
    );

    // Test deleting shard deletes it from secondary
    delete_shard(&mut writer, shard.id.clone()).await;

    // wait for the shard to be deleted
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let err_search_result = secondary_reader.search(query).await;
    assert!(err_search_result.is_err());

    Ok(())
}

struct ShardDetails {
    id: String,
}

async fn create_shard(writer: &mut TestNodeWriter) -> ShardDetails {
    let request = Request::new(NewShardRequest::default());
    let new_shard_response = writer
        .new_shard(request)
        .await
        .expect("Unable to create new shard");
    let shard_id = &new_shard_response.get_ref().id;

    writer
        .add_vector_set(NewVectorSetRequest {
            id: Some(VectorSetId {
                shard: Some(ShardId {
                    id: shard_id.clone(),
                }),
                vectorset: "test_vector_set".to_string(),
            }),
            similarity: VectorSimilarity::Cosine as i32,
        })
        .await
        .expect("Unable to create vector set");
    create_test_resources(writer, shard_id.clone()).await;
    ShardDetails {
        id: shard_id.to_owned(),
    }
}

async fn delete_shard(writer: &mut TestNodeWriter, shard_id: String) {
    let request = Request::new(ShardId {
        id: shard_id.clone(),
    });
    writer
        .delete_shard(request)
        .await
        .expect("Unable to delete shard");
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

    let mut user_vectors = HashMap::new();
    user_vectors.insert(
        "test_vector".to_string(),
        UserVector {
            vector: vec![1.0, 2.0, 3.0],
            labels: vec!["label1".to_string(), "label2".to_string()],
            start: 0,
            end: 3,
        },
    );

    for (name, mut resource) in resources.into_iter() {
        resource_uuids.insert(name, resource.resource.as_ref().unwrap().uuid.clone());
        resource.vectors.insert(
            "test_vector_set".to_string(),
            UserVectors {
                vectors: user_vectors.clone(),
            },
        );
        let response = writer.set_resource(resource).await.unwrap();
        assert_eq!(response.get_ref().status, op_status::Status::Ok as i32);
    }

    resource_uuids
}

fn create_search_request(shard_id: impl Into<String>, query: impl Into<String>) -> SearchRequest {
    SearchRequest {
        shard: shard_id.into(),
        body: query.into(),
        paragraph: true,
        document: true,
        result_per_page: 10,
        ..Default::default()
    }
}

async fn run_search(reader: &mut TestNodeReader, request: SearchRequest) -> SearchResponse {
    let response = reader.search(request).await.unwrap();
    response.into_inner()
}
