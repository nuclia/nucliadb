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

use common::resources as test_resources;
use common::NodeFixture;
use nucliadb_core::protos::op_status::Status;
use nucliadb_core::protos::NewShardRequest;
use nucliadb_node::shards::indexes::DEFAULT_VECTORS_INDEX_NAME;
use nucliadb_protos::noderesources::ResourceId;
use nucliadb_protos::noderesources::ShardId;
use nucliadb_protos::noderesources::VectorSetId;
use nucliadb_protos::nodewriter::NewVectorSetRequest;
use nucliadb_protos::utils::VectorSimilarity;
use rstest::*;
use tonic::Request;

#[rstest]
#[tokio::test]
async fn test_vectorsets() -> Result<(), Box<dyn std::error::Error>> {
    use nucliadb_core::protos::VectorIndexConfig;

    let mut fixture = NodeFixture::new();
    fixture.with_writer().await?.with_reader().await?;
    let mut writer = fixture.writer_client();
    let _reader = fixture.reader_client();

    let response = writer.new_shard(Request::new(NewShardRequest::default())).await?;
    let shard_id = &response.get_ref().id;

    let response = writer
        .list_vector_sets(ShardId {
            id: shard_id.clone(),
        })
        .await?;
    assert_eq!(
        response.get_ref().shard,
        Some(ShardId {
            id: shard_id.clone()
        })
    );
    assert_eq!(response.get_ref().vectorsets, vec![DEFAULT_VECTORS_INDEX_NAME.to_string()]);

    let vectorset = "gecko".to_string();

    let response = writer
        .add_vector_set(Request::new(NewVectorSetRequest {
            id: Some(VectorSetId {
                shard: Some(ShardId {
                    id: shard_id.clone(),
                }),
                vectorset: vectorset.clone(),
            }),
            config: Some(VectorIndexConfig {
                similarity: VectorSimilarity::Dot.into(),
                normalize_vectors: true,
                ..Default::default()
            }),
            ..Default::default()
        }))
        .await?;
    assert_eq!(response.get_ref().status(), Status::Ok);

    let response = writer
        .list_vector_sets(ShardId {
            id: shard_id.clone(),
        })
        .await?;
    assert_eq!(
        response.get_ref().shard,
        Some(ShardId {
            id: shard_id.clone()
        })
    );
    let mut vectorsets = response.get_ref().vectorsets.clone();
    vectorsets.sort();
    let mut expected = vec![DEFAULT_VECTORS_INDEX_NAME.to_string(), vectorset.clone()];
    expected.sort();
    assert_eq!(vectorsets, expected);

    // Work with multiple vectorsets
    let resource = test_resources::little_prince(shard_id);
    let rid = resource.resource.as_ref().unwrap().uuid.clone();

    let response = writer.set_resource(resource).await?;
    assert_eq!(response.get_ref().status(), Status::Ok);

    let response = writer.set_resource(test_resources::people_and_places(shard_id)).await?;
    assert_eq!(response.get_ref().status(), Status::Ok);

    let response = writer
        .remove_resource(ResourceId {
            shard_id: shard_id.clone(),
            uuid: rid.clone(),
        })
        .await?;
    assert_eq!(response.get_ref().status(), Status::Ok);

    // Removal of the default vectorset is not allowed (yet)
    let response = writer
        .remove_vector_set(VectorSetId {
            shard: Some(ShardId {
                id: shard_id.clone(),
            }),
            vectorset: DEFAULT_VECTORS_INDEX_NAME.to_string(),
        })
        .await?;
    assert_eq!(response.get_ref().status(), Status::Error);
    assert!(response.get_ref().detail.contains("is reserved and can't be removed"));

    // A user-created vectorset can be deleted
    let response = writer
        .remove_vector_set(VectorSetId {
            shard: Some(ShardId {
                id: shard_id.clone(),
            }),
            vectorset: vectorset.clone(),
        })
        .await?;
    assert_eq!(response.get_ref().status(), Status::Ok);

    let response = writer
        .list_vector_sets(ShardId {
            id: shard_id.clone(),
        })
        .await?;
    assert_eq!(
        response.get_ref().shard,
        Some(ShardId {
            id: shard_id.clone()
        })
    );
    assert_eq!(response.get_ref().vectorsets, vec![DEFAULT_VECTORS_INDEX_NAME.to_string()]);

    // TODO: to be continued

    Ok(())
}
