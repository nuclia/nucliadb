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
use nucliadb_core::protos::{NewShardRequest, ReleaseChannel};
use nucliadb_protos::noderesources::ShardId;
use nucliadb_protos::noderesources::VectorSetId;
use nucliadb_protos::nodewriter::NewVectorSetRequest;
use nucliadb_protos::utils::VectorSimilarity;
use rstest::*;
use tonic::Request;

#[rstest]
#[tokio::test]
async fn test_vectorsets(
    #[values(ReleaseChannel::Stable, ReleaseChannel::Experimental)] release_channel: ReleaseChannel,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut fixture = NodeFixture::new();
    fixture.with_writer().await?.with_reader().await?;
    let mut writer = fixture.writer_client();
    let _reader = fixture.reader_client();

    let new_shard_response = writer
        .new_shard(Request::new(NewShardRequest {
            release_channel: release_channel.into(),
            ..Default::default()
        }))
        .await?;
    let shard_id = &new_shard_response.get_ref().id;

    let new_vectorset_response = writer
        .add_vector_set(Request::new(NewVectorSetRequest {
            id: Some(VectorSetId {
                shard: Some(ShardId {
                    id: shard_id.clone(),
                }),
                vectorset: "gecko".to_string(),
            }),
            similarity: VectorSimilarity::Dot.into(),
            normalize_vectors: true,
        }))
        .await?;
    assert_eq!(new_vectorset_response.get_ref().status(), Status::Ok);

    let result = writer.set_resource(test_resources::little_prince(shard_id)).await?;
    assert_eq!(result.get_ref().status(), Status::Ok);

    // TODO: to be continued

    Ok(())
}
