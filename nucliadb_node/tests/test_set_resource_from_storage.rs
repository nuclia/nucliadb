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
use object_store::path::Path;

use common::{resources, NodeFixture};
use nucliadb_core::protos::prost::Message;
use nucliadb_core::protos::{IndexMessage, NewShardRequest, ShardId};
use rstest::*;
use tonic::Request;

#[rstest]
#[tokio::test]
async fn test_set_resource_from_storage() -> Result<(), Box<dyn std::error::Error>> {
    let mut fixture = NodeFixture::new();
    fixture.with_writer().await?.with_reader().await?;
    let mut writer = fixture.writer_client();
    let mut reader = fixture.reader_client();

    let request = Request::new(NewShardRequest::default());
    let new_shard_response = writer.new_shard(request).await.expect("Unable to create new shard");
    let shard_id = &new_shard_response.get_ref().id;
    let mut resource = resources::little_prince(shard_id);

    // Clear the shard id to simulate the payload coming from storage
    resource.shard_id.clear();

    let location = Path::from("foobar");
    let bytes = resource.encode_to_vec();
    fixture.settings.object_store.put(&location, bytes.into()).await?;

    let request = IndexMessage {
        shard: shard_id.clone(),
        storage_key: "foobar".into(),
        ..Default::default()
    };
    writer.set_resource_from_storage(request).await?;

    let request = Request::new(ShardId {
        id: shard_id.clone(),
    });
    let document_ids = reader.document_ids(request).await?;
    assert_eq!(document_ids.get_ref().ids.len(), 2);

    Ok(())
}
