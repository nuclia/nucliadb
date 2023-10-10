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

use common::{resources, NodeFixture, TestNodeWriter};
use nucliadb_core::protos::{
    op_status, DownloadShardFileRequest, GetShardFilesRequest, NewShardRequest,
};
use serde_json::value::Value;
use tonic::Request;

#[tokio::test]
async fn test_download_shard() -> Result<(), Box<dyn std::error::Error>> {
    let mut fixture = NodeFixture::new();
    fixture.with_writer().await?.with_reader().await?;
    let mut writer = fixture.writer_client();
    let mut reader = fixture.reader_client();

    let shard = create_shard(&mut writer).await;

    // lets download it, first we get the list of files
    let response = reader
        .get_shard_files(Request::new(GetShardFilesRequest {
            shard_id: shard.id.clone(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(!response.files.is_empty());

    // then we iterate on them
    for shard_file in response.files.into_iter() {
        let req = DownloadShardFileRequest {
            shard_id: shard.id.clone(),
            relative_path: shard_file.relative_path.clone(),
        };

        // grabbing a JSON file and making sure it's readable
        if shard_file.relative_path == "paragraph/meta.json" {
            let mut content = String::new();
            let mut response_stream = reader
                .download_shard_file(Request::new(req))
                .await
                .unwrap()
                .into_inner();

            while let Some(response) = response_stream.message().await? {
                // Process the response element (FileChunk in this case).
                let chunk = String::from_utf8_lossy(&response.data);
                content.push_str(&chunk);
            }

            assert!(serde_json::from_str::<Value>(content.as_str()).is_ok());
        }
    }

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
    create_test_resources(writer, shard_id.clone()).await;
    ShardDetails {
        id: shard_id.to_owned(),
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
