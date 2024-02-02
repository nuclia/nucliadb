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
use std::time::SystemTime;

use ::nucliadb_protos::utils::Security;
use common::{NodeFixture, TestNodeWriter};
use nucliadb_core::protos as nucliadb_protos;
use nucliadb_protos::op_status::Status;
use nucliadb_protos::prost_types::Timestamp;
use nucliadb_protos::resource::ResourceStatus;
use nucliadb_protos::{IndexMetadata, NewShardRequest, Resource, ResourceId, SearchRequest};
use tonic::Request;
use uuid::Uuid;

async fn create_dummy_resources(total: u8, writer: &mut TestNodeWriter, shard_id: String, access_groups: Vec<String>) {
    for i in 0..total {
        println!("Creating dummy resource {}/{}", i, total);
        let rid = Uuid::new_v4();
        let field = format!("dummy-{i:0>3}");

        let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap();
        let timestamp = Timestamp {
            seconds: now.as_secs() as i64 - (total - i) as i64,
            nanos: 0,
        };

        let labels = vec![format!("/dummy{i:0>3}")];
        let mut texts = HashMap::new();
        texts.insert(
            field,
            nucliadb_protos::TextInformation {
                text: format!("Dummy text {i:0>3}"),
                labels: vec![],
            },
        );
        let mut security = Security::default();
        if !access_groups.is_empty() {
            security.access_groups = access_groups.clone();
        }

        let result = writer
            .set_resource(Resource {
                shard_id: shard_id.clone(),
                resource: Some(ResourceId {
                    shard_id: shard_id.clone(),
                    uuid: rid.to_string(),
                }),
                status: ResourceStatus::Processed as i32,
                metadata: Some(IndexMetadata {
                    created: Some(timestamp.clone()),
                    modified: Some(timestamp),
                }),
                labels,
                texts,
                security: Some(security),
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(result.get_ref().status(), Status::Ok);
    }
}

#[tokio::test]
async fn test_security_search() -> Result<(), Box<dyn std::error::Error>> {
    let mut fixture = NodeFixture::new();
    fixture.with_writer().await?.with_reader().await?;
    let mut writer = fixture.writer_client();
    let mut reader = fixture.reader_client();

    let new_shard_response = writer.new_shard(Request::new(NewShardRequest::default())).await?;
    let shard_id = &new_shard_response.get_ref().id;

    let group_engineering = "engineering".to_string();

    create_dummy_resources(1, &mut writer, shard_id.clone(), vec![group_engineering.clone()]).await;

    // Searching with no security should return 1 results
    let response = reader
        .search(SearchRequest {
            body: "text".to_string(),
            shard: shard_id.clone(),
            document: true,
            security: Some(Security {
                access_groups: vec![],
            }),
            ..Default::default()
        })
        .await
        .unwrap();

    assert_eq!(response.get_ref().document.as_ref().unwrap().total, 1);

    // Searching with an unknown group should return no results
    let response = reader
        .search(SearchRequest {
            shard: shard_id.clone(),
            document: true,
            security: Some(Security {
                access_groups: vec!["unknown".to_string()],
            }),
            ..Default::default()
        })
        .await
        .unwrap();

    assert_eq!(response.get_ref().document.as_ref(), None);

    // Searching with engineering group should return only one result
    let response = reader
        .search(SearchRequest {
            shard: shard_id.clone(),
            document: true,
            security: Some(Security {
                access_groups: vec![group_engineering.clone()],
            }),
            ..Default::default()
        })
        .await
        .unwrap();

    assert_eq!(response.get_ref().document.as_ref().unwrap().total, 1);

    // Searching with engineering group and another unknown group
    // should return 1 result, as the results are expected to be the union of the groups.
    let response = reader
        .search(SearchRequest {
            shard: shard_id.clone(),
            document: true,
            security: Some(Security {
                access_groups: vec!["/unknown".to_string(), group_engineering.clone()],
            }),
            ..Default::default()
        })
        .await
        .unwrap();

    assert_eq!(response.get_ref().document.as_ref().unwrap().total, 1);

    Ok(())
}

#[tokio::test]
async fn test_security_search_public_resource() -> Result<(), Box<dyn std::error::Error>> {
    let mut fixture = NodeFixture::new();
    fixture.with_writer().await?.with_reader().await?;
    let mut writer = fixture.writer_client();
    let mut reader = fixture.reader_client();

    let new_shard_response = writer.new_shard(Request::new(NewShardRequest::default())).await?;
    let shard_id = &new_shard_response.get_ref().id;

    create_dummy_resources(1, &mut writer, shard_id.clone(), vec![]).await;

    // Searching with no security should return 1 results
    let response = reader
        .search(SearchRequest {
            body: "text".to_string(),
            shard: shard_id.clone(),
            document: true,
            security: Some(Security {
                access_groups: vec![],
            }),
            ..Default::default()
        })
        .await
        .unwrap();

    assert_eq!(response.get_ref().document.as_ref().unwrap().total, 1);

    // Searching with an unknown group should return one result too, as the resource is public
    let response = reader
        .search(SearchRequest {
            shard: shard_id.clone(),
            document: true,
            security: Some(Security {
                access_groups: vec!["/unknown".to_string()],
            }),
            ..Default::default()
        })
        .await
        .unwrap();

    assert_eq!(response.get_ref().document.as_ref().unwrap().total, 1);

    Ok(())
}
