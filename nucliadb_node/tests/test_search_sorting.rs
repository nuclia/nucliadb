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

use common::{NodeFixture, TestNodeReader, TestNodeWriter};
use nucliadb_core::protos as nucliadb_protos;
use nucliadb_protos::op_status::Status;
use nucliadb_protos::prost_types::Timestamp;
use nucliadb_protos::resource::ResourceStatus;
use nucliadb_protos::{IndexMetadata, NewShardRequest, Resource, ResourceId, SearchRequest};
use tonic::Request;
use uuid::Uuid;

async fn create_dummy_resources(total: u8, writer: &mut TestNodeWriter, shard_id: String) {
    for i in 0..total {
        let rid = Uuid::new_v4();
        let field = format!("dummy-{i:0>3}");

        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();
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
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(result.get_ref().status(), Status::Ok);
    }
}

#[tokio::test]
async fn test_search_sorting() -> Result<(), Box<dyn std::error::Error>> {
    let mut fixture = NodeFixture::new();
    fixture.with_writer().await?.with_reader().await?;
    let mut writer = fixture.writer_client();
    let mut reader = fixture.reader_client();

    let new_shard_response = writer
        .new_shard(Request::new(NewShardRequest::default()))
        .await?;
    let shard_id = &new_shard_response.get_ref().id;

    create_dummy_resources(20, &mut writer, shard_id.clone()).await;

    async fn paginated_search(
        reader: &mut TestNodeReader,
        shard_id: String,
        order: Option<nucliadb_protos::OrderBy>,
        page_size: i32,
    ) -> Vec<String> {
        let mut fields = Vec::new();
        let mut page = 0;
        let mut next_page = true;

        while next_page {
            let response = reader
                .search(SearchRequest {
                    shard: shard_id.clone(),
                    order: order.clone(),
                    document: true,
                    page_number: page,
                    result_per_page: page_size,
                    ..Default::default()
                })
                .await
                .unwrap();

            let document_response = response.get_ref().document.as_ref().unwrap();
            fields.extend(document_response.results.iter().cloned().map(|r| r.field));

            next_page = document_response.next_page;
            page += 1;
        }

        fields
    }

    // --------------------------------------------------------------
    // Test: sort by creation date in ascending order
    // --------------------------------------------------------------

    let order = Some(nucliadb_protos::OrderBy {
        sort_by: nucliadb_protos::order_by::OrderField::Created.into(),
        r#type: nucliadb_protos::order_by::OrderType::Asc.into(),
        ..Default::default()
    });
    let fields = paginated_search(&mut reader, shard_id.clone(), order, 5).await;
    let mut sorted_fields = fields.clone();
    sorted_fields.sort();
    assert_eq!(fields, sorted_fields);

    // --------------------------------------------------------------
    // Test: sort by modified date in ascending order
    // --------------------------------------------------------------

    let order = Some(nucliadb_protos::OrderBy {
        sort_by: nucliadb_protos::order_by::OrderField::Modified.into(),
        r#type: nucliadb_protos::order_by::OrderType::Asc.into(),
        ..Default::default()
    });
    let fields = paginated_search(&mut reader, shard_id.clone(), order, 5).await;

    let mut sorted_fields = fields.clone();
    sorted_fields.sort();
    assert_eq!(fields, sorted_fields);

    // --------------------------------------------------------------
    // Test: sort by creation date in descending order
    // --------------------------------------------------------------

    let order = Some(nucliadb_protos::OrderBy {
        sort_by: nucliadb_protos::order_by::OrderField::Created.into(),
        r#type: nucliadb_protos::order_by::OrderType::Desc.into(),
        ..Default::default()
    });
    let fields = paginated_search(&mut reader, shard_id.clone(), order, 5).await;

    let mut sorted_fields = fields.clone();
    sorted_fields.sort();
    sorted_fields.reverse();
    assert_eq!(fields, sorted_fields);

    // --------------------------------------------------------------
    // Test: sort by modified date in descending order
    // --------------------------------------------------------------

    let order = Some(nucliadb_protos::OrderBy {
        sort_by: nucliadb_protos::order_by::OrderField::Modified.into(),
        r#type: nucliadb_protos::order_by::OrderType::Desc.into(),
        ..Default::default()
    });
    let fields = paginated_search(&mut reader, shard_id.clone(), order, 5).await;

    let mut sorted_fields = fields.clone();
    sorted_fields.sort();
    sorted_fields.reverse();
    assert_eq!(fields, sorted_fields);

    Ok(())
}
