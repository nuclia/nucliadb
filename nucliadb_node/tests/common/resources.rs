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

use std::time::SystemTime;

use nucliadb_core::protos::prost_types::Timestamp;
use nucliadb_core::protos::resource::ResourceStatus;
use nucliadb_core::protos::{IndexMetadata, Resource, ResourceId};
use uuid::Uuid;

pub fn minimal_resource(shard_id: String) -> Resource {
    let resource_id = Uuid::new_v4().to_string();

    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();
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
            shard_id: shard_id,
            uuid: resource_id,
        }),
        status: ResourceStatus::Processed as i32,
        metadata: Some(metadata),
        ..Default::default()
    }
}
