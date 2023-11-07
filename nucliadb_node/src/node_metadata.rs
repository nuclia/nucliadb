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

use nucliadb_core::NodeResult;

use crate::settings::Settings;
use crate::utils::{self, get_primary_node_id, list_shards};

pub struct NodeMetadata {
    shard_count: u64,
    settings: Settings,
}

impl From<NodeMetadata> for nucliadb_core::protos::NodeMetadata {
    fn from(node_metadata: NodeMetadata) -> Self {
        nucliadb_core::protos::NodeMetadata {
            shard_count: node_metadata.shard_count,
            node_id: utils::read_host_key(node_metadata.settings.host_key_path())
                .unwrap()
                .to_string(),
            primary_node_id: get_primary_node_id(node_metadata.settings.data_path()),
            ..Default::default()
        }
    }
}

pub fn create_node_metadata_pb(
    settings: Settings,
    node_metadata: NodeMetadata,
) -> nucliadb_core::protos::NodeMetadata {
    nucliadb_core::protos::NodeMetadata {
        shard_count: node_metadata.shard_count,
        node_id: utils::read_host_key(settings.host_key_path())
            .unwrap()
            .to_string(),
        primary_node_id: get_primary_node_id(settings.data_path()),
        ..Default::default()
    }
}

impl NodeMetadata {
    pub async fn new(settings: Settings) -> NodeResult<Self> {
        Ok(Self {
            settings: settings.clone(),
            shard_count: list_shards(settings.shards_path())
                .await
                .len()
                .try_into()
                .unwrap(),
        })
    }

    pub fn shard_count(&self) -> u64 {
        self.shard_count
    }

    pub fn new_shard(&mut self) {
        self.shard_count += 1;
    }

    pub fn delete_shard(&mut self) {
        self.shard_count -= 1;
    }
}
