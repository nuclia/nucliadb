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
use serde::{Deserialize, Serialize};
use tokio::fs;

use crate::{env, utils};

async fn number_of_shards() -> NodeResult<usize> {
    let mut count = 0;
    let mut entries = fs::read_dir(env::shards_path()).await?;
    while let Some(entry) = entries.next_entry().await? {
        let entry_path = entry.path();
        if entry_path.is_dir() {
            count += 1;
        }
    }

    Ok(count)
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct NodeMetadata {
    #[serde(default)]
    shard_count: u64,
}

impl From<NodeMetadata> for nucliadb_core::protos::NodeMetadata {
    fn from(node_metadata: NodeMetadata) -> Self {
        nucliadb_core::protos::NodeMetadata {
            shard_count: node_metadata.shard_count,
            node_id: utils::read_host_key(env::host_key_path())
                .unwrap()
                .to_string(),
            ..Default::default()
        }
    }
}

impl NodeMetadata {
    pub async fn new() -> NodeResult<Self> {
        Ok(Self {
            shard_count: number_of_shards().await?.try_into().unwrap(),
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
