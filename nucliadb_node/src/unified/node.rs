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

use nucliadb_index::index;
use std::sync::{Arc, Mutex};

use super::{replication::PrimaryReplicator, shards::ShardManager};

pub struct ShardWriter {
    shard_manager: Arc<Mutex<ShardManager>>,
    primary_replicator: Arc<Mutex<PrimaryReplicator>>,
}

impl ShardWriter {
    pub fn new(
        shard_manager: Arc<Mutex<ShardManager>>,
        primary_replicator: Arc<Mutex<PrimaryReplicator>>,
    ) -> Self {
        Self {
            shard_manager,
            primary_replicator,
        }
    }

    pub fn index_resource(
        &mut self,
        shard_id: &str,
        resource_data: index::ResourceData,
    ) -> Result<u64, String> {
        let shard = self.shard_manager.lock().unwrap().get_shard(shard_id);

        if !shard.is_ok() {
            return Err(format!("Shard {} does not exist", shard_id));
        } else {
            let indexing_result = shard
                .unwrap()
                .lock()
                .unwrap()
                .index_resource(resource_data)
                .unwrap();
            self.primary_replicator
                .lock()
                .unwrap()
                .commit(shard_id, indexing_result.segment_entry_id);

            Ok(indexing_result.segment_entry_id)
        }
    }

    pub fn delete_resource(&mut self, shard_id: &str, resource_id: &str) -> Result<u64, String> {
        let shard = self.shard_manager.lock().unwrap().get_shard(shard_id);

        if !shard.is_ok() {
            return Err(format!("Shard {} does not exist", shard_id));
        } else {
            let indexing_result = shard
                .unwrap()
                .lock()
                .unwrap()
                .delete_resource(resource_id)
                .unwrap();
            self.primary_replicator
                .lock()
                .unwrap()
                .commit(shard_id, indexing_result.segment_entry_id);

            Ok(indexing_result.segment_entry_id)
        }
    }
}
