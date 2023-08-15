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
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use nucliadb_protos::unified;
use tokio_stream;
use tonic::Request;

pub struct ShardCommit {
    pub shard_id: String,
    pub position: u64,
    pub segments: Vec<String>,
}

pub struct PrimaryReplicator {
    // hashmap of [secondary][shard] -> position
    secondary_positions: HashMap<String, HashMap<String, u64>>,
    shard_manager: Arc<Mutex<super::shards::ShardManager>>,
}

pub struct SecondaryReplicator {
    shard_manager: Arc<Mutex<super::shards::ShardManager>>,
}

impl PrimaryReplicator {
    pub fn new(shard_manager: Arc<Mutex<super::shards::ShardManager>>) -> Self {
        // on startup, load shard positions from disk
        Self {
            secondary_positions: HashMap::new(),
            shard_manager: shard_manager,
        }
    }

    pub fn secondary_committed(&mut self, secondary_id: &str, shard_id: &str, position: u64) {
        /*
        Updated secondary position.
        */
        let secondary_positions = self
            .secondary_positions
            .entry(secondary_id.to_string())
            .or_insert_with(HashMap::new);

        secondary_positions.insert(shard_id.to_string(), position);
    }

    pub fn unregister_secondary(&mut self, secondary: &str) {
        /*
        Unregister a secondary.
        */
        if self.secondary_positions.contains_key(secondary) {
            self.secondary_positions.remove(secondary);
        }
    }

    pub fn commit(&self, shard_id: &str, segment_id: u64) {
        // XXX
        // Called to allow a "hook" to signal replicator to push out
        // more commits but not implemented yet so
        // we do it only on a poll.
        // XXX
    }

    pub fn get_commits(&self, secondary_id: &str, limit: u16) -> Vec<ShardCommit> {
        /*
        Get commits for a secondary.
        */
        let mut commits = Vec::new();
        let sm = self.shard_manager.lock().unwrap();

        for shard_id in sm.get_shard_ids() {
            let index_index_look = sm.get_shard(shard_id.as_str()).unwrap();
            let index_shard = index_index_look.lock().unwrap();
            let position = index_shard.log_position();

            let mut push_changes = false;
            if self.secondary_positions.contains_key(secondary_id)
                && self.secondary_positions[secondary_id].contains_key(shard_id.as_str())
            {
                if self.secondary_positions[secondary_id][shard_id.as_str()] < position {
                    // We have a new commit
                    push_changes = true;
                }
            } else {
                // We have a new shard
                push_changes = true;
            }

            if push_changes {
                // We have a commits to send, pull current list of segments from
                // the active shard
                {
                    commits.push(ShardCommit {
                        shard_id: shard_id.to_string(),
                        position: position,
                        segments: index_shard
                            .alive_segments()
                            .into_iter()
                            .map(|e| e.to_string())
                            .collect(),
                    });
                }
            }
            if commits.len() >= limit as usize {
                break;
            }
        }

        commits
    }

    pub fn get_segment_filepath(&self, shard_id: &str, segment_id: &str) -> String {
        // get segment file
        format!("{}/{}", shard_id, segment_id)
    }
}

impl SecondaryReplicator {
    pub fn new(shard_manager: Arc<Mutex<super::shards::ShardManager>>) -> Self {
        Self {
            shard_manager: shard_manager,
        }
    }

    pub fn commit(&mut self, shard_id: &str, _segments: Vec<String>, position: u64) {
        // XXX
        // download all missing segments
        // update list of segments
        // commit
        // delete old segments

        // let idx_shard = self
        //     .shard_manager
        //     .lock()
        //     .unwrap()
        //     .get_shard(shard_id)
        //     .unwrap();
        // let idx_shard = idx_shard.lock().unwrap();

        // let existing_segments = idx_shard.alive_segments().into_iter().collect::<Vec<_>>();
    }

    pub fn get_position(&self, shard_id: &str) -> u64 {
        let index_shard = self.shard_manager.lock().unwrap().get_shard(shard_id);
        if index_shard.is_err() {
            // XXX shard could be deleted and will need to be handled as well
            return 0;
        }
        index_shard.unwrap().lock().unwrap().log_position()
    }

    pub fn get_positions(&self) -> Vec<(String, u64)> {
        let mut positions = Vec::new();
        let sm = self.shard_manager.lock().unwrap();

        for shard_id in sm.get_shard_ids() {
            let index_shard = self
                .shard_manager
                .lock()
                .unwrap()
                .get_shard(shard_id.as_str());
            if index_shard.is_err() {
                continue;
            }
            positions.push((
                shard_id,
                index_shard.unwrap().lock().unwrap().log_position(),
            ));
        }
        positions
    }
}

pub async fn connect_to_primary_and_replicate(
    primary_address: String,
    secondary_id: String,
    replicator: Arc<Mutex<SecondaryReplicator>>,
) -> Result<(), tonic::transport::Error> {
    let mut client =
        unified::node_service_client::NodeServiceClient::connect(primary_address).await?;
    // .max_decoding_message_size(256 * 1024 * 1024)
    // .max_encoding_message_size(256 * 1024 * 1024);

    loop {
        let mut stream_data = Vec::new();
        stream_data.push(unified::SecondaryReplicateRequest {
            secondary_id: secondary_id.clone(),
            positions: replicator
                .lock()
                .unwrap()
                .get_positions()
                .into_iter()
                .map(|(k, v)| unified::ShardReplicationPosition {
                    shard_id: k,
                    position: v,
                })
                .collect(),
        });
        // start streaming request
        let mut stream = client
            .replicate(Request::new(tokio_stream::iter(stream_data)))
            .await
            .unwrap()
            .into_inner();

        let mut no_new_commits = true;
        while let Some(resp) = stream.message().await.unwrap() {
            for commit in resp.commits {
                if replicator
                    .lock()
                    .unwrap()
                    .get_position(commit.shard_id.as_str())
                    < commit.position
                {
                    replicator.lock().unwrap().commit(
                        &commit.shard_id,
                        commit.segments,
                        commit.position,
                    );
                    no_new_commits = false;
                }
            }
        }
        if no_new_commits {
            // wait a bit before trying again
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }
    }
}
