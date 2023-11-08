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
use std::cmp::min;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use nucliadb_core::tracing::{debug, warn};
use nucliadb_core::NodeResult;
use nucliadb_protos::{noderesources, replication};
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Response;

use crate::replication::NodeRole;
use crate::settings::Settings;
use crate::shards::providers::unbounded_cache::AsyncUnboundedShardWriterCache;
use crate::shards::providers::AsyncShardWriterProvider;
use crate::utils::list_shards;
pub struct ReplicationServiceGRPCDriver {
    settings: Settings,
    shards: Arc<AsyncUnboundedShardWriterCache>,
    node_id: String,
}

impl ReplicationServiceGRPCDriver {
    pub fn new(
        settings: Settings,
        shard_cache: Arc<AsyncUnboundedShardWriterCache>,
        node_id: String,
    ) -> Self {
        Self {
            settings,
            shards: shard_cache,
            node_id,
        }
    }

    /// This function must be called before using this service
    pub async fn initialize(&self) -> NodeResult<()> {
        // should we do this?
        self.shards.load_all().await?;
        Ok(())
    }
}

async fn stream_file(
    chunk_size: u64,
    shard_path: &Path,
    generation_id: &str,
    rel_filepath: &Path,
    sender: &tokio::sync::mpsc::Sender<Result<replication::ReplicateShardResponse, tonic::Status>>,
) -> NodeResult<()> {
    let filepath = shard_path.join(rel_filepath);

    if !filepath.exists() {
        debug!(
            "File not found when index thought it should be: {}",
            filepath.to_string_lossy(),
        );
        return Ok(());
    }

    debug!("Streaming file {}", filepath.to_string_lossy());
    let mut total = 0;
    let mut chunk = 1;
    let mut file = File::open(filepath.clone()).await?;
    let filesize = file.metadata().await?.len();

    loop {
        let vec_size = min(chunk_size, filesize - total);
        total += chunk_size;
        let mut buf = vec![0; vec_size as usize];
        file.read_exact(buf.as_mut_slice()).await?;
        let reply = replication::ReplicateShardResponse {
            generation_id: generation_id.to_string(),
            filepath: rel_filepath.to_string_lossy().into(),
            data: buf,
            chunk,
            read_position: total,
            total_size: filesize,
        };
        chunk += 1;
        sender.send(Ok(reply)).await.unwrap();
        if total >= filesize {
            return Ok(());
        }
    }
}

async fn stream_data(
    shard_path: &Path,
    generation_id: &str,
    rel_filepath: &Path,
    data: Vec<u8>,
    sender: &tokio::sync::mpsc::Sender<Result<replication::ReplicateShardResponse, tonic::Status>>,
) {
    let filepath = shard_path.join(rel_filepath);
    let filesize = data.len();
    debug!("Streaming file {} {}", filepath.to_string_lossy(), filesize);

    let reply = replication::ReplicateShardResponse {
        generation_id: generation_id.to_string(),
        filepath: rel_filepath.to_string_lossy().into(),
        data,
        chunk: 1,
        read_position: filesize as u64,
        total_size: filesize as u64,
    };
    sender.send(Ok(reply)).await.unwrap();
}

#[tonic::async_trait]
impl replication::replication_service_server::ReplicationService for ReplicationServiceGRPCDriver {
    type ReplicateShardStream =
        ReceiverStream<Result<replication::ReplicateShardResponse, tonic::Status>>;

    async fn check_replication_state(
        &self,
        raw_request: tonic::Request<replication::SecondaryCheckReplicationStateRequest>,
    ) -> Result<tonic::Response<replication::PrimaryCheckReplicationStateResponse>, tonic::Status>
    {
        if self.settings.node_role() != NodeRole::Primary {
            return Err(tonic::Status::unavailable(
                "This node is not a primary node",
            ));
        }
        let request = raw_request.into_inner();
        let mut resp_shard_states = Vec::new();
        let request_shard_states = request.shard_states;
        let shard_ids = list_shards(self.settings.shards_path()).await;
        let shards_to_remove = request_shard_states
            .iter()
            .filter(|s| !shard_ids.contains(&s.shard_id))
            .map(|s| s.shard_id.clone())
            .collect();

        for shard_id in shard_ids {
            let mut shard = self.shards.get(shard_id.clone()).await;
            if shard.is_none() {
                let loaded = self.shards.load(shard_id.clone()).await;
                if loaded.is_err() {
                    warn!("Failed to load shard: {:?}, Error: {:?}", shard_id, loaded);
                    continue;
                }
                shard = Some(loaded.unwrap());
            }
            if let Some(shard) = shard {
                let gen_id = shard.get_generation_id();
                let req_shard_gen_id = request_shard_states
                    .clone()
                    .into_iter()
                    .find(|s| s.shard_id == shard_id)
                    .map(|s| s.generation_id);
                if req_shard_gen_id.is_none()
                    || req_shard_gen_id.unwrap_or("".to_string()) != gen_id
                {
                    resp_shard_states.push(replication::PrimaryShardReplicationState {
                        shard_id,
                        generation_id: shard.get_generation_id(),
                        kbid: shard.get_kbid(),
                        similarity: shard.get_similarity().to_string(),
                    });
                }
            } else {
                warn!("Shard {} not found", shard_id);
            }
        }

        let response = replication::PrimaryCheckReplicationStateResponse {
            shard_states: resp_shard_states,
            shards_to_remove,
            primary_id: self.node_id.clone(),
        };
        Ok(Response::new(response))
    }

    async fn replicate_shard(
        &self,
        raw_request: tonic::Request<replication::ReplicateShardRequest>,
    ) -> Result<tonic::Response<Self::ReplicateShardStream>, tonic::Status> {
        let request = raw_request.into_inner();

        let receiver = tokio::sync::mpsc::channel(4);
        let sender: tokio::sync::mpsc::Sender<
            Result<replication::ReplicateShardResponse, tonic::Status>,
        > = receiver.0.clone();

        let shard_lookup = self.shards.get(request.shard_id.clone()).await;
        if shard_lookup.is_none() {
            return Err(tonic::Status::not_found(format!(
                "Shard {} not found",
                request.shard_id
            )));
        }
        let shard = shard_lookup.unwrap();
        let generation_id = shard.get_generation_id();
        let shard_path = shard.path.clone();
        let chunk_size = request.chunk_size;
        let ignored_segement_ids: HashMap<String, Vec<String>> = request
            .existing_segment_ids
            .iter()
            .map(|(k, v)| (k.clone(), v.items.clone()))
            .collect();

        tokio::spawn(async move {
            // do not allow garbage collection while streaming out shard
            let _gc_lock = shard.gc_lock.lock().await;

            // getting shard files can block during an active write
            let sshard = Arc::clone(&shard); // moved shard reference into blocking task
            let shard_files = tokio::task::spawn_blocking(move || {
                sshard.get_shard_files(&ignored_segement_ids).unwrap()
            })
            .await
            .map_err(|error| {
                tonic::Status::internal(format!("Error getting shard files: {error:?}"))
            })
            .expect("Error getting shard files");

            for segment_files in shard_files {
                for segment_file in segment_files.files {
                    stream_file(
                        chunk_size,
                        &shard_path,
                        &generation_id,
                        &PathBuf::from(segment_file),
                        &sender,
                    )
                    .await
                    .unwrap();
                }
                for (metadata_file, data) in segment_files.metadata_files {
                    stream_data(
                        &shard_path,
                        &generation_id,
                        &PathBuf::from(metadata_file),
                        data,
                        &sender,
                    )
                    .await;
                }
            }

            // top level additional files
            for filename in ["metadata.json", "versions.json"] {
                stream_file(
                    chunk_size,
                    &shard_path,
                    &generation_id,
                    &PathBuf::from(filename),
                    &sender,
                )
                .await
                .unwrap();
            }
        });

        Ok(Response::new(ReceiverStream::new(receiver.1)))
    }

    async fn get_metadata(
        &self,
        _request: tonic::Request<noderesources::EmptyQuery>,
    ) -> Result<tonic::Response<noderesources::NodeMetadata>, tonic::Status> {
        let metadata = crate::node_metadata::NodeMetadata::new(self.settings.clone()).await;
        match metadata {
            Ok(metadata) => Ok(tonic::Response::new(metadata.into())),
            Err(error) => Err(tonic::Status::internal(error.to_string())),
        }
    }
}
