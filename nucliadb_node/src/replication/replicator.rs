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

use std::fs;
use std::path::Path;
use std::sync::Arc;

use nucliadb_core::metrics::replication as replication_metrics;
use nucliadb_core::tracing::{error, info, warn};
use nucliadb_core::{metrics, Error, NodeResult};
use nucliadb_protos::replication;
use tokio::io::AsyncWriteExt;
use tonic::Request;

use crate::replication::health::ReplicationHealthManager;
use crate::settings::Settings;
use crate::shards::metadata::ShardMetadata;
use crate::shards::providers::unbounded_cache::AsyncUnboundedShardWriterCache;
use crate::shards::providers::AsyncShardWriterProvider;
use crate::shards::writer::ShardWriter;
use crate::utils::list_shards;

pub async fn replicate_shard(
    shard_state: replication::PrimaryShardReplicationState,
    mut client: replication::replication_service_client::ReplicationServiceClient<
        tonic::transport::Channel,
    >,
    shard: Arc<ShardWriter>,
) -> NodeResult<()> {
    let metrics = metrics::get_metrics();
    let existing_segment_ids = shard
        .get_shard_segments()?
        .iter()
        .map(|(seg_type, seg_ids)| {
            (
                seg_type.clone(),
                replication::SegmentIds {
                    items: seg_ids.clone(),
                },
            )
        })
        .collect();
    let mut stream = client
        .replicate_shard(Request::new(replication::ReplicateShardRequest {
            shard_id: shard_state.shard_id.clone(),
            chunk_size: 1024 * 1024 * 2,
            existing_segment_ids,
        }))
        .await?
        .into_inner();

    let shard_path = shard.path.clone();
    let replicate_work_path = format!("{}/replication", shard_path.to_string_lossy());
    // create replication work path if not exists
    if !std::path::Path::new(&replicate_work_path).exists() {
        std::fs::create_dir_all(&replicate_work_path)?;
    }
    let mut generation_id = None;
    let mut filepath = None;
    let mut temp_filepath = format!("{}/{}", replicate_work_path.clone(), uuid::Uuid::new_v4());
    let mut current_read_bytes = 0;
    let mut file = tokio::fs::File::create(temp_filepath.clone()).await?;
    let mut start = std::time::SystemTime::now();
    while let Some(resp) = stream.message().await? {
        if generation_id.is_none() {
            generation_id = Some(resp.generation_id.clone());
            // write generation id to shard
            shard.set_generation_id(resp.generation_id);
        }
        if filepath.is_some() && Some(resp.filepath.clone()) != filepath {
            std::fs::remove_file(temp_filepath)?;
            return Err(Error::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "We should have finished previous file before starting a new one",
            )));
        }
        if filepath.is_none() {
            filepath = Some(resp.filepath.clone());
        }

        file.write_all(&resp.data).await?;
        let took = start
            .elapsed()
            .map(|elapsed| elapsed.as_secs_f64())
            .unwrap_or(f64::NAN);
        current_read_bytes += resp.data.len() as u64;

        metrics.record_replicated_bytes(took / resp.data.len() as f64);

        if current_read_bytes == resp.total_size {
            // finish copying file
            file.flush().await?;
            file.sync_all().await?;
            // close file
            drop(file);

            let dest_filepath = format!(
                "{}/{}",
                shard_path.to_string_lossy(),
                filepath.clone().unwrap()
            );
            // check if path exists
            if std::path::Path::new(&dest_filepath).exists() {
                std::fs::remove_file(dest_filepath.clone())?;
            }
            // mkdirs directory if not exists
            if let Some(parent) = Path::new(&dest_filepath).parent() {
                fs::create_dir_all(parent)?;
            }

            std::fs::rename(temp_filepath.clone(), dest_filepath.clone())?;

            filepath = None;
            current_read_bytes = 0;
            temp_filepath = format!("{}/{}", replicate_work_path.clone(), uuid::Uuid::new_v4());
            file = tokio::fs::File::create(temp_filepath.clone()).await?;
        }

        start = std::time::SystemTime::now();
    }
    drop(file);

    // cleanup leftovers
    if std::path::Path::new(&temp_filepath).exists() {
        std::fs::remove_file(temp_filepath.clone())?;
    }

    // gc after replication to clean up old segments
    tokio::task::spawn_blocking(move || shard.gc())
        .await?
        .expect("GC failed");

    Ok(())
}

pub async fn connect_to_primary_and_replicate(
    settings: Arc<Settings>,
    shard_cache: Arc<AsyncUnboundedShardWriterCache>,
    secondary_id: String,
) -> NodeResult<()> {
    let mut primary_address = settings.primary_address();
    if !primary_address.starts_with("http://") {
        primary_address = format!("http://{}", primary_address);
    }
    eprintln!("Connecting to primary: {:?}", primary_address);
    let mut client = replication::replication_service_client::ReplicationServiceClient::connect(
        primary_address.clone(),
    )
    .await?;
    // .max_decoding_message_size(256 * 1024 * 1024)
    // .max_encoding_message_size(256 * 1024 * 1024);address);

    let repl_health_mng = ReplicationHealthManager::new(Arc::clone(&settings));
    let metrics = metrics::get_metrics();

    loop {
        let existing_shards = list_shards(settings.shards_path()).await;
        let mut shard_states = Vec::new();
        for shard_id in existing_shards.clone() {
            let mut shard = shard_cache.get(shard_id.clone()).await;
            if shard.is_none() {
                let loaded = shard_cache.load(shard_id.clone()).await;
                if loaded.is_err() {
                    warn!("Failed to load shard: {:?}", loaded);
                    continue;
                }
                shard = Some(loaded?);
            }
            let shard = shard.unwrap();
            let gen_id = shard.get_generation_id();
            shard_states.push(replication::SecondaryShardReplicationState {
                shard_id: shard_id.clone(),
                generation_id: gen_id,
            });
        }
        info!("Sending shard states: {:?}", shard_states.clone());

        let replication_state: replication::PrimaryCheckReplicationStateResponse = client
            .check_replication_state(Request::new(
                replication::SecondaryCheckReplicationStateRequest {
                    secondary_id: secondary_id.clone(),
                    shard_states,
                },
            ))
            .await?
            .into_inner();

        metrics.record_replication_op(replication_metrics::ShardOpsKey {
            operation: "check_replication_state".to_string(),
        });
        info!("Got replication state: {:?}", replication_state.clone());

        for shard_state in replication_state.shard_states {
            let shard_id = shard_state.shard_id.clone();
            info!("Replicating shard: {:?}", shard_id);
            let shard_lookup;
            if existing_shards.contains(&shard_id) {
                shard_lookup = shard_cache.load(shard_id.clone()).await;
            } else {
                warn!("Creating shard to replicate: {shard_id}");
                let shard_create = shard_cache
                    .create(ShardMetadata {
                        id: Some(shard_state.shard_id.clone()),
                        kbid: Some(shard_state.kbid.clone()),
                        similarity: Some(shard_state.similarity.clone().into()),
                        channel: None,
                    })
                    .await;
                if shard_create.is_err() {
                    warn!("Failed to create shard: {:?}", shard_create);
                    continue;
                }
                shard_lookup = shard_create;
            }

            replicate_shard(shard_state, client.clone(), shard_lookup?).await?;
            metrics.record_replication_op(replication_metrics::ShardOpsKey {
                operation: "shard_replicated".to_string(),
            });
        }
        repl_health_mng.healthy();
        // sleep for some time
        tokio::time::sleep(std::time::Duration::from_secs(
            settings.replication_delay_seconds(),
        ))
        .await;
    }
}

pub async fn connect_to_primary_and_replicate_forever(
    settings: Arc<Settings>,
    shard_cache: Arc<AsyncUnboundedShardWriterCache>,
    secondary_id: String,
) -> NodeResult<()> {
    loop {
        let result = connect_to_primary_and_replicate(
            settings.clone(),
            shard_cache.clone(),
            secondary_id.clone(),
        )
        .await;
        if result.is_err() {
            error!(
                "Error happened during replication. Will retry: {:?}",
                result
            );
            tokio::time::sleep(std::time::Duration::from_secs(
                settings.replication_delay_seconds(),
            ))
            .await;
        } else {
            // normal exit
            return Ok(());
        }
    }
}
