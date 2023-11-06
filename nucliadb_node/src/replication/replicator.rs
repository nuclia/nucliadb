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
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use futures::Future;
use nucliadb_core::metrics::replication as replication_metrics;
use nucliadb_core::tracing::{debug, error, info, warn};
use nucliadb_core::{metrics, Error, NodeResult};
use nucliadb_protos::prelude::EmptyQuery;
use nucliadb_protos::replication;
use tokio::io::AsyncWriteExt;
use tokio::sync::Semaphore;
use tokio::time::Duration; // Import the Future trait
use tonic::Request;

use crate::replication::health::ReplicationHealthManager;
use crate::settings::Settings;
use crate::shards::metadata::ShardMetadata;
use crate::shards::providers::unbounded_cache::AsyncUnboundedShardWriterCache;
use crate::shards::providers::AsyncShardWriterProvider;
use crate::shards::writer::ShardWriter;
use crate::utils::{list_shards, set_primary_node_id};

pub async fn replicate_shard(
    shard_state: replication::PrimaryShardReplicationState,
    mut client: replication::replication_service_client::ReplicationServiceClient<
        tonic::transport::Channel,
    >,
    shard: Arc<ShardWriter>,
) -> NodeResult<()> {
    // do not allow gc while replicating
    let _gc_lock = shard.gc_lock.lock().await;

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
            chunk_size: 1024 * 1024 * 3,
            existing_segment_ids,
        }))
        .await?
        .into_inner();

    let shard_path = shard.path.clone();
    let replicate_work_path = shard_path.join("replication");
    // create replication work path if not exists
    if !replicate_work_path.exists() {
        std::fs::create_dir_all(&replicate_work_path)?;
    }
    let mut generation_id = None;
    let mut filepath = None;
    let mut temp_filepath = replicate_work_path.join(uuid::Uuid::new_v4().to_string());
    let mut current_read_bytes = 0;
    let mut file = tokio::fs::File::create(temp_filepath.clone()).await?;
    while let Some(resp) = stream.message().await? {
        generation_id = Some(resp.generation_id);
        if filepath.is_some() && Some(resp.filepath.clone()) != filepath {
            std::fs::remove_file(temp_filepath)?;
            return Err(Error::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "We should have finished previous file before starting a new one",
            )));
        }
        if filepath.is_none() {
            filepath = Some(resp.filepath.clone());
            debug!("Replicating file: {:?}", resp.filepath.clone());
        }

        file.write_all(&resp.data).await?;
        current_read_bytes += resp.data.len() as u64;

        metrics.record_replicated_bytes(resp.data.len() as u64);

        if current_read_bytes == resp.total_size {
            // finish copying file
            file.flush().await?;
            file.sync_all().await?;
            // close file
            drop(file);

            let dest_filepath = shard_path.join(filepath.clone().unwrap());
            // check if path exists
            if dest_filepath.exists() {
                std::fs::remove_file(dest_filepath.clone())?;
            }
            // mkdirs directory if not exists
            if let Some(parent) = dest_filepath.parent() {
                fs::create_dir_all(parent)?;
            }

            std::fs::rename(temp_filepath.clone(), dest_filepath.clone())?;
            debug!(
                "Finished replicating file: {:?} to shard {:?}",
                resp.filepath, shard_state.shard_id
            );

            filepath = None;
            current_read_bytes = 0;
            temp_filepath = replicate_work_path.join(uuid::Uuid::new_v4().to_string());
            file = tokio::fs::File::create(temp_filepath.clone()).await?;
        }
    }
    drop(file);
    drop(_gc_lock);

    if generation_id.is_some() {
        // After successful sync, set the generation id
        shard.set_generation_id(generation_id.unwrap());
    }

    // cleanup leftovers
    if std::path::Path::new(&temp_filepath).exists() {
        std::fs::remove_file(temp_filepath.clone())?;
    }

    debug!("Finished replicating shard: {:?}", shard_state.shard_id);

    // gc after replication to clean up old segments
    let sshard = Arc::clone(&shard); // moved shard for gc
    tokio::task::spawn_blocking(move || sshard.gc())
        .await?
        .expect("GC failed");

    Ok(())
}

struct ReplicateWorkerPool {
    work_lock: Arc<Semaphore>,
    max_size: usize,
}

impl ReplicateWorkerPool {
    pub fn new(max_size: usize) -> Self {
        Self {
            work_lock: Arc::new(Semaphore::new(max_size)),
            max_size,
        }
    }

    pub async fn add<F>(&mut self, worker: F) -> NodeResult<()>
    where F: Future<Output = NodeResult<()>> + Send + 'static {
        let work_lock = Arc::clone(&self.work_lock);
        let permit = work_lock.acquire_owned().await.unwrap();

        tokio::spawn(async move {
            let result = worker.await;
            drop(permit);
            if result.is_err() {
                error!("Error replicating shard: {:?}", result);
            }
        });
        Ok(())
    }

    pub async fn wait_for_workers(self) -> NodeResult<()> {
        self.work_lock.close();
        while self.work_lock.available_permits() != self.max_size {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok(())
    }
}

pub async fn connect_to_primary_and_replicate(
    settings: Settings,
    shard_cache: Arc<AsyncUnboundedShardWriterCache>,
    secondary_id: String,
    shutdown_notified: Arc<AtomicBool>,
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

    let repl_health_mng = ReplicationHealthManager::new(settings.clone());
    let metrics = metrics::get_metrics();

    let primary_node_metadata = client
        .get_metadata(Request::new(EmptyQuery {}))
        .await?
        .into_inner();

    set_primary_node_id(settings.data_path(), primary_node_metadata.node_id)?;

    loop {
        if shutdown_notified.load(std::sync::atomic::Ordering::Relaxed) {
            return Ok(());
        }

        let existing_shards = list_shards(settings.shards_path()).await;
        let mut shard_states = Vec::new();
        let mut worker_pool =
            ReplicateWorkerPool::new(settings.replication_max_concurrency() as usize);
        for shard_id in existing_shards.clone() {
            let mut shard = shard_cache.get(shard_id.clone()).await;
            if shard.is_none() {
                let loaded = shard_cache.load(shard_id.clone()).await;
                if loaded.is_err() {
                    warn!(
                        "Failed to load shard from disk, deleting shard in case of corruption and \
                         retrying: {:?}, Error: {:?}",
                        shard_id.clone(),
                        loaded
                    );
                    // delete directory
                    let shard_path = settings.shards_path().join(shard_id.clone());
                    if shard_path.exists() {
                        std::fs::remove_dir_all(shard_path)?;
                    }
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
        debug!("Sending shard states: {:?}", shard_states.clone());

        let replication_state: replication::PrimaryCheckReplicationStateResponse = client
            .check_replication_state(Request::new(
                replication::SecondaryCheckReplicationStateRequest {
                    secondary_id: secondary_id.clone(),
                    shard_states,
                },
            ))
            .await?
            .into_inner();

        metrics.record_replication_op(replication_metrics::ReplicationOpsKey {
            operation: "check_replication_state".to_string(),
        });

        let no_shards_to_sync = replication_state.shard_states.is_empty();
        let no_shards_to_remove = replication_state.shards_to_remove.is_empty();

        let start = std::time::SystemTime::now();
        for shard_state in replication_state.shard_states {
            if shutdown_notified.load(std::sync::atomic::Ordering::Relaxed) {
                return Ok(());
            }

            let shard_id = shard_state.shard_id.clone();
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
            let shard = shard_lookup?;

            info!(
                "Replicating shard: {:?}, Primary generation: {:?}, Current generation: {:?}",
                shard_id,
                shard_state.generation_id,
                shard.get_generation_id()
            );

            let replicate_work_path = shard.path.join("replication");
            if replicate_work_path.exists() {
                // clear out replication directory before we start in case there is anything
                // left behind from a former failed sync
                std::fs::remove_dir_all(replicate_work_path)?;
            }

            worker_pool
                .add(replicate_shard(shard_state, client.clone(), shard))
                .await?;
            metrics.record_replication_op(replication_metrics::ReplicationOpsKey {
                operation: "shard_replicated".to_string(),
            });
        }

        for shard_id in replication_state.shards_to_remove {
            info!("Removing shard: {:?}", shard_id);
            if !existing_shards.contains(&shard_id) {
                continue;
            }
            let shard_lookup = shard_cache.delete(shard_id.clone()).await;
            if shard_lookup.is_err() {
                warn!("Failed to delete shard: {:?}", shard_lookup);
                continue;
            }
            metrics.record_replication_op(replication_metrics::ReplicationOpsKey {
                operation: "shard_removed".to_string(),
            });
        }

        worker_pool.wait_for_workers().await?;

        // Healthy check and delays for manage replication.
        //
        // 1. If we're healthy, we'll sleep for a while and check again.
        // 2. If backed up replicating, we'll try replicating again immediately and check again.
        let elapsed = start
            .elapsed()
            .map(|elapsed| elapsed.as_secs_f64())
            .expect("Error getting elapsed time");
        if elapsed < settings.replication_healthy_delay() as f64 {
            // only update healthy marker if we're up-to-date in the configured healthy time
            repl_health_mng.update_healthy();
        }

        if no_shards_to_sync && no_shards_to_remove {
            // if we have any changes, check again immediately
            // otherwise, wait for a bit
            tokio::time::sleep(std::time::Duration::from_secs(
                settings.replication_delay_seconds(),
            ))
            .await;
        }
    }
}

pub async fn connect_to_primary_and_replicate_forever(
    settings: Settings,
    shard_cache: Arc<AsyncUnboundedShardWriterCache>,
    secondary_id: String,
    shutdown_notified: Arc<AtomicBool>,
) -> NodeResult<()> {
    loop {
        if shutdown_notified.load(std::sync::atomic::Ordering::Relaxed) {
            return Ok(());
        }
        let result = connect_to_primary_and_replicate(
            settings.clone(),
            Arc::clone(&shard_cache),
            secondary_id.clone(),
            Arc::clone(&shutdown_notified),
        )
        .await;

        if result.is_ok() {
            return Ok(());
        }

        error!(
            "Error happened during replication. Will retry: {:?}",
            result
        );
        tokio::time::sleep(std::time::Duration::from_secs(
            settings.replication_delay_seconds(),
        ))
        .await;
    }
}
