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

use futures::Future;
use nucliadb_core::metrics::replication as replication_metrics;
use nucliadb_core::tracing::{debug, error, info, warn};
use nucliadb_core::{metrics, Error, NodeResult};
use nucliadb_protos::prelude::EmptyQuery;
use nucliadb_protos::replication;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::sync::Semaphore;
use tokio::time::Duration; // Import the Future trait
use tonic::Request;

use crate::cache::ShardWriterCache;
use crate::disk_structure;
use crate::replication::health::ReplicationHealthManager;
use crate::settings::Settings;
use crate::shards::writer::ShardWriter;
use crate::utils::{list_shards, set_primary_node_id};

pub enum ShardStub {
    Shard(Arc<ShardWriter>),
    New {
        id: String,
        path: PathBuf,
        cache: Arc<ShardWriterCache>,
    },
}

impl ShardStub {
    fn get_shard_segments(&self) -> NodeResult<HashMap<String, Vec<String>>> {
        if let ShardStub::Shard(s) = self {
            s.get_shard_segments()
        } else {
            Ok(HashMap::new())
        }
    }

    fn path(&self) -> &PathBuf {
        match self {
            ShardStub::Shard(shard) => &shard.path,
            ShardStub::New {
                path,
                ..
            } => path,
        }
    }

    fn get_open_shard(self) -> NodeResult<Arc<ShardWriter>> {
        match self {
            ShardStub::Shard(shard) => Ok(shard),
            ShardStub::New {
                id,
                cache,
                ..
            } => cache.get(&id),
        }
    }
}

pub async fn replicate_shard(
    shard_state: replication::PrimaryShardReplicationState,
    mut client: replication::replication_service_client::ReplicationServiceClient<tonic::transport::Channel>,
    shard: ShardStub,
) -> NodeResult<()> {
    let metrics = metrics::get_metrics();

    // do not allow gc while replicating
    let mut _gc_lock = None;
    if let ShardStub::Shard(ref s) = shard {
        _gc_lock = Some(s.gc_lock.lock().await);
    }

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

    let replicate_work_path = shard.path().join("replication");
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

            let dest_filepath = shard.path().join(filepath.clone().unwrap());
            // check if path exists
            if dest_filepath.exists() {
                std::fs::remove_file(dest_filepath.clone())?;
            }
            // mkdirs directory if not exists
            if let Some(parent) = dest_filepath.parent() {
                fs::create_dir_all(parent)?;
            }

            std::fs::rename(temp_filepath.clone(), dest_filepath.clone())?;
            debug!("Finished replicating file: {:?} to shard {:?}", resp.filepath, shard_state.shard_id);

            filepath = None;
            current_read_bytes = 0;
            temp_filepath = replicate_work_path.join(uuid::Uuid::new_v4().to_string());
            file = tokio::fs::File::create(temp_filepath.clone()).await?;
        }
    }
    drop(file);
    drop(_gc_lock);

    // Open the shard if replicating for the first time
    let shard = shard.get_open_shard()?;

    if let Some(gen_id) = generation_id {
        // After successful sync, set the generation id
        shard.metadata.set_generation_id(gen_id);
    } else {
        warn!("No generation id received for shard: {:?}", shard_state.shard_id);
    }

    // cleanup leftovers
    if std::path::Path::new(&temp_filepath).exists() {
        std::fs::remove_file(temp_filepath.clone())?;
    }

    debug!("Finished replicating shard: {:?}", shard_state.shard_id);

    // GC after replication to clean up old segments
    // We only do this here because GC will not be done otherwise on a secondary
    // XXX this should be removed once we refactor gc/merging
    shard.reload()?;
    let sshard = Arc::clone(&shard); // moved shard for gc
    tokio::task::spawn_blocking(move || sshard.collect_garbage()).await?.expect("GC failed");

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
    where
        F: Future<Output = NodeResult<()>> + Send + 'static,
    {
        let work_lock = Arc::clone(&self.work_lock);
        let permit = work_lock.acquire_owned().await.unwrap();

        tokio::spawn(async move {
            let result = worker.await;
            drop(permit);
            if let Err(e) = result {
                if let Some(status) = e.downcast_ref::<tonic::Status>() {
                    if status.code() == tonic::Code::NotFound {
                        warn!("Error replicating shard, not found in primary: {status:?}");
                        return;
                    }
                }

                error!("Error replicating shard: {e:?}");
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
    shard_cache: Arc<ShardWriterCache>,
    secondary_id: String,
    shutdown_notified: Arc<AtomicBool>,
) -> NodeResult<()> {
    let mut primary_address = settings.primary_address.clone();
    if !primary_address.starts_with("http://") {
        primary_address = format!("http://{}", primary_address);
    }
    eprintln!("Connecting to primary: {:?}", primary_address);
    let mut client =
        replication::replication_service_client::ReplicationServiceClient::connect(primary_address.clone())
            .await?
            .max_decoding_message_size(256 * 1024 * 1024);
    // .max_encoding_message_size(256 * 1024 * 1024);address);

    let repl_health_mng = ReplicationHealthManager::new(settings.clone());
    let metrics = metrics::get_metrics();

    let primary_node_metadata = client.get_metadata(Request::new(EmptyQuery {})).await?.into_inner();

    set_primary_node_id(&settings.data_path, primary_node_metadata.node_id)?;

    loop {
        if shutdown_notified.load(std::sync::atomic::Ordering::Relaxed) {
            return Ok(());
        }

        let existing_shards = list_shards(settings.shards_path()).await;
        let mut shard_states = Vec::new();
        let mut worker_pool = ReplicateWorkerPool::new(settings.replication_max_concurrency as usize);
        for shard_id in existing_shards.clone() {
            if let Some(metadata) = shard_cache.get_metadata(shard_id.clone()) {
                shard_states.push(replication::SecondaryShardReplicationState {
                    shard_id: shard_id.clone(),
                    generation_id: metadata.get_generation_id().unwrap_or("UNSET_SECONDARY".to_string()),
                });
            }
        }
        debug!("Sending shard states: {:?}", shard_states.clone());

        let replication_state: replication::PrimaryCheckReplicationStateResponse = client
            .check_replication_state(Request::new(replication::SecondaryCheckReplicationStateRequest {
                secondary_id: secondary_id.clone(),
                shard_states,
            }))
            .await?
            .into_inner();

        metrics.record_replication_op(replication_metrics::ReplicationOpsKey {
            operation: "check_replication_state".to_string(),
        });

        let no_shards_to_sync = replication_state.shard_states.is_empty();
        let no_shards_to_remove = replication_state.shards_to_remove.is_empty();

        let start = std::time::Instant::now();
        for shard_state in replication_state.shard_states {
            if shutdown_notified.load(std::sync::atomic::Ordering::Relaxed) {
                return Ok(());
            }

            let shard_id = shard_state.shard_id.clone();
            let shard_path = disk_structure::shard_path_by_id(&settings.shards_path(), &shard_id);
            let shard = if existing_shards.contains(&shard_id) {
                let shard_cache_clone = shard_cache.clone();
                let shard_id_clone = shard_id.clone();
                let shard_lookup = tokio::task::spawn_blocking(move || shard_cache_clone.get(&shard_id_clone)).await?;
                if let Ok(shard) = shard_lookup {
                    ShardStub::Shard(shard)
                } else {
                    // Shard exists but could not open, delete and recreate
                    warn!("Could not open replica shard {shard_id}, starting from scratch...");
                    std::fs::remove_dir_all(&shard_path)?;
                    std::fs::create_dir(&shard_path)?;
                    ShardStub::New {
                        id: shard_id.clone(),
                        path: shard_path.clone(),
                        cache: shard_cache.clone(),
                    }
                }
            } else {
                std::fs::create_dir(&shard_path)?;
                ShardStub::New {
                    id: shard_id.clone(),
                    path: shard_path.clone(),
                    cache: shard_cache.clone(),
                }
            };

            let mut current_gen_id = "UNKNOWN".to_string();
            if let Some(metadata) = shard_cache.get_metadata(shard_id.clone()) {
                current_gen_id = metadata.get_generation_id().unwrap_or("UNSET_SECONDARY".to_string());
            }

            info!(
                "Replicating shard: {:?}, Primary generation: {:?}, Current generation: {:?}",
                shard_id, shard_state.generation_id, current_gen_id
            );

            let replicate_work_path = shard_path.join("replication");
            if replicate_work_path.exists() {
                // clear out replication directory before we start in case there is anything
                // left behind from a former failed sync
                std::fs::remove_dir_all(replicate_work_path)?;
            }

            worker_pool.add(replicate_shard(shard_state, client.clone(), shard)).await?;
            metrics.record_replication_op(replication_metrics::ReplicationOpsKey {
                operation: "shard_replicated".to_string(),
            });
        }

        for shard_id in replication_state.shards_to_remove {
            info!("Removing shard: {:?}", shard_id);
            if !existing_shards.contains(&shard_id) {
                continue;
            }
            let shard_cache_clone = shard_cache.clone();
            let shard_lookup = tokio::task::spawn_blocking(move || shard_cache_clone.delete(&shard_id)).await?;
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
        let elapsed = start.elapsed();
        if elapsed < settings.replication_healthy_delay {
            // only update healthy marker if we're up-to-date in the configured healthy time
            repl_health_mng.update_healthy();
        }

        if no_shards_to_sync && no_shards_to_remove {
            // if we have any changes, check again immediately
            // otherwise, wait for a bit
            tokio::time::sleep(settings.replication_delay()).await;
        }
    }
}

pub async fn connect_to_primary_and_replicate_forever(
    settings: Settings,
    shard_cache: Arc<ShardWriterCache>,
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

        error!("Error happened during replication. Will retry: {:?}", result);
        tokio::time::sleep(settings.replication_delay()).await;
    }
}
