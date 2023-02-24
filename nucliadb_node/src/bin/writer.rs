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

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Instant;
use std::{fs, io};

use anyhow::{anyhow, Context, Result};
use futures::Stream;
use nucliadb_cluster::{node, Key, Node, NodeHandle, NodeType};
use nucliadb_core::protos::node_writer_server::NodeWriterServer;
use nucliadb_core::protos::GetShardRequest;
use nucliadb_core::tracing::*;
use nucliadb_core::NodeResult;
use nucliadb_node::env;
use nucliadb_node::reader::NodeReaderService;
use nucliadb_node::telemetry::init_telemetry;
use nucliadb_node::writer::grpc_driver::{
    NodeWriterEvent, NodeWriterGRPCDriver, NodeWriterMetadata,
};
use nucliadb_node::writer::NodeWriterService;
use tokio::signal::unix::SignalKind;
use tokio::signal::{ctrl_c, unix};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio_stream::StreamExt;
use tonic::transport::Server;
use uuid::Uuid;

const LOAD_SCORE_KEY: Key<f32> = Key::new("load_score");
const SHARD_COUNT_KEY: Key<u64> = Key::new("shard_count");

#[tokio::main]
async fn main() -> NodeResult<()> {
    eprintln!("NucliaDB Writer Node starting...");
    let _guard = init_telemetry()?;

    let start_bootstrap = Instant::now();

    let metadata_path = env::metadata_path();
    let node_metadata = load_node_metadata(&metadata_path).await?;
    let mut node_writer_service = NodeWriterService::new();

    std::fs::create_dir_all(env::shards_path())?;
    if !env::lazy_loading() {
        node_writer_service.load_shards()?;
    }

    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    let grpc_sender = sender.clone();
    let grpc_driver = NodeWriterGRPCDriver::from(node_writer_service).with_sender(grpc_sender);
    let host_key_path = env::host_key_path();
    let public_ip = env::public_ip().await;
    let chitchat_port = env::chitchat_port();
    let seed_nodes = env::seed_nodes();

    let chitchat_addr = SocketAddr::from_str(&format!("{}:{}", public_ip, chitchat_port))?;

    // Cluster
    let host_key = read_or_create_host_key(Path::new(&host_key_path))?;
    let node = Node::builder()
        .register_as(NodeType::Io)
        .on_local_network(chitchat_addr)
        .with_id(host_key.to_string())
        .with_seed_nodes(seed_nodes)
        .insert_to_initial_state(LOAD_SCORE_KEY, node_metadata.load_score())
        .insert_to_initial_state(SHARD_COUNT_KEY, node_metadata.shard_count())
        .build()?;
    let node = node.start().await?;
    let cluster_watcher = node.cluster_watcher().await;
    let update_handle = node.clone();

    nucliadb_telemetry::sync::start_telemetry_loop();
    tokio::spawn(start_grpc_service(grpc_driver));
    tokio::spawn(monitor_cluster(cluster_watcher));
    tokio::spawn(watch_node_update(
        update_handle,
        receiver,
        node_metadata,
        metadata_path,
    ));

    info!("Bootstrap complete in: {:?}", start_bootstrap.elapsed());
    eprintln!("Running");

    wait_for_sigkill().await?;
    node.shutdown().await?;
    Ok(())
}

async fn wait_for_sigkill() -> NodeResult<()> {
    let mut sigterm = unix::signal(SignalKind::terminate())?;
    let mut sigquit = unix::signal(SignalKind::quit())?;
    tokio::select! {
        _ = sigterm.recv() => println!("Terminating on SIGTERM"),
        _ = sigquit.recv() => println!("Terminating on SIGQUIT"),
        _ = ctrl_c() => println!("Terminating on ctrl-c"),
    }
    Ok(())
}
pub async fn start_grpc_service(grpc_driver: NodeWriterGRPCDriver) {
    let addr = env::writer_listen_address();

    info!("Listening for gRPC requests at: {:?}", addr);

    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<NodeWriterServer<NodeWriterGRPCDriver>>()
        .await;

    Server::builder()
        .add_service(health_service)
        .add_service(NodeWriterServer::new(grpc_driver))
        .serve(addr)
        .await
        .expect("Error starting gRPC service");
}

pub async fn monitor_cluster(cluster_watcher: impl Stream<Item = Vec<NodeHandle>>) {
    info!("Start cluster monitoring");

    let mut cluster_watcher = Box::pin(cluster_watcher);

    loop {
        debug!("Wait for cluster update");
        if let Some(live_nodes) = cluster_watcher.next().await {
            let cluster_snapshot = node::cluster_snapshot(live_nodes).await;

            if let Ok(snapshot) = serde_json::to_string(&cluster_snapshot) {
                info!("Cluster update: {snapshot}");
            };
        } else {
            error!("Cluster snapshot cannot be serialized");
        }
    }
}

async fn update_node_state<T: std::fmt::Display>(node: &NodeHandle, key: Key<T>, value: T) {
    info!("Update node state: {key} = {value}");
    node.update_state(key, value).await;
}

pub async fn watch_node_update(
    node: NodeHandle,
    mut receiver: UnboundedReceiver<NodeWriterEvent>,
    mut metadata: NodeWriterMetadata,
    path: PathBuf,
) {
    info!("Start node update task");

    while let Some(event) = receiver.recv().await {
        debug!("Receive node update event: {event:?}");

        match event {
            NodeWriterEvent::ShardCreation(id, knowledge_box) => {
                metadata.new_shard(id, knowledge_box, 0.0);
                update_node_state(&node, SHARD_COUNT_KEY, metadata.shard_count()).await;
            }
            NodeWriterEvent::ShardDeletion(id) => {
                metadata.delete_shard(id);
                update_node_state(&node, LOAD_SCORE_KEY, metadata.load_score()).await;
                update_node_state(&node, SHARD_COUNT_KEY, metadata.shard_count()).await;
            }
            NodeWriterEvent::ParagraphCount(id, paragraph_count) => {
                metadata.update_shard(id, paragraph_count);
                update_node_state(&node, LOAD_SCORE_KEY, metadata.load_score()).await;
            }
        }

        if let Err(e) = persist_node_metadata(&path, &metadata).await {
            error!("{e}");
        } else {
            info!("Node metadata file updated successfully");
        }
    }

    info!("Node update task stopped");
}

pub fn read_host_key(host_key_path: &Path) -> Result<Uuid> {
    let host_key_contents = fs::read(host_key_path)
        .with_context(|| format!("Failed to read host key from '{}'", host_key_path.display()))?;

    let host_key = Uuid::from_slice(host_key_contents.as_slice())
        .with_context(|| format!("Invalid host key from '{}'", host_key_path.display()))?;

    Ok(host_key)
}

/// Reads the key that makes a node unique from the given file.
/// If the file does not exist, it generates an ID and writes it to the file
/// so that it can be reused on reboot.
pub fn read_or_create_host_key(host_key_path: &Path) -> Result<Uuid> {
    let host_key;

    if host_key_path.exists() {
        host_key = read_host_key(host_key_path)?;
        info!(host_key=?host_key, host_key_path=?host_key_path, "Read existing host key.");
    } else {
        if let Some(dir) = host_key_path.parent() {
            if !dir.exists() {
                fs::create_dir_all(dir).with_context(|| {
                    format!("Failed to create host key directory '{}'", dir.display())
                })?;
            }
        }
        host_key = Uuid::new_v4();
        fs::write(host_key_path, host_key.as_bytes()).with_context(|| {
            format!("Failed to write host key to '{}'", host_key_path.display())
        })?;
        info!(host_key=?host_key, host_key_path=?host_key_path, "Create new host key.");
    }

    Ok(host_key)
}

pub async fn load_node_metadata(path: &Path) -> Result<NodeWriterMetadata> {
    info!("Loading node metadata file '{}'", path.display());

    match tokio::fs::read_to_string(path).await {
        Ok(metadata) => serde_json::from_str(&metadata)
            .map_err(|e| anyhow!("Cannot deserialize node metadata: {e}")),
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            info!("Node metadata file is missing. Starting creation...");

            let mut node_reader = NodeReaderService::new();

            let shards = node_reader
                .iter_shards()
                .map_err(|e| anyhow!("Cannot read shards folder: {e}"))?;

            let mut metadata = NodeWriterMetadata::default();

            for shard in shards {
                let shard = shard.map_err(|e| anyhow!("Cannot load shard: {e}"))?;

                match shard.get_info(&GetShardRequest::default()) {
                    Ok(count) if count.metadata.is_some() => metadata.new_shard(
                        shard.id,
                        count.metadata.unwrap().kbid,
                        count.paragraphs as f32,
                    ),
                    Ok(_) => return Err(anyhow!("Missing shard metadata for {}", shard.id)),
                    Err(e) => return Err(anyhow!("Cannot get metrics for {}: {e:?}", shard.id)),
                }
            }

            persist_node_metadata(path, &metadata).await?;

            info!("Node metadata file created successfully");

            Ok(metadata)
        }
        Err(e) => panic!("Cannot load node metadata file: {e}"),
    }
}

pub async fn persist_node_metadata(path: &Path, metadata: &NodeWriterMetadata) -> Result<()> {
    match serde_json::to_string(&metadata) {
        Ok(metadata) => tokio::fs::write(&path, metadata)
            .await
            .map_err(|e| anyhow!("Cannot update node metadata file: {e}")),
        Err(e) => Err(anyhow!("Cannot serialize node metadata: {e}")),
    }
}
