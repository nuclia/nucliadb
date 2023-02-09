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

use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use std::str::FromStr;
use std::time::Instant;

use anyhow::{Context, Result};
use futures::Stream;
use nucliadb_cluster::{node, Key, Node, NodeHandle, NodeType};
use nucliadb_core::protos::node_writer_server::NodeWriterServer;
use nucliadb_core::protos::GetShardRequest;
use nucliadb_core::tracing::*;
use nucliadb_node::env;
use nucliadb_node::reader::NodeReaderService;
use nucliadb_node::telemetry::init_telemetry;
use nucliadb_node::writer::grpc_driver::{
    NodeWriterEvent, NodeWriterGRPCDriver, NodeWriterMetadata,
};
use nucliadb_node::writer::NodeWriterService;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio_stream::StreamExt;
use tonic::transport::Server;
use uuid::Uuid;

const LOAD_SCORE_KEY: Key<f32> = Key::new("load_score");
const SHARD_COUNT_KEY: Key<u64> = Key::new("shard_count");

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("NucliaDB Writer Node starting...");
    let _guard = init_telemetry()?;

    let start_bootstrap = Instant::now();

    let node_metadata = get_node_metadata();
    let mut node_writer_service = NodeWriterService::new();

    std::fs::create_dir_all(env::shards_path())?;
    if !env::lazy_loading() {
        node_writer_service.load_shards()?;
    }

    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    let grpc_driver = NodeWriterGRPCDriver::from(node_writer_service).with_sender(sender);
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
        .insert_to_initial_state(LOAD_SCORE_KEY, node_metadata.paragraph_count as f32)
        .insert_to_initial_state(SHARD_COUNT_KEY, node_metadata.shard_count)
        .build()?;

    let node = node.start().await?;

    let writer_task = tokio::spawn(start_grpc_service(grpc_driver));

    let telemetry_handle = nucliadb_telemetry::sync::start_telemetry_loop();

    let cluster_watcher = node.cluster_watcher().await;
    let monitor_task = tokio::task::spawn(monitor_cluster(cluster_watcher));

    let update_task = tokio::spawn(watch_node_update(node, receiver, node_metadata));

    info!("Bootstrap complete in: {:?}", start_bootstrap.elapsed());
    eprintln!("Running");

    tokio::try_join!(writer_task, monitor_task, update_task)?;
    telemetry_handle.terminate_telemetry().await;
    // node_writer_service.shutdown().await;

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

pub async fn watch_node_update(
    node: NodeHandle,
    mut receiver: UnboundedReceiver<NodeWriterEvent>,
    mut metadata: NodeWriterMetadata,
) {
    info!("Start node update task");

    while let Some(event) = receiver.recv().await {
        match event {
            NodeWriterEvent::ShardCreation => {
                metadata.shard_count = metadata.shard_count.saturating_add(1);
                info!("Update node state: shard_count = {}", metadata.shard_count);
                node.update_state(SHARD_COUNT_KEY, metadata.shard_count)
                    .await;
            }
            NodeWriterEvent::ShardDeletion => {
                metadata.shard_count = metadata.shard_count.saturating_sub(1);
                info!("Update node state: shard_count = {}", metadata.shard_count);
                node.update_state(SHARD_COUNT_KEY, metadata.shard_count)
                    .await;
            }
            NodeWriterEvent::ParagraphCount(paragraph_count) => {
                metadata.paragraph_count = paragraph_count;
                info!("Update node state: load_score = {paragraph_count}");
                node.update_state(LOAD_SCORE_KEY, paragraph_count as f32)
                    .await;
            }
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

pub fn get_node_metadata() -> NodeWriterMetadata {
    let mut node_reader = NodeReaderService::new();

    let shards = match node_reader.iter_shards() {
        Ok(shards) => shards,
        Err(e) => {
            error!("Cannot read shards folder: {e}");

            return NodeWriterMetadata::default();
        }
    };

    let mut node_metadata = NodeWriterMetadata::default();

    for shard in shards {
        let shard = match shard {
            Ok(shard) => shard,
            Err(e) => {
                error!("Cannot load shard: {e}");
                continue;
            }
        };

        match shard.get_info(&GetShardRequest::default()) {
            Ok(count) => {
                node_metadata.shard_count += 1;
                node_metadata.paragraph_count += count.paragraphs as u64;
            }
            Err(e) => error!("Cannot get metrics for {}: {e:?}", shard.id),
        }
    }

    node_metadata
}
