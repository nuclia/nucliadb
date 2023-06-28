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
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Instant;

use anyhow::{Context, Result};
use futures::Stream;
use nucliadb_cluster::{node, Key, Node, NodeHandle, NodeType};
use nucliadb_core::metrics::middleware::MetricsLayer;
use nucliadb_core::protos::node_writer_server::NodeWriterServer;
use nucliadb_core::tracing::*;
use nucliadb_core::{metrics, NodeResult};
use nucliadb_node::env;
use nucliadb_node::http_server::{run_http_metrics_server, MetricsServerOptions};
use nucliadb_node::middleware::{GrpcDebugLogsLayer, GrpcInstrumentorLayer};
use nucliadb_node::node_metadata::NodeMetadata;
use nucliadb_node::telemetry::init_telemetry;
use nucliadb_node::writer::grpc_driver::{NodeWriterEvent, NodeWriterGRPCDriver};
use nucliadb_node::writer::NodeWriterService;
use tokio::signal::unix::SignalKind;
use tokio::signal::{ctrl_c, unix};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_stream::StreamExt;
use tonic::transport::Server;
use uuid::Uuid;

const SHARD_COUNT_KEY: Key<u64> = Key::new("shard_count");

type GrpcServer = NodeWriterServer<NodeWriterGRPCDriver>;

#[derive(Debug)]
pub enum NodeUpdate {
    ShardCount(u64),
}

#[tokio::main]
async fn main() -> NodeResult<()> {
    eprintln!("NucliaDB Writer Node starting...");
    let start_bootstrap = Instant::now();
    let _guard = init_telemetry()?;
    let metrics = metrics::get_metrics();
    let data_path = env::data_path();
    let metadata_path = env::metadata_path();

    if !data_path.exists() {
        std::fs::create_dir(data_path)?;
    }

    let mut node_writer_service = NodeWriterService::new()?;
    let node_metadata = NodeMetadata::load_or_create(&metadata_path)?;

    if !env::lazy_loading() {
        node_writer_service.load_shards()?;
    }

    let (metadata_sender, metadata_receiver) = tokio::sync::mpsc::unbounded_channel();
    let (update_sender, update_receiver) = tokio::sync::mpsc::unbounded_channel();
    let grpc_sender = metadata_sender.clone();
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
        .insert_to_initial_state(SHARD_COUNT_KEY, node_metadata.shard_count())
        .build()?;
    let node = node.start().await?;
    let cluster_watcher = node.cluster_watcher().await;
    let update_handle = node.clone();

    nucliadb_telemetry::sync::start_telemetry_loop();

    tokio::spawn(start_grpc_service(grpc_driver));
    {
        let task = update_node_metadata(
            update_sender,
            metadata_receiver,
            node_metadata,
            metadata_path,
        );
        match metrics.task_monitor("UpdateNodeMetadata".to_string()) {
            Some(monitor) => {
                let instrumented = monitor.instrument(task);
                tokio::spawn(instrumented)
            }
            None => tokio::spawn(task),
        };
    }
    let update_task = tokio::spawn(update_node_state(update_handle, update_receiver));
    let monitor_task = {
        let task = monitor_cluster(cluster_watcher);
        match metrics.task_monitor("ClusterMonitor".to_string()) {
            Some(monitor) => {
                let instrumented = monitor.instrument(task);
                tokio::spawn(instrumented)
            }
            None => tokio::spawn(task),
        }
    };
    let metrics_task = tokio::spawn(run_http_metrics_server(MetricsServerOptions {
        default_http_port: 3032,
    }));

    info!("Bootstrap complete in: {:?}", start_bootstrap.elapsed());
    eprintln!("Running");

    wait_for_sigkill().await?;

    info!("Shutting down NucliaDB Writer Node...");
    // abort all the tasks that hold a chitchat TCP/IP connection
    monitor_task.abort();
    update_task.abort();
    metrics_task.abort();
    let _ = monitor_task.await;
    let _ = update_task.await;
    let _ = metrics_task.await;
    // then close the chitchat TCP/IP connection
    node.shutdown().await?;
    // wait some time to handle latest gRPC calls
    tokio::time::sleep(env::shutdown_delay()).await;

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

    let tracing_middleware = GrpcInstrumentorLayer::default();
    let debug_logs_middleware = GrpcDebugLogsLayer::default();
    let metrics_middleware = MetricsLayer::default();

    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter.set_serving::<GrpcServer>().await;

    Server::builder()
        .layer(tracing_middleware)
        .layer(debug_logs_middleware)
        .layer(metrics_middleware)
        .add_service(health_service)
        .add_service(GrpcServer::new(grpc_driver))
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
            } else {
                error!("Cluster snapshot cannot be serialized");
            }
        }
    }
}

async fn update_node_state(node: NodeHandle, mut metadata_receiver: UnboundedReceiver<NodeUpdate>) {
    info!("Start node update task");

    while let Some(event) = metadata_receiver.recv().await {
        info!("Receive node update event: {event:?}");

        match event {
            NodeUpdate::ShardCount(count) => node.update_state(SHARD_COUNT_KEY, count).await,
        }
    }
}

pub async fn update_node_metadata(
    update_sender: UnboundedSender<NodeUpdate>,
    mut metadata_receiver: UnboundedReceiver<NodeWriterEvent>,
    mut node_metadata: NodeMetadata,
    path: PathBuf,
) {
    info!("Start node update task");

    while let Some(event) = metadata_receiver.recv().await {
        debug!("Receive metadata update event: {event:?}");

        let result = match event {
            NodeWriterEvent::ShardCreation => {
                node_metadata.new_shard();
                update_sender.send(NodeUpdate::ShardCount(node_metadata.shard_count()))
            }
            NodeWriterEvent::ShardDeletion => {
                node_metadata.delete_shard();
                update_sender.send(NodeUpdate::ShardCount(node_metadata.shard_count()))
            }
        };

        if let Err(e) = result {
            warn!("Cannot send node update: {e:?}");
        }

        if let Err(e) = node_metadata.save(&path) {
            error!("Node metadata update failed: {e}");
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
                std::fs::create_dir(dir).with_context(|| {
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
