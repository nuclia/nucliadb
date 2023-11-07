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

use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Instant;

use nucliadb_core::protos::node_writer_server::NodeWriterServer;
use nucliadb_core::tracing::*;
use nucliadb_core::{metrics, NodeResult};
use nucliadb_node::grpc::middleware::{
    GrpcDebugLogsLayer, GrpcInstrumentorLayer, GrpcTasksMetricsLayer,
};
use nucliadb_node::grpc::writer::{NodeWriterEvent, NodeWriterGRPCDriver};
use nucliadb_node::http_server::run_http_server;
use nucliadb_node::node_metadata::NodeMetadata;
use nucliadb_node::replication::replicator::connect_to_primary_and_replicate_forever;
use nucliadb_node::replication::service::ReplicationServiceGRPCDriver;
use nucliadb_node::replication::NodeRole;
use nucliadb_node::settings::providers::env::EnvSettingsProvider;
use nucliadb_node::settings::providers::SettingsProvider;
use nucliadb_node::settings::Settings;
use nucliadb_node::telemetry::init_telemetry;
use nucliadb_node::{lifecycle, utils};
use nucliadb_protos::replication;
use tokio::signal::unix::SignalKind;
use tokio::signal::{ctrl_c, unix};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tonic::transport::Server;
type GrpcServer = NodeWriterServer<NodeWriterGRPCDriver>;
use nucliadb_node::shards::providers::unbounded_cache::AsyncUnboundedShardWriterCache;
use tokio::sync::Notify;

#[derive(Debug)]
pub enum NodeUpdate {
    ShardCount(u64),
}

fn get_shutdown_notifier() -> (Arc<Notify>, Arc<AtomicBool>) {
    // Returns a tuple with a shutdown notifier and a shutdown notified.
    // This is useful when you want to notify a shutdown and wait for it to be notified
    // but also need to be able to check if server is shutting down
    //
    let shutdown_notifier = Arc::new(Notify::new());
    let shutdown_notified = Arc::new(AtomicBool::new(false));

    let shutdown_notifier_clone = Arc::clone(&shutdown_notifier);
    let shutdown_notified_clone = Arc::clone(&shutdown_notified);

    tokio::spawn(async move {
        shutdown_notifier_clone.notified().await;
        shutdown_notified_clone.store(true, std::sync::atomic::Ordering::Relaxed);
    });

    (shutdown_notifier, shutdown_notified)
}

#[tokio::main]
async fn main() -> NodeResult<()> {
    eprintln!("NucliaDB Writer Node starting...");
    let start_bootstrap = Instant::now();

    let settings: Settings = EnvSettingsProvider::generate_settings()?;

    let _guard = init_telemetry(&settings)?;
    let metrics = metrics::get_metrics();

    let data_path = settings.data_path();
    if !data_path.exists() {
        std::fs::create_dir(data_path.clone())?;
    }

    // XXX it probably should be moved to a more clear abstraction
    lifecycle::initialize_writer(settings.clone())?;
    let node_metadata = NodeMetadata::new(settings.clone()).await?;
    let (metadata_sender, metadata_receiver) = tokio::sync::mpsc::unbounded_channel();

    let host_key_path = settings.host_key_path();
    let node_id = utils::read_or_create_host_key(host_key_path)?;

    nucliadb_node::analytics::sync::start_analytics_loop();

    let (shutdown_notifier, shutdown_notified) = get_shutdown_notifier();
    let shard_cache = Arc::new(AsyncUnboundedShardWriterCache::new(settings.clone()));

    let mut replication_task = None;
    if settings.node_role() == NodeRole::Secondary {
        // when it's a secondary server, do not even run the writer GRPC service
        // because nothing should happen through that interface
        replication_task = Some(tokio::spawn(connect_to_primary_and_replicate_forever(
            settings.clone(),
            Arc::clone(&shard_cache),
            node_id.to_string(),
            Arc::clone(&shutdown_notified),
        )));
    }

    let grpc_task = tokio::spawn(start_grpc_service(
        settings.clone(),
        Arc::clone(&shard_cache),
        metadata_sender.clone(),
        node_id.to_string(),
        Arc::clone(&shutdown_notifier),
    ));

    {
        let task = update_node_metadata(metadata_receiver, node_metadata);
        match metrics.task_monitor("UpdateNodeMetadata".to_string()) {
            Some(monitor) => {
                let instrumented = monitor.instrument(task);
                tokio::spawn(instrumented)
            }
            None => tokio::spawn(task),
        };
    }
    let metrics_task = tokio::spawn(run_http_server(settings.clone()));

    info!("Bootstrap complete in: {:?}", start_bootstrap.elapsed());

    wait_for_sigkill(Arc::clone(&shutdown_notifier)).await?;

    info!("Shutting down NucliaDB Writer Node...");
    metrics_task.abort();
    grpc_task.await??; // wait for shutdown of server instead of aborting it
    let _ = metrics_task.await;

    if let Some(repl_task) = replication_task {
        repl_task.await??;
    }

    Ok(())
}

async fn wait_for_sigkill(shutdown_notifier: Arc<Notify>) -> NodeResult<()> {
    let mut sigterm = unix::signal(SignalKind::terminate())?;
    let mut sigquit = unix::signal(SignalKind::quit())?;

    tokio::select! {
        _ = sigterm.recv() => println!("Terminating on SIGTERM"),
        _ = sigquit.recv() => println!("Terminating on SIGQUIT"),
        _ = ctrl_c() => println!("Terminating on ctrl-c"),
    }

    shutdown_notifier.notify_waiters();

    Ok(())
}

pub async fn start_grpc_service(
    settings: Settings,
    shard_cache: Arc<AsyncUnboundedShardWriterCache>,
    metadata_sender: UnboundedSender<NodeWriterEvent>,
    node_id: String,
    shutdown_notifier: Arc<Notify>,
) -> NodeResult<()> {
    let listen_address = settings.writer_listen_address();

    let tracing_middleware = GrpcInstrumentorLayer;
    let debug_logs_middleware = GrpcDebugLogsLayer;
    let metrics_middleware = GrpcTasksMetricsLayer;

    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter.set_serving::<GrpcServer>().await;

    let mut server_builder = Server::builder()
        .layer(tracing_middleware)
        .layer(debug_logs_middleware)
        .layer(metrics_middleware)
        .add_service(health_service);

    if settings.node_role() == NodeRole::Primary {
        let grpc_driver = NodeWriterGRPCDriver::new(settings.clone(), shard_cache.clone())
            .with_sender(metadata_sender);
        grpc_driver.initialize().await?;
        server_builder = server_builder.add_service(GrpcServer::new(grpc_driver));
    }
    let replication_server = replication::replication_service_server::ReplicationServiceServer::new(
        ReplicationServiceGRPCDriver::new(settings.clone(), shard_cache.clone(), node_id),
    );
    server_builder = server_builder.add_service(replication_server);

    eprintln!("Listening for gRPC requests at: {:?}", listen_address);
    server_builder
        .serve_with_shutdown(listen_address, async {
            shutdown_notifier.notified().await;
        })
        .await
        .expect("Error starting gRPC service");

    Ok(())
}

pub async fn update_node_metadata(
    mut metadata_receiver: UnboundedReceiver<NodeWriterEvent>,
    mut node_metadata: NodeMetadata,
) {
    info!("Start node update task");

    while let Some(event) = metadata_receiver.recv().await {
        debug!("Receive metadata update event: {event:?}");

        match event {
            NodeWriterEvent::ShardCreation => node_metadata.new_shard(),
            NodeWriterEvent::ShardDeletion => node_metadata.delete_shard(),
        };
    }

    info!("Node update task stopped");
}
