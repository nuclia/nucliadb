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

use std::sync::Arc;
use std::time::Instant;

use nucliadb_core::protos::node_writer_server::NodeWriterServer;
use nucliadb_core::tracing::*;
use nucliadb_core::{metrics, NodeResult};
use nucliadb_node::grpc::middleware::{
    GrpcDebugLogsLayer, GrpcInstrumentorLayer, GrpcTasksMetricsLayer,
};
use nucliadb_node::grpc::writer::{NodeWriterEvent, NodeWriterGRPCDriver};
use nucliadb_node::http_server::{run_http_server, ServerOptions};
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

#[derive(Debug)]
pub enum NodeUpdate {
    ShardCount(u64),
}

#[tokio::main]
async fn main() -> NodeResult<()> {
    eprintln!("NucliaDB Writer Node starting...");
    let start_bootstrap = Instant::now();

    let settings: Arc<Settings> = Arc::new(EnvSettingsProvider::generate_settings()?);

    let _guard = init_telemetry(&settings)?;
    let metrics = metrics::get_metrics();

    let data_path = settings.data_path();
    if !data_path.exists() {
        std::fs::create_dir(data_path.clone())?;
    }

    // XXX it probably should be moved to a more clear abstraction
    lifecycle::initialize_writer(&data_path, &settings.shards_path())?;
    let node_metadata = NodeMetadata::new().await?;
    let (metadata_sender, metadata_receiver) = tokio::sync::mpsc::unbounded_channel();

    let host_key_path = settings.host_key_path();
    let node_id = utils::read_or_create_host_key(host_key_path)?;

    nucliadb_telemetry::sync::start_telemetry_loop();

    let grpc_task = tokio::spawn(start_grpc_service(
        settings.clone(),
        metadata_sender.clone(),
        node_id.to_string(),
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
    let metrics_task = tokio::spawn(run_http_server(ServerOptions {
        default_http_port: 3032,
    }));

    info!("Bootstrap complete in: {:?}", start_bootstrap.elapsed());

    wait_for_sigkill().await?;

    info!("Shutting down NucliaDB Writer Node...");
    metrics_task.abort();
    grpc_task.abort();
    let _ = metrics_task.await;
    let _ = grpc_task.await;

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

pub async fn start_grpc_service(
    settings: Arc<Settings>,
    metadata_sender: UnboundedSender<NodeWriterEvent>,
    node_id: String,
) -> NodeResult<()> {
    let listen_address = settings.writer_listen_address();

    let tracing_middleware = GrpcInstrumentorLayer;
    let debug_logs_middleware = GrpcDebugLogsLayer;
    let metrics_middleware = GrpcTasksMetricsLayer;

    let shard_cache = Arc::new(AsyncUnboundedShardWriterCache::new(settings.shards_path()));

    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter.set_serving::<GrpcServer>().await;

    let mut server_builder = Server::builder()
        .layer(tracing_middleware)
        .layer(debug_logs_middleware)
        .layer(metrics_middleware)
        .add_service(health_service);

    if settings.node_role() == NodeRole::Secondary {
        // when it's a secondary server, do not even run the writer GRPC service
        // because nothing should happen through that interface
        tokio::spawn(connect_to_primary_and_replicate_forever(
            settings.clone(),
            shard_cache.clone(),
            node_id,
        ));
    } else {
        let grpc_driver = NodeWriterGRPCDriver::new(Arc::clone(&settings), shard_cache.clone())
            .with_sender(metadata_sender);
        grpc_driver.initialize().await?;
        let replication_server =
            replication::replication_service_server::ReplicationServiceServer::new(
                ReplicationServiceGRPCDriver::new(settings.clone(), shard_cache.clone()),
            );
        server_builder = server_builder
            .add_service(GrpcServer::new(grpc_driver))
            .add_service(replication_server);
    }

    eprintln!("Listening for gRPC requests at: {:?}", listen_address);
    server_builder
        .serve(listen_address)
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
