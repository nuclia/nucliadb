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
use nucliadb_node::settings::providers::env::EnvSettingsProvider;
use nucliadb_node::settings::providers::SettingsProvider;
use nucliadb_node::telemetry::init_telemetry;
use nucliadb_node::{lifecycle, utils};
use tokio::signal::unix::SignalKind;
use tokio::signal::{ctrl_c, unix};
use tokio::sync::mpsc::UnboundedReceiver;
use tonic::transport::Server;

type GrpcServer = NodeWriterServer<NodeWriterGRPCDriver>;

#[derive(Debug)]
pub enum NodeUpdate {
    ShardCount(u64),
}

#[tokio::main]
async fn main() -> NodeResult<()> {
    eprintln!("NucliaDB Writer Node starting...");
    let start_bootstrap = Instant::now();

    let settings = Arc::new(EnvSettingsProvider::generate_settings()?);

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
    let grpc_sender = metadata_sender.clone();
    let writer_settings = Arc::clone(&settings);
    let grpc_driver = NodeWriterGRPCDriver::new(writer_settings).with_sender(grpc_sender);
    grpc_driver.initialize().await?;

    let host_key_path = settings.host_key_path();
    let _ = utils::read_or_create_host_key(host_key_path)?;

    nucliadb_telemetry::sync::start_telemetry_loop();

    tokio::spawn(start_grpc_service(
        grpc_driver,
        settings.writer_listen_address(),
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
    eprintln!("Running");

    wait_for_sigkill().await?;

    info!("Shutting down NucliaDB Writer Node...");
    metrics_task.abort();
    let _ = metrics_task.await;

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

pub async fn start_grpc_service(grpc_driver: NodeWriterGRPCDriver, listen_address: SocketAddr) {
    info!("Listening for gRPC requests at: {:?}", listen_address);

    let tracing_middleware = GrpcInstrumentorLayer;
    let debug_logs_middleware = GrpcDebugLogsLayer;
    let metrics_middleware = GrpcTasksMetricsLayer;

    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter.set_serving::<GrpcServer>().await;

    Server::builder()
        .layer(tracing_middleware)
        .layer(debug_logs_middleware)
        .layer(metrics_middleware)
        .add_service(health_service)
        .add_service(GrpcServer::new(grpc_driver))
        .serve(listen_address)
        .await
        .expect("Error starting gRPC service");
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
