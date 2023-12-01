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

use nucliadb_core::protos::node_reader_server::NodeReaderServer;
use nucliadb_core::tracing::*;
use nucliadb_core::{node_error, NodeResult};
use nucliadb_node::grpc::middleware::{
    GrpcDebugLogsLayer, GrpcInstrumentorLayer, GrpcTasksMetricsLayer,
};
use nucliadb_node::grpc::reader::NodeReaderGRPCDriver;
use nucliadb_node::http_server::run_http_server;
use nucliadb_node::lifecycle;
use nucliadb_node::replication::health::ReplicationHealthManager;
use nucliadb_node::replication::NodeRole;
use nucliadb_node::settings::providers::env::EnvSettingsProvider;
use nucliadb_node::settings::providers::SettingsProvider;
use nucliadb_node::settings::Settings;
use nucliadb_node::telemetry::init_telemetry;
use tokio::signal::unix::SignalKind;
use tokio::signal::{ctrl_c, unix};
use tokio::sync::Notify;
use tonic::transport::Server;
use tonic_health::server::HealthReporter;

type GrpcServer = NodeReaderServer<NodeReaderGRPCDriver>;

#[tokio::main]
async fn main() -> NodeResult<()> {
    eprintln!("NucliaDB Reader Node starting...");
    let start_bootstrap = Instant::now();

    let settings: Settings = EnvSettingsProvider::generate_settings()?;

    if !settings.data_path().exists() {
        return Err(node_error!("Data directory missing"));
    }

    // XXX it probably should be moved to a more clear abstraction
    lifecycle::initialize_reader(settings.clone());

    let _guard = init_telemetry(&settings)?;
    nucliadb_node::analytics::sync::start_analytics_loop();

    let shutdown_notifier = Arc::new(Notify::new());

    let grpc_task = tokio::spawn(start_grpc_service(
        settings.clone(),
        Arc::clone(&shutdown_notifier),
    ));
    let metrics_task = tokio::spawn(run_http_server(settings.clone()));

    info!("Bootstrap complete in: {:?}", start_bootstrap.elapsed());
    eprintln!("Running");

    wait_for_sigkill(Arc::clone(&shutdown_notifier)).await?;
    info!("Shutting down NucliaDB Reader Node...");
    metrics_task.abort();
    grpc_task.await??;
    let _ = metrics_task.await;

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

async fn health_checker(mut health_reporter: HealthReporter, settings: Settings) -> NodeResult<()> {
    if settings.node_role() == NodeRole::Primary {
        // cut out early, this check is for secondaries only right now
        health_reporter.set_serving::<GrpcServer>().await;
        return Ok(());
    }

    let repl_health_mng = ReplicationHealthManager::new(settings.clone());
    loop {
        if repl_health_mng.healthy() {
            health_reporter.set_serving::<GrpcServer>().await;
        } else {
            health_reporter.set_not_serving::<GrpcServer>().await;
        }
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
}

pub async fn start_grpc_service(
    settings: Settings,
    shutdown_notifier: Arc<Notify>,
) -> NodeResult<()> {
    let listen_address = settings.reader_listen_address();
    eprintln!(
        "Reader listening for gRPC requests at: {:?}",
        listen_address
    );

    let tracing_middleware = GrpcInstrumentorLayer;
    let debug_logs_middleware = GrpcDebugLogsLayer;
    let metrics_middleware = GrpcTasksMetricsLayer;

    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    tokio::spawn(health_checker(health_reporter, settings.clone()));

    let grpc_driver = NodeReaderGRPCDriver::new(settings.clone());
    grpc_driver.initialize().await?;

    Server::builder()
        .layer(tracing_middleware)
        .layer(debug_logs_middleware)
        .layer(metrics_middleware)
        .add_service(health_service)
        .add_service(GrpcServer::new(grpc_driver))
        .serve_with_shutdown(listen_address, async {
            shutdown_notifier.notified().await;
        })
        .await
        .expect("Error starting gRPC reader");

    Ok(())
}
