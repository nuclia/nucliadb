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
use std::time::Instant;

use nucliadb_core::metrics::middleware::MetricsLayer;
use nucliadb_core::protos::node_reader_server::NodeReaderServer;
use nucliadb_core::tracing::*;
use nucliadb_core::{env, NodeResult};
use nucliadb_node::http_server::{run_http_metrics_server, MetricsServerOptions};
use nucliadb_node::reader::grpc_driver::NodeReaderGRPCDriver;
use nucliadb_node::reader::NodeReaderService;
use nucliadb_node::telemetry::init_telemetry;
use tokio::signal::unix::SignalKind;
use tokio::signal::{ctrl_c, unix};
use tonic::transport::Server;

type GrpcServer = NodeReaderServer<NodeReaderGRPCDriver>;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("NucliaDB Reader Node starting...");
    let shards_path = env::shards_path();
    let tmp_workspace = env::tmp_path();
    if !shards_path.exists() {
        std::fs::create_dir_all(shards_path)?;
    }
    if !tmp_workspace.exists() {
        std::fs::create_dir_all(tmp_workspace)?;
    }

    let _guard = init_telemetry()?;
    let start_bootstrap = Instant::now();
    let mut node_reader_service = NodeReaderService::new();

    if !env::lazy_loading() {
        node_reader_service.load_shards()?;
    }

    let grpc_driver = NodeReaderGRPCDriver::from(node_reader_service);
    let _grpc_task = tokio::spawn(start_grpc_service(grpc_driver));
    let metrics_task = tokio::spawn(run_http_metrics_server(MetricsServerOptions {
        default_http_port: 3031,
    }));

    info!("Bootstrap complete in: {:?}", start_bootstrap.elapsed());
    eprintln!("Running");

    wait_for_sigkill().await?;
    info!("Shutting down NucliaDB Reader Node...");
    // wait some time to handle latest gRPC calls
    tokio::time::sleep(env::shutdown_delay()).await;
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

pub async fn start_grpc_service(grpc_driver: NodeReaderGRPCDriver) {
    let addr = env::reader_listen_address();

    info!("Reader listening for gRPC requests at: {:?}", addr);

    let metrics_middleware = MetricsLayer::default();

    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter.set_serving::<GrpcServer>().await;

    Server::builder()
        .layer(metrics_middleware)
        .add_service(health_service)
        .add_service(GrpcServer::new(grpc_driver))
        .serve(addr)
        .await
        .expect("Error starting gRPC reader");
}
