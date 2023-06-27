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
use nucliadb_core::{node_error, NodeResult};
use nucliadb_node::env;
use nucliadb_node::http_server::{run_http_metrics_server, MetricsServerOptions};
use nucliadb_node::middleware::{GrpcDebugLogsLayer, GrpcInstrumentorLayer};
use nucliadb_node::reader::grpc_driver::{GrpcReaderOptions, NodeReaderGRPCDriver};
use nucliadb_node::shards::{AsyncReaderShardsProvider, AsyncUnboundedShardReaderCache};
use nucliadb_node::telemetry::init_telemetry;
use tokio::signal::unix::SignalKind;
use tokio::signal::{ctrl_c, unix};
use tonic::transport::Server;

type GrpcServer = NodeReaderServer<NodeReaderGRPCDriver>;

#[tokio::main]
async fn main() -> NodeResult<()> {
    eprintln!("NucliaDB Reader Node starting...");

    if !env::data_path().exists() {
        return Err(node_error!("Data directory missing"));
    }

    let _guard = init_telemetry()?;
    let start_bootstrap = Instant::now();
    let shards_provider = AsyncUnboundedShardReaderCache::new();

    if !env::lazy_loading() {
        shards_provider.load_all().await?;
    }

    let grpc_options = GrpcReaderOptions {
        lazy_loading: env::lazy_loading(),
    };
    let grpc_driver = NodeReaderGRPCDriver::new(grpc_options);
    grpc_driver.initialize().await?;

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
        .expect("Error starting gRPC reader");
}
