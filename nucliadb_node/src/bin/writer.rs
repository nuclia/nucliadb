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
use std::net::SocketAddr;
use std::path::Path;
use std::str::FromStr;
use std::time::Instant;

use nucliadb_cluster::cluster::{read_or_create_host_key, Cluster, NodeType};
use nucliadb_node::config::Configuration;
use nucliadb_node::metrics;
use nucliadb_node::metrics::report::NodeReport;
use nucliadb_node::reader::NodeReaderService;
use nucliadb_node::telemetry::init_telemetry;
use nucliadb_node::writer::grpc_driver::NodeWriterGRPCDriver;
use nucliadb_node::writer::NodeWriterService;
use nucliadb_protos::node_writer_server::NodeWriterServer;
use tokio_stream::wrappers::WatchStream;
use tokio_stream::StreamExt;
use tonic::transport::Server;
use tracing::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("NucliaDB Writer Node starting...");
    let _guard = init_telemetry()?;

    let start_bootstrap = Instant::now();

    let mut node_writer_service = NodeWriterService::new();

    std::fs::create_dir_all(Configuration::shards_path())?;
    if !Configuration::lazy_loading() {
        node_writer_service.load_shards()?;
    }

    let grpc_driver = NodeWriterGRPCDriver::from(node_writer_service);
    let host_key_path = Configuration::host_key_path();
    let public_ip = Configuration::public_ip().await;
    let chitchat_port = Configuration::chitchat_port();
    let seed_nodes = Configuration::seed_nodes();

    let chitchat_addr = SocketAddr::from_str(&format!("{}:{}", public_ip, chitchat_port))?;

    // Cluster
    let host_key = read_or_create_host_key(Path::new(&host_key_path))?;
    let chitchat_cluster = Cluster::new(
        host_key.to_string(),
        chitchat_addr,
        NodeType::Node,
        seed_nodes,
    )
    .await?;

    let writer_task = tokio::spawn(async move {
        let addr = Configuration::writer_listen_address();
        info!("Writer listening for gRPC requests at: {:?}", addr);
        let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter
            .set_serving::<NodeWriterServer<NodeWriterGRPCDriver>>()
            .await;

        Server::builder()
            .add_service(health_service)
            .add_service(NodeWriterServer::new(grpc_driver))
            .serve(addr)
            .await
            .expect("Error starting gRPC writer");
    });

    let monitor_task = tokio::spawn(async move {
        let mut watcher = WatchStream::new(chitchat_cluster.members_change_watcher());
        loop {
            debug!("node writer wait updates");
            if let Some(update) = watcher.next().await {
                if let Ok(json_update) = serde_json::to_string(&update) {
                    info!("Chitchat cluster updated: {json_update}");
                };
            } else {
                error!("Chitchat cluster updated monitor fail");
            }
        }
    });

    if let Some(prometheus_url) = Configuration::get_prometheus_url() {
        info!("Start metrics task");

        let report = NodeReport::new(host_key.to_string())?;
        let mut metrics_publisher = metrics::Publisher::new("node_metrics", prometheus_url);

        if let Some((username, password)) =
            Configuration::get_prometheus_username().zip(Configuration::get_prometheus_password())
        {
            metrics_publisher = metrics_publisher.with_credentials(username, password);
        }

        let mut node_reader = NodeReaderService::new();
        node_reader.load_shards()?;

        let push_timing = Configuration::get_prometheus_push_timing();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(push_timing);

            loop {
                interval.tick().await;

                let (shard_count, paragraph_count) = node_reader.cache.values().fold(
                    (0, 0),
                    |(shard_count, paragraph_count), shard| {
                        (
                            shard_count + 1,
                            paragraph_count + shard.get_info().paragraphs,
                        )
                    },
                );

                report.shard_count.set(shard_count);
                report.paragraph_count.set(paragraph_count as i64);

                if let Err(e) = metrics_publisher.publish(&report).await {
                    error!("Cannot publish Node metrics: {}", e);
                } else {
                    info!(
                        "Publish Node metrics: shard_count {}, paragraph_count {}",
                        shard_count, paragraph_count
                    )
                }
            }
        });
    }

    info!("Bootstrap complete in: {:?}", start_bootstrap.elapsed());
    eprintln!("Running");

    tokio::try_join!(writer_task, monitor_task)?;

    // node_writer_service.shutdown().await;

    Ok(())
}
