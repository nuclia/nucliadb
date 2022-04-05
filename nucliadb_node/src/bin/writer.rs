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
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use nucliadb_cluster::cluster::{read_or_create_host_key, Cluster};
use nucliadb_node::config::Configuration;
use nucliadb_node::writer::NodeWriterService;
use nucliadb_protos::node_writer_server::NodeWriterServer;
use tonic::transport::Server;
use tracing::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("NucliaDB Writer Node starting...");
    env_logger::init();

    let _guard = sentry::init((
        Configuration::sentry_url(),
        sentry::ClientOptions {
            release: sentry::release_name!(),
            ..Default::default()
        },
    ));

    let start_bootstrap = Instant::now();

    let node_writer_service = NodeWriterService::new();

    std::fs::create_dir_all(Configuration::shards_path())?;
    if !Configuration::lazy_loading() {
        node_writer_service.load_shards().await?;
    }

    let host_key_path = Configuration::host_key_path();
    let swim_addr = Configuration::swim_addr();
    let swim_peers_addrs = Configuration::swim_peers_addrs();

    // Cluster
    let host_key = read_or_create_host_key(Path::new(&host_key_path))?;
    let cluster = Arc::new(Cluster::new(
        host_key.to_string(),
        swim_addr,
        'N',
        Configuration::swim_timeout() as u64,
        Configuration::swim_interval() as u64,
    )?);

    // Basicamente especifica todos los peers y quita el que soy yo.
    for peer_addr in swim_peers_addrs {
        if peer_addr != swim_addr {
            debug!("Add peer node: {}", peer_addr);
            cluster.add_peer_node(peer_addr).await;
        }
    }

    let writer_task = tokio::spawn(async move {
        let addr = Configuration::writer_listen_address();
        info!("Writer listening for gRPC requests at: {:?}", addr);
        let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter
            .set_serving::<NodeWriterServer<NodeWriterService>>()
            .await;

        Server::builder()
            .add_service(health_service)
            .add_service(NodeWriterServer::new(node_writer_service))
            .serve(addr)
            .await
            .expect("Error starting gRPC writer");
    });

    info!("Bootstrap complete in: {:?}", start_bootstrap.elapsed());
    eprintln!("Running");

    tokio::try_join!(writer_task)?;

    // node_writer_service.shutdown().await;

    Ok(())
}
