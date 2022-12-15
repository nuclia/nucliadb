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

use nucliadb_node::reader::NodeReaderService;
use nucliadb_node::reader::grpc_driver::NodeReaderGRPCDriver;
use nucliadb_protos::node_reader_client::NodeReaderClient;
use nucliadb_protos::node_reader_server::NodeReaderServer;
use std::str::FromStr;
use std::net::{SocketAddr, IpAddr};
use tonic::transport::{Server, Channel};
use tonic_health::proto::HealthCheckRequest;
use tonic_health::proto::health_client::HealthClient;
use tonic_health::proto::health_check_response::ServingStatus;

use super::misc::linear_backoff;


const READER_HOST: &str = "127.0.0.1";
const READER_PORT: u16 = 18031;

pub type TestNodeReader = NodeReaderClient<Channel>;


/// Start a Node Reader gRPC server ready to accept new connections
pub async fn node_reader_server() {
    tokio::spawn(async {
        let addr = SocketAddr::new(IpAddr::from_str(READER_HOST).unwrap(), READER_PORT);
        let reader_server = NodeReaderServer::new(
            NodeReaderGRPCDriver::from(
                NodeReaderService::new()
            )
        );
        let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter
            .set_serving::<NodeReaderServer<NodeReaderGRPCDriver>>()
            .await;

        Server::builder()
            .add_service(health_service)
            .add_service(reader_server)
            .serve(addr)
            .await
            .expect("Error starting gRPC server")
    });


    let server_uri = tonic::transport::Uri::builder()
        .scheme("http")
        .authority(format!("{}:{}", READER_HOST, READER_PORT))
        .path_and_query("/")
        .build()
        .unwrap();

    let mut wait = 100;
    const MAX_WAIT: u64 = 5000;
    while let Err(error) = Channel::builder(server_uri.clone()).connect().await {
        tokio::time::sleep(tokio::time::Duration::from_millis(wait)).await;
        wait = linear_backoff(wait, 2);
        if wait > MAX_WAIT {
            panic!(
                "Something went wrong while starting reader gRPC server (too many times): {:?}",
                error
            );
        }
    }

    let mut stub: HealthClient<Channel> = HealthClient::new(Channel::builder(server_uri).connect().await.unwrap());
    let result = stub.check(
        HealthCheckRequest { service: "nodereader.NodeReader".to_string() }
    ).await.unwrap();

    if result.get_ref().status() != ServingStatus::Serving {
        panic!(
            "reader gRPC server not serving, it's {:?}!",
            result.get_ref().status()
        );
    }
}


/// Create a new Node Reader gRPC Client
pub async fn node_reader_client() -> TestNodeReader {
    let client = NodeReaderClient::connect(
        format!("http://{}:{}", READER_HOST, READER_PORT)
    )
        .await
        .expect("Error creating gRPC reader client");

    client
}
