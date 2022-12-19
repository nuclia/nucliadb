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

use super::{READER_HOST, READER_PORT, SERVER_STARTUP_TIMEOUT};


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

    let mut stub = backoff::future::retry(
        backoff::ExponentialBackoffBuilder::new()
            .with_max_elapsed_time(Some(SERVER_STARTUP_TIMEOUT))
            .build(),
        || async {
            match Channel::builder(server_uri.clone()).connect().await {
                Ok(channel) => {
                    let stub = HealthClient::new(channel);
                    Ok(stub)
                },
                Err(err) => {
                    Err(backoff::Error::Transient { err: err, retry_after: None })
                },
            }
        }
    ).await.unwrap();

    let result = stub.check(
        HealthCheckRequest { service: "nodereader.NodeReader".to_string() }
    ).await.unwrap();

    let serving_status = result.get_ref().status();
    assert!(
        serving_status == ServingStatus::Serving,
        "Test error: reader gRPC server is not serving, it is {serving_status:?}"
    );
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
