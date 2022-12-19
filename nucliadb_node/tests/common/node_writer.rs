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

use nucliadb_node::writer::NodeWriterService;
use nucliadb_node::writer::grpc_driver::NodeWriterGRPCDriver;
use nucliadb_protos::node_writer_client::NodeWriterClient;
use nucliadb_protos::node_writer_server::NodeWriterServer;
use nucliadb_protos::{EmptyQuery, ShardId};
use std::str::FromStr;
use std::net::{SocketAddr, IpAddr};
use tonic::Request;
use tonic::transport::{Server, Channel};
use tonic_health::proto::HealthCheckRequest;
use tonic_health::proto::health_client::HealthClient;
use tonic_health::proto::health_check_response::ServingStatus;

use super::{SERVER_STARTUP_TIMEOUT, WRITER_HOST, WRITER_PORT};


pub type TestNodeWriter = NodeWriterClient<Channel>;


/// Start a Node Writer gRPC server ready to accept new connections
pub async fn node_writer_server() {
    tokio::spawn(async {
        let addr = SocketAddr::new(IpAddr::from_str(WRITER_HOST).unwrap(), WRITER_PORT);
        let writer_server = NodeWriterServer::new(
            NodeWriterGRPCDriver::from(
                NodeWriterService::new()
            )
        );
        let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter
            .set_serving::<NodeWriterServer<NodeWriterGRPCDriver>>()
            .await;

        Server::builder()
            .add_service(health_service)
            .add_service(writer_server)
            .serve(addr)
            .await
            .expect("Error starting gRPC server")
    });


    let server_uri = tonic::transport::Uri::builder()
        .scheme("http")
        .authority(format!("{}:{}", WRITER_HOST, WRITER_PORT))
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
        HealthCheckRequest { service: "nodewriter.NodeWriter".to_string() }
    ).await.unwrap();

    let serving_status = result.get_ref().status();
    assert!(
        serving_status == ServingStatus::Serving,
        "Test error: writer gRPC server is not serving, it is {serving_status:?}"
    );
}


/// Create a new Node Writer gRPC Client
pub async fn node_writer_client() -> TestNodeWriter {
    let client = NodeWriterClient::connect(
        format!("http://{}:{}", WRITER_HOST, WRITER_PORT)
    )
        .await
        .expect("Error creating gRPC writer client");

    client
}


// Operations


pub async fn new_shard(writer: &mut TestNodeWriter) -> String {
    let response = writer
        .new_shard(Request::new(EmptyQuery {}))
        .await
        .unwrap();

    let shard_id = response.get_ref().id.clone();

    shard_id
}

pub async fn get_shard(writer: &mut TestNodeWriter, shard_id: String) -> String {
    let response = writer
        .get_shard(Request::new(
            ShardId {
                id: shard_id.clone()
            }
        ))
        .await
        .unwrap();

    let shard_id = response.get_ref().id.clone();

    shard_id
}

pub async fn list_shards(writer: &mut TestNodeWriter) -> Vec<String> {
    let response = writer
        .list_shards(Request::new(EmptyQuery {}))
        .await
        .unwrap();

    let shards = response
        .get_ref()
        .ids
        .iter()
        .map(|shard_id| shard_id.id.clone())
        .collect::<Vec<String>>();

    shards
}

pub async fn delete_shard(writer: &mut TestNodeWriter, shard_id: String) -> String {
    let response = writer
        .delete_shard(Request::new(ShardId {
            id: shard_id
        }))
        .await
        .unwrap();

    let deleted_shard_id = response.get_ref().id.clone();

    deleted_shard_id
}
