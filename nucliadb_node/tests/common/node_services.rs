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
use std::time::Duration;

use nucliadb_core::protos::node_reader_client::NodeReaderClient;
use nucliadb_core::protos::node_reader_server::NodeReaderServer;
use nucliadb_core::protos::node_writer_client::NodeWriterClient;
use nucliadb_core::protos::node_writer_server::NodeWriterServer;
use nucliadb_node::grpc::reader::NodeReaderGRPCDriver;
use nucliadb_node::grpc::writer::NodeWriterGRPCDriver;
use nucliadb_node::settings::Settings;
use nucliadb_node::{env, lifecycle};
use once_cell::sync::{Lazy, OnceCell};
use tempfile::TempDir;
use tokio::sync::Mutex;
use tonic::transport::{Channel, Server};

use crate::common::{READER_ADDR, SERVER_STARTUP_TIMEOUT, WRITER_ADDR};

pub type TestNodeReader = NodeReaderClient<Channel>;
pub type TestNodeWriter = NodeWriterClient<Channel>;

static READER_SERVER_INITIALIZED: Lazy<Arc<Mutex<bool>>> =
    Lazy::new(|| Arc::new(Mutex::new(false)));
static WRITER_SERVER_INITIALIZED: Lazy<Arc<Mutex<bool>>> =
    Lazy::new(|| Arc::new(Mutex::new(false)));

static TEST_DATA_PATH: OnceCell<TempDir> = OnceCell::new();

// Use global settings for now, change it when more test flexibility is desired
static SETTINGS: Lazy<Arc<Settings>> = Lazy::new(|| {
    // REVIEW: temporary directory is not being deleted at the end of the tests.
    // In fact, tests create a different test directory per test file...
    let tempdir = TEST_DATA_PATH.get_or_init(|| {
        let tempdir = TempDir::new().expect("Unable to create temporary data directory");
        println!("Using data path directory: {}", tempdir.path().display());
        tempdir
    });

    let settings = Settings::builder()
        .data_path(tempdir.path())
        .build()
        .expect("Error while building test settings");
    Arc::new(settings)
});

async fn start_reader(addr: SocketAddr) {
    let mut initialized_lock = READER_SERVER_INITIALIZED.lock().await;
    if *initialized_lock {
        return;
    }
    tokio::spawn(async move {
        lifecycle::initialize_reader();
        let grpc_driver = NodeReaderGRPCDriver::new(Arc::clone(&SETTINGS));
        grpc_driver
            .initialize()
            .await
            .expect("Unable to initialize reader gRPC");
        let reader_server = NodeReaderServer::new(grpc_driver);
        Server::builder()
            .add_service(reader_server)
            .serve(addr)
            .await
            .map_or_else(
                |err| {
                    panic!("Error starting gRPC server: {err:?}");
                },
                |_| {
                    *initialized_lock = true;
                },
            );
    });
}

async fn start_writer(addr: SocketAddr) {
    let mut initialized_lock = WRITER_SERVER_INITIALIZED.lock().await;
    if *initialized_lock {
        return;
    }
    let data_path = SETTINGS.data_path();
    let shards_path = SETTINGS.shards_path();
    if !data_path.exists() {
        std::fs::create_dir(&data_path).expect("Can not create data directory");
    }

    tokio::spawn(async move {
        let settings = SETTINGS.clone();
        lifecycle::initialize_writer(&data_path, &shards_path)
            .expect("Writer initialization has failed");
        let writer_server = NodeWriterServer::new(NodeWriterGRPCDriver::new(settings));
        Server::builder()
            .add_service(writer_server)
            .serve(addr)
            .await
            .map_or_else(
                |err| {
                    panic!("Error starting gRPC server: {err:?}");
                },
                |_| {
                    *initialized_lock = true;
                },
            );
    });
}

async fn wait_for_service_ready(addr: SocketAddr, timeout: Duration) -> anyhow::Result<()> {
    let server_uri = tonic::transport::Uri::builder()
        .scheme("http")
        .authority(addr.to_string())
        .path_and_query("/")
        .build()
        .unwrap();

    backoff::future::retry(
        backoff::ExponentialBackoffBuilder::new()
            .with_initial_interval(Duration::from_millis(10))
            .with_max_interval(Duration::from_millis(100))
            .with_max_elapsed_time(Some(timeout))
            .build(),
        || async {
            match Channel::builder(server_uri.clone()).connect().await {
                Ok(_channel) => Ok(()),
                Err(err) => Err(backoff::Error::Transient {
                    err,
                    retry_after: None,
                }),
            }
        },
    )
    .await?;

    Ok(())
}

fn initialize_file_system() {
    let data_path = env::data_path();
    if !data_path.exists() {
        std::fs::create_dir(&data_path).expect("Cannot create data directory");
    }

    let shards_path = env::shards_path();
    if !shards_path.exists() {
        std::fs::create_dir(&shards_path).expect("Cannot create shards directory");
    }
}

async fn node_reader_server() -> anyhow::Result<()> {
    start_reader(*READER_ADDR).await;
    wait_for_service_ready(*READER_ADDR, SERVER_STARTUP_TIMEOUT).await?;
    Ok(())
}

async fn node_writer_server() -> anyhow::Result<()> {
    start_writer(*WRITER_ADDR).await;
    wait_for_service_ready(*WRITER_ADDR, SERVER_STARTUP_TIMEOUT).await?;
    Ok(())
}

async fn node_reader_client() -> TestNodeReader {
    let endpoint = format!("http://{}", *READER_ADDR);
    NodeReaderClient::connect(endpoint)
        .await
        .expect("Error creating gRPC reader client")
}

async fn node_writer_client() -> TestNodeWriter {
    let endpoint = format!("http://{}", *WRITER_ADDR);
    NodeWriterClient::connect(endpoint)
        .await
        .expect("Error creating gRPC reader client")
}

pub async fn node_reader() -> TestNodeReader {
    node_reader_server()
        .await
        .expect("Error starting node reader");
    node_reader_client().await
}

pub async fn node_writer() -> TestNodeWriter {
    node_writer_server()
        .await
        .expect("Error starting node writer");
    node_writer_client().await
}
