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

use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use nucliadb_core::protos::node_reader_client::NodeReaderClient;
use nucliadb_core::protos::node_reader_server::NodeReaderServer;
use nucliadb_core::protos::node_writer_client::NodeWriterClient;
use nucliadb_core::protos::node_writer_server::NodeWriterServer;
use nucliadb_node::grpc::reader::NodeReaderGRPCDriver;
use nucliadb_node::grpc::writer::NodeWriterGRPCDriver;
use nucliadb_node::lifecycle;
use nucliadb_node::replication::replicator::connect_to_primary_and_replicate;
use nucliadb_node::replication::service::ReplicationServiceGRPCDriver;
use nucliadb_node::settings::*;
use nucliadb_node::shards::providers::unbounded_cache::AsyncUnboundedShardWriterCache;
use nucliadb_node::utils::read_or_create_host_key;
use nucliadb_protos::replication;
use tempfile::TempDir;
use tokio::sync::Notify;
use tonic::transport::{Channel, Server};

pub const SERVER_STARTUP_TIMEOUT: Duration = Duration::from_secs(5);

pub type TestNodeReader = NodeReaderClient<Channel>;
pub type TestNodeWriter = NodeWriterClient<Channel>;

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

fn find_open_port() -> u16 {
    loop {
        let port = rand::random::<u16>() % 10000 + 10000;
        let listener = TcpListener::bind(("127.0.0.1", port));
        if listener.is_ok() {
            return port;
        }
    }
}

pub struct NodeFixture {
    pub settings: Settings,
    pub secondary_settings: Settings,
    reader_client: Option<TestNodeReader>,
    writer_client: Option<TestNodeWriter>,
    secondary_reader_client: Option<TestNodeReader>,
    reader_addr: SocketAddr,
    writer_addr: SocketAddr,
    secondary_reader_addr: SocketAddr,
    reader_server_task: Option<tokio::task::JoinHandle<()>>,
    writer_server_task: Option<tokio::task::JoinHandle<()>>,
    primary_shard_cache: Option<Arc<AsyncUnboundedShardWriterCache>>,
    secondary_reader_server_task: Option<tokio::task::JoinHandle<()>>,
    secondary_writer_server_task: Option<tokio::task::JoinHandle<()>>,
    secondary_shard_cache: Option<Arc<AsyncUnboundedShardWriterCache>>,
    tempdir: TempDir,
    secondary_tempdir: TempDir,
    shutdown_notifier: Arc<Notify>,
    shutdown_notified: Arc<AtomicBool>,
}

impl NodeFixture {
    pub fn new() -> Self {
        let tempdir = TempDir::new().expect("Unable to create temporary data directory");
        let secondary_tempdir = TempDir::new().expect("Unable to create temporary data directory");

        let reader_addr =
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), find_open_port());
        let writer_addr =
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), find_open_port());
        let secondary_reader_addr =
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), find_open_port());

        let settings = Settings::builder()
            .data_path(tempdir.path())
            .reader_listen_address(reader_addr.to_string())
            .writer_listen_address(writer_addr.to_string())
            .build()
            .expect("Error while building test settings");
        let secondary_settings = Settings::builder()
            .data_path(secondary_tempdir.path())
            .reader_listen_address(secondary_reader_addr.to_string())
            .primary_address(writer_addr.to_string())
            .replication_delay_seconds(1_u64)
            .build()
            .expect("Error while building test settings");

        for _setting in [&settings, &secondary_settings] {
            let data_path = _setting.data_path();
            if !data_path.exists() {
                std::fs::create_dir(&data_path).expect("Cannot create data directory");
            }

            let shards_path = _setting.shards_path();
            if !shards_path.exists() {
                std::fs::create_dir(&shards_path).expect("Cannot create shards directory");
            }
        }

        let shutdown_notifier = Arc::new(Notify::new());
        let shutdown_notified = Arc::new(AtomicBool::new(false));

        let shutdown_notifier_clone = Arc::clone(&shutdown_notifier);
        let shutdown_notified_clone = Arc::clone(&shutdown_notified);

        tokio::spawn(async move {
            shutdown_notifier_clone.notified().await;
            shutdown_notified_clone.store(true, std::sync::atomic::Ordering::Relaxed);
        });

        Self {
            settings,
            secondary_settings,
            reader_client: None,
            writer_client: None,
            secondary_reader_client: None,
            reader_addr,
            writer_addr,
            secondary_reader_addr,
            reader_server_task: None,
            writer_server_task: None,
            primary_shard_cache: None,
            secondary_reader_server_task: None,
            secondary_writer_server_task: None,
            secondary_shard_cache: None,
            tempdir,
            secondary_tempdir,
            shutdown_notifier,
            shutdown_notified,
        }
    }

    pub async fn with_writer(&mut self) -> anyhow::Result<&mut Self> {
        if self.writer_server_task.is_some() {
            return Ok(self);
        }

        let settings = self.settings.clone();
        let cache_settings = self.settings.clone();
        let addr = self.writer_addr;
        let shards_cache = Arc::new(AsyncUnboundedShardWriterCache::new(cache_settings));
        self.primary_shard_cache = Some(Arc::clone(&shards_cache));
        let notifier = Arc::clone(&self.shutdown_notifier);
        self.writer_server_task = Some(tokio::spawn(async move {
            lifecycle::initialize_writer(settings.clone())
                .expect("Writer initialization has failed");
            let writer_server = NodeWriterServer::new(NodeWriterGRPCDriver::new(
                settings.clone(),
                Arc::clone(&shards_cache),
            ));
            let replication_server =
                replication::replication_service_server::ReplicationServiceServer::new(
                    ReplicationServiceGRPCDriver::new(
                        settings.clone(),
                        Arc::clone(&shards_cache),
                        "primary_id".to_string(),
                    ),
                );
            Server::builder()
                .add_service(writer_server)
                .add_service(replication_server)
                .serve_with_shutdown(addr, async {
                    notifier.notified().await;
                })
                .await
                .unwrap();
        }));

        wait_for_service_ready(self.writer_addr, SERVER_STARTUP_TIMEOUT).await?;

        let endpoint = format!("http://{}", addr);
        self.writer_client = Some(NodeWriterClient::connect(endpoint).await?);

        Ok(self)
    }

    pub async fn with_secondary_writer(&mut self) -> anyhow::Result<&mut Self> {
        if self.secondary_writer_server_task.is_some() {
            return Ok(self);
        }

        let settings = self.secondary_settings.clone();
        let cache_settings = self.secondary_settings.clone();
        let host_key_path = settings.host_key_path();
        let node_id = read_or_create_host_key(host_key_path)?;
        let shards_cache = Arc::new(AsyncUnboundedShardWriterCache::new(cache_settings));
        self.secondary_shard_cache = Some(shards_cache.clone());
        let notified = Arc::clone(&self.shutdown_notified);
        self.secondary_writer_server_task = Some(tokio::spawn(async move {
            lifecycle::initialize_writer(settings.clone())
                .expect("Writer initialization has failed");
            connect_to_primary_and_replicate(settings, shards_cache, node_id.to_string(), notified)
                .await
                .unwrap();
        }));

        Ok(self)
    }

    pub async fn with_reader(&mut self) -> anyhow::Result<&mut Self> {
        if self.writer_client.is_none() {
            self.with_writer().await?;
        }

        let settings = self.settings.clone();
        let addr = self.reader_addr;

        let notifier = Arc::clone(&self.shutdown_notifier);
        self.reader_server_task = Some(tokio::spawn(async move {
            lifecycle::initialize_reader(settings.clone());
            let grpc_driver = NodeReaderGRPCDriver::new(settings.clone());
            grpc_driver
                .initialize()
                .await
                .expect("Unable to initialize reader gRPC");
            let reader_server = NodeReaderServer::new(grpc_driver);
            Server::builder()
                .add_service(reader_server)
                .serve_with_shutdown(addr, async {
                    notifier.notified().await;
                })
                .await
                .unwrap();
        }));

        wait_for_service_ready(self.reader_addr, SERVER_STARTUP_TIMEOUT).await?;

        let endpoint = format!("http://{}", addr);
        self.reader_client = Some(NodeReaderClient::connect(endpoint).await?);

        Ok(self)
    }

    pub async fn with_secondary_reader(&mut self) -> anyhow::Result<&mut Self> {
        if self.secondary_writer_server_task.is_none() {
            self.with_secondary_writer().await?;
        }

        let settings = self.settings.clone();
        let addr = self.secondary_reader_addr;
        let notifier = Arc::clone(&self.shutdown_notifier);

        self.reader_server_task = Some(tokio::spawn(async move {
            lifecycle::initialize_reader(settings.clone());
            let grpc_driver = NodeReaderGRPCDriver::new(settings.clone());
            grpc_driver
                .initialize()
                .await
                .expect("Unable to initialize reader gRPC");
            let reader_server = NodeReaderServer::new(grpc_driver);
            Server::builder()
                .add_service(reader_server)
                .serve_with_shutdown(addr, async {
                    notifier.notified().await;
                })
                .await
                .unwrap();
        }));

        wait_for_service_ready(self.reader_addr, SERVER_STARTUP_TIMEOUT).await?;

        let endpoint = format!("http://{}", addr);
        self.secondary_reader_client = Some(NodeReaderClient::connect(endpoint).await?);

        Ok(self)
    }

    pub fn reader_client(&self) -> TestNodeReader {
        self.reader_client
            .as_ref()
            .expect("Reader client not initialized")
            .clone()
    }

    pub fn secondary_reader_client(&self) -> TestNodeReader {
        self.secondary_reader_client
            .as_ref()
            .expect("Reader client not initialized")
            .clone()
    }

    pub fn writer_client(&self) -> TestNodeWriter {
        self.writer_client
            .as_ref()
            .expect("Writer client not initialized")
            .clone()
    }

    pub fn primary_shard_cache(&self) -> Arc<AsyncUnboundedShardWriterCache> {
        self.primary_shard_cache
            .as_ref()
            .expect("Shard cache not initialized")
            .clone()
    }

    pub fn secondary_shard_cache(&self) -> Arc<AsyncUnboundedShardWriterCache> {
        self.secondary_shard_cache
            .as_ref()
            .expect("Shard cache not initialized")
            .clone()
    }
}

impl Drop for NodeFixture {
    fn drop(&mut self) {
        self.shutdown_notifier.notify_waiters();
    }
}
