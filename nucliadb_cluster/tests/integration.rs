use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener};
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use std::{env, io};

use bytes::BytesMut;
use dockertest::{Composition, DockerTest, StartPolicy};
use log::error;
use nucliadb_cluster::{Node, NodeHandle, NodeSnapshot, NodeType};
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use uuid::Uuid;

const SEED_NODE: &str = "0.0.0.0:4040";
const UPDATE_INTERVAL: Duration = Duration::from_millis(250);

pub async fn create_seed_node() -> anyhow::Result<NodeHandle> {
    let node = Node::builder()
        .register_as(NodeType::Io)
        .on_local_network(SocketAddr::from_str(SEED_NODE).unwrap())
        .with_seed_nodes(vec![SEED_NODE.to_string()])
        .with_update_interval(UPDATE_INTERVAL)
        .build()?;

    let node = node.start().await?;

    Ok(node)
}

pub fn find_available_port() -> anyhow::Result<u16> {
    let socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);
    let listener = TcpListener::bind(socket)?;
    let port = listener.local_addr()?.port();

    Ok(port)
}

pub async fn create_node_for_test_with_id(
    id: String,
    seed_node: String,
) -> anyhow::Result<NodeHandle> {
    let port = find_available_port()?;

    eprintln!("port: {port}");

    let node = Node::builder()
        .with_id(id)
        .register_as(NodeType::Io)
        .on_local_network(SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), port))
        .with_seed_nodes(vec![seed_node])
        .with_update_interval(UPDATE_INTERVAL)
        .build()?;

    let node = node.start().await?;

    Ok(node)
}

/// Creates a local cluster listening on a random port.
pub async fn create_node_for_test(seed_node: String) -> anyhow::Result<NodeHandle> {
    let peer_uuid = Uuid::new_v4().to_string();
    let node = create_node_for_test_with_id(peer_uuid, seed_node).await?;

    Ok(node)
}

pub fn setup_logging_for_tests() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        env_logger::builder().format_timestamp(None).init();
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial("cluster")]
async fn test_cluster_single_node() {
    setup_logging_for_tests();
    // create seed node
    let node = create_seed_node().await.unwrap();

    tokio::time::sleep(UPDATE_INTERVAL * 2).await;

    let live_nodes = node.live_nodes().await;

    assert_eq!(live_nodes.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial("cluster")]
async fn test_cluster_two_nodes() {
    setup_logging_for_tests();
    // create seed node
    let first_node = create_seed_node().await.unwrap();
    let mut cluster_watcher = first_node.cluster_watcher().await;

    // add node to cluster
    let second_node = create_node_for_test(SEED_NODE.to_string()).await.unwrap();

    // allow nodes start and communicate
    tokio::time::sleep(UPDATE_INTERVAL * 2).await;

    match tokio::time::timeout(UPDATE_INTERVAL, cluster_watcher.next()).await {
        Ok(Some(live_nodes)) => {
            let live_nodes = live_nodes
                .into_iter()
                .map(|node| node.id().to_string())
                .collect::<Vec<_>>();

            assert_eq!(live_nodes.len(), 2);
            assert!(live_nodes.contains(&first_node.id().to_string()));
            assert!(live_nodes.contains(&second_node.id().to_string()));
        }
        Ok(None) => {
            panic!("no changes in cluster");
        }
        Err(e) => {
            panic!("timeout while waiting cluster changes: {e}");
        }
    }
}

enum NodeOperation {
    Add,
    Delete,
}

struct TestClusterState {
    nodes: AtomicUsize,
}

impl TestClusterState {
    fn new() -> Self {
        Self {
            nodes: AtomicUsize::new(0),
        }
    }

    pub(crate) fn change_state(&self, ops: &NodeOperation) {
        match ops {
            NodeOperation::Add => self.nodes.fetch_add(1, Ordering::AcqRel),
            NodeOperation::Delete => self.nodes.fetch_sub(1, Ordering::AcqRel),
        };
    }

    pub(crate) fn nodes(&self) -> usize {
        self.nodes.load(Ordering::Relaxed)
    }
}

#[ignore = "ignore"]
#[allow(unused_assignments)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_integration_3_nodes_with_monitor() {
    let registry = env::var("IMAGE_REPOSITORY").unwrap();
    let sock_path = env::var("SOCKET_PATH").unwrap();
    let mut docker = DockerTest::new();

    let unix_stream = UnixStream::connect(sock_path.clone()).await.unwrap();
    let (tx, mut rx) = mpsc::channel::<()>(2);
    let state = TestClusterState::new();
    let mut operation = NodeOperation::Add;

    tokio::spawn(async move {
        loop {
            if unix_stream.readable().await.is_ok() {
                let mut buffer = BytesMut::with_capacity(512);
                match unix_stream.try_read(&mut buffer) {
                    Ok(bytes_read) => {
                        assert_ne!(
                            bytes_read, 0,
                            "0 bytes read from socket. Connection closed by writer"
                        );
                        let update = serde_json::from_slice::<Vec<NodeSnapshot>>(&buffer).unwrap();
                        state.change_state(&operation);
                        assert_eq!(update.len(), state.nodes());
                        if state.nodes() == 2 {
                            tx.send(()).await.unwrap();
                            operation = NodeOperation::Delete;
                            break; // TODO: when dockertest crate will implement stop/pause delete
                                   // this break
                        }
                    }
                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => continue,
                    Err(e) => {
                        error!("error during reading from unix socket: {e}");
                        break;
                    }
                }
            }
        }
    });

    let mut watcher_node = Composition::with_repository(registry.clone())
        .with_container_name("node_with_watcher")
        .with_start_policy(StartPolicy::Strict);
    watcher_node.bind_mount(sock_path.clone(), sock_path.clone());

    let node2 = Composition::with_repository(registry.clone())
        .with_container_name("node2")
        .with_start_policy(StartPolicy::Strict);

    let node3 = Composition::with_repository(registry)
        .with_container_name("node3")
        .with_start_policy(StartPolicy::Strict);

    docker.add_composition(watcher_node);
    docker.add_composition(node2);
    docker.add_composition(node3);
    docker.run(|_ops| async move {
        // wait until all containers started and all nodes will be in cluster
        assert_eq!(Some(()), rx.recv().await);

        // TODO: when dockertest crate will implement stop/pause for containers
        // sequentially turn off the nodes from the cluster
    });
}
