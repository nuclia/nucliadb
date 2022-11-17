use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener};
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{env, io};

use bytes::BytesMut;
use dockertest::{Composition, DockerTest, StartPolicy};
use log::error;
use nucliadb_cluster::cluster::{Cluster, Member, NodeType, CLUSTER_GOSSIP_INTERVAL};
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use uuid::Uuid;

const SEED_NODE: &str = "0.0.0.0:40400";

pub async fn create_seed_node() -> anyhow::Result<Cluster> {
    // create seed node
    let peer_addr = SocketAddr::from_str(SEED_NODE).unwrap();
    Ok(Cluster::new(
        Uuid::new_v4().to_string(),
        peer_addr,
        NodeType::Node,
        vec![SEED_NODE.to_string()],
    )
    .await?)
}

pub fn find_available_port() -> anyhow::Result<u16> {
    let socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);
    let listener = TcpListener::bind(socket)?;
    let port = listener.local_addr()?.port();
    Ok(port)
}

pub async fn create_cluster_for_test_with_id(
    peer_uuid: String,
    seed_node: String,
) -> anyhow::Result<Cluster> {
    let port = find_available_port()?;
    eprintln!("port: {port}");
    let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), port);
    let cluster = Cluster::new(peer_uuid, peer_addr, NodeType::Node, vec![seed_node]).await?;
    Ok(cluster)
}

/// Creates a local cluster listening on a random port.
pub async fn create_cluster_for_test(seed_node: String) -> anyhow::Result<Cluster> {
    let peer_uuid = Uuid::new_v4().to_string();
    let cluster = create_cluster_for_test_with_id(peer_uuid, seed_node).await?;
    Ok(cluster)
}

pub fn setup_logging_for_tests() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        env_logger::builder().format_timestamp(None).init();
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_cluster_single_node() {
    setup_logging_for_tests();
    // create seed node
    let cluster = create_seed_node().await.unwrap();

    tokio::time::sleep(CLUSTER_GOSSIP_INTERVAL * 2).await;

    let members: Vec<Member> = cluster.members().await;
    assert_eq!(members.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_cluster_two_nodes() {
    setup_logging_for_tests();
    // create seed node
    let cluster = create_seed_node().await.unwrap();
    let mut watcher = cluster.members_change_watcher();

    // add node to cluster
    let cluster1 = create_cluster_for_test(SEED_NODE.to_string())
        .await
        .unwrap();

    // allow nodes start and communicate
    tokio::time::sleep(CLUSTER_GOSSIP_INTERVAL * 2).await;

    match tokio::time::timeout(CLUSTER_GOSSIP_INTERVAL, watcher.changed()).await {
        Ok(_) => {
            let update = &*watcher.borrow();
            assert_eq!(update.len(), 1);
            assert_eq!(update[0].node_id, cluster1.id.id);
            assert_eq!(cluster1.members().await.len(), 2)
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
                        let update = serde_json::from_slice::<Vec<Member>>(&buffer).unwrap();
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
