use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chitchat::transport::UdpTransport;
use chitchat::{
    spawn_chitchat, Chitchat, ChitchatConfig, ChitchatHandle, FailureDetectorConfig, NodeId,
};
use serde::{Deserialize, Serialize};
use strum::{Display as EnumDisplay, EnumString};
use tokio::sync::watch;
use tokio_stream::StreamExt;
use tracing::{debug, error, info};
use uuid::Uuid;

use crate::error::Error;

/// The ID that makes the cluster unique.
const CLUSTER_ID: &str = "nucliadb-cluster";

pub const CLUSTER_GOSSIP_INTERVAL: Duration = Duration::from_millis(100);

pub fn read_host_key(host_key_path: &Path) -> Result<Uuid, Error> {
    let host_key_contents =
        fs::read(host_key_path).map_err(|err| Error::ReadHostKey(err.to_string()))?;

    let host_key = Uuid::from_slice(host_key_contents.as_slice())
        .map_err(|err| Error::ReadHostKey(err.to_string()))?;

    Ok(host_key)
}

/// Reads the key that makes a node unique from the given file.
/// If the file does not exist, it generates an ID and writes it to the file
/// so that it can be reused on reboot.
pub fn read_or_create_host_key(host_key_path: &Path) -> Result<Uuid, Error> {
    let host_key;

    if host_key_path.exists() {
        host_key = read_host_key(host_key_path)?;
        info!(host_key=?host_key, host_key_path=?host_key_path, "Read existing host key.");
    } else {
        if let Some(dir) = host_key_path.parent() {
            if !dir.exists() {
                fs::create_dir_all(dir).map_err(|err| Error::WriteHostKey(err.to_string()))?
            }
        }
        host_key = Uuid::new_v4();
        fs::write(host_key_path, host_key.as_bytes())
            .map_err(|err| Error::WriteHostKey(err.to_string()))?;
        info!(host_key=?host_key, host_key_path=?host_key_path, "Create new host key.");
    }

    Ok(host_key)
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, EnumString, EnumDisplay)]
pub enum NodeType {
    Node,
    Search,
    Ingest,
}

/// A member information.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Member {
    /// An ID that makes a member unique.
    pub node_id: String,

    /// Listen address.
    pub listen_addr: SocketAddr,

    // Type of node 'l': node reader 'e': node writer 'r': reader 'w': writer
    pub node_type: NodeType,

    /// If true, it means self.
    pub is_self: bool,
}

impl From<&Cluster> for Member {
    fn from(cluster: &Cluster) -> Self {
        Member {
            node_id: cluster.id.id.clone(),
            listen_addr: cluster.listen_addr,
            node_type: cluster.node_type.clone(),
            is_self: true,
        }
    }
}

impl Member {
    pub fn build(node_id: &NodeId, chitchat: &Chitchat) -> Result<Self, Error> {
        chitchat
            .node_state(node_id)
            .and_then(|state| state.get("node_type"))
            .ok_or_else(|| Error::MissingNodeState(node_id.id.clone()))
            .and_then(|node_type| {
                node_type
                    .parse()
                    .map_err(|_| Error::UnknownNodeType(node_type.to_string()))
            })
            .map(|node_type| Member {
                node_type,
                node_id: node_id.id.clone(),
                is_self: node_id.eq(chitchat.self_node_id()),
                listen_addr: node_id.gossip_public_address,
            })
    }
}

/// This is an implementation of a cluster using the chitchat protocol.
pub struct Cluster {
    /// A socket address that represents itself.
    pub listen_addr: SocketAddr,

    /// The actual cluster that implement the Scuttlebutt protocol.
    chitchat_handle: ChitchatHandle,

    /// self node id
    pub id: NodeId,

    /// seflf node type
    pub node_type: NodeType,

    // watcher for receiving actual list of nodes
    members: watch::Receiver<Vec<Member>>,

    /// A stop flag of cluster monitoring task.
    /// Once the cluster is created, a task to monitor cluster events will be started.
    /// Nodes do not need to be monitored for events once they are detached from the cluster.
    /// You need to update this value to get out of the task loop.
    stop: Arc<AtomicBool>,
}

impl Cluster {
    /// Create a cluster given a host key and a listen address.
    /// When a cluster is created, the thread that monitors cluster events
    /// will be started at the same time.
    pub async fn new(
        node_id: String,
        listen_addr: SocketAddr,
        node_type: NodeType,
        seed_node: Vec<String>,
    ) -> Result<Self, Error> {
        info!( node_id=?node_id, listen_addr=?listen_addr, "Create new cluster.");
        let chitchat_node_id = NodeId::new(node_id, listen_addr);

        let config = ChitchatConfig {
            node_id: chitchat_node_id,
            gossip_interval: CLUSTER_GOSSIP_INTERVAL,
            cluster_id: CLUSTER_ID.to_string(),
            listen_addr,
            seed_nodes: seed_node,
            failure_detector_config: FailureDetectorConfig::default(),
        };

        let chitchat_handle = spawn_chitchat(config, Vec::new(), &UdpTransport)
            .await
            .map_err(|e| Error::CannotStartCluster(e.to_string()))?;

        let (self_id, mut cluster_watcher) = chitchat_handle
            .with_chitchat(|chitchat| {
                let state = chitchat.self_node_state();
                state.set("node_type", node_type.clone());
                (
                    chitchat.self_node_id().clone(),
                    chitchat.live_nodes_watcher(),
                )
            })
            .await;

        let (members_tx, members_rx) = watch::channel(Vec::<Member>::new());

        let chitchat = chitchat_handle.chitchat();

        // Create cluster.
        let cluster = Cluster {
            listen_addr,
            chitchat_handle,
            id: self_id,
            node_type,
            members: members_rx,
            stop: Arc::new(AtomicBool::new(false)),
        };
        // Prepare to start a task that will monitor cluster events.
        let monitor_task_stop = cluster.stop.clone();

        let chitchat_clone = Arc::clone(&chitchat);
        // Start to monitor the node status updates.
        tokio::task::spawn(async move {
            while !monitor_task_stop.load(Ordering::Relaxed) {
                if let Some(live_nodes) = cluster_watcher.next().await {
                    let guard = chitchat_clone.lock().await;
                    let update_members: Vec<Member> = live_nodes
                        .iter()
                        .map(|node_id| Member::build(node_id, &guard).unwrap())
                        .collect();
                    let dead: Vec<Member> = guard
                        .dead_nodes()
                        .map(|node_id| Member::build(node_id, &guard).unwrap())
                        .collect();
                    debug!(updated_memberlist=?update_members);
                    debug!(dead_nodes=?dead);
                    if let Err(e) = members_tx.send(update_members) {
                        debug!("all member updates receivers closed: {e}");
                        break;
                    }
                }
            }
            debug!("receive a stop signal");
        });

        let chitchat_clone = Arc::clone(&chitchat);
        let interval_task_stop = cluster.stop.clone();
        tokio::task::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(500));
            while !interval_task_stop.load(Ordering::Relaxed) {
                interval.tick().await;
                let mut chitchat_guard = chitchat_clone.lock().await;
                chitchat_guard.update_nodes_liveliness();
            }
        });

        Ok(cluster)
    }

    /// Return watchstream for monitoring change of `members`
    pub fn members_change_watcher(&self) -> watch::Receiver<Vec<Member>> {
        self.members.clone()
    }

    pub async fn add_peer_node(&self, peer_addr: SocketAddr) -> Result<(), anyhow::Error> {
        self.chitchat_handle.gossip(peer_addr)
    }

    pub async fn shutdown(self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Err(e) = self.chitchat_handle.shutdown().await {
            error!("Error during chitchat shutdown: {e}");
        }
    }

    /// Return `members` list
    pub async fn members(&self) -> Vec<Member> {
        let mut nodes: Vec<Member> = self
            .chitchat_handle
            .with_chitchat(|chitchat| {
                chitchat.update_nodes_liveliness();
                chitchat
                    .live_nodes()
                    .map(|node_id| Member::build(node_id, chitchat).unwrap())
                    .collect()
            })
            .await;
        nodes.push(Member::from(self));
        nodes
    }
}
