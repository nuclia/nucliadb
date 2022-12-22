use std::collections::HashSet;
use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use chitchat::transport::UdpTransport;
use chitchat::{
    spawn_chitchat, Chitchat, ChitchatConfig, ChitchatHandle, FailureDetectorConfig, NodeId,
};
use serde::{Deserialize, Serialize};
use strum::{Display as EnumDisplay, EnumString};
use tokio_stream::wrappers::WatchStream;
use tracing::info;
use uuid::Uuid;

use crate::error::Error;

const NODE_TYPE_KEY: &str = "node_type";
const LOAD_SCORE_KEY: &str = "load_score";

pub trait Score {
    fn score(&self) -> f32;
}

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

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, EnumString, EnumDisplay)]
pub enum NodeType {
    Node,
    Search,
    Ingest,
}

/// A member information.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Member {
    /// An ID that makes a member unique.
    pub node_id: String,

    /// Listen address.
    pub listen_addr: SocketAddr,

    // Type of node 'l': node reader 'e': node writer 'r': reader 'w': writer
    pub node_type: NodeType,

    /// If true, it means self.
    pub is_self: bool,

    /// the load score of the node
    pub load_score: f32,
}

impl Member {
    pub fn build(node_id: &NodeId, chitchat: &Chitchat) -> Result<Self, Error> {
        chitchat
            .node_state(node_id)
            .and_then(|state| {
                state
                    .get(NODE_TYPE_KEY)
                    .map(|node_type| (node_type, state.get(LOAD_SCORE_KEY).unwrap_or("0")))
            })
            .ok_or_else(|| Error::MissingNodeState(node_id.id.clone()))
            .and_then(|(node_type, load_score)| {
                Ok((
                    node_type
                        .parse()
                        .map_err(|_| Error::UnknownNodeType(node_type.to_string()))?,
                    load_score
                        .parse()
                        .map_err(|_| Error::InvalidLoadScore(load_score.to_string()))?,
                ))
            })
            .map(|(node_type, load_score)| Member {
                node_type,
                load_score,
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
    handle: ChitchatHandle,

    /// self node id
    pub id: NodeId,

    /// seflf node type
    pub node_type: NodeType,
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
        let node_id = NodeId::new(node_id, listen_addr);

        let config = ChitchatConfig {
            node_id: node_id.clone(),
            gossip_interval: CLUSTER_GOSSIP_INTERVAL,
            cluster_id: CLUSTER_ID.to_string(),
            listen_addr,
            seed_nodes: seed_node,
            failure_detector_config: FailureDetectorConfig::default(),
        };

        let handle = spawn_chitchat(config, Vec::new(), &UdpTransport)
            .await
            .map_err(|e| Error::CannotStartCluster(e.to_string()))?;

        handle
            .with_chitchat(|chitchat| {
                let state = chitchat.self_node_state();
                state.set(NODE_TYPE_KEY, node_type);
                state.set(LOAD_SCORE_KEY, 0f32);
            })
            .await;

        // Create cluster.
        let cluster = Cluster {
            id: node_id,
            listen_addr,
            handle,
            node_type,
        };

        Ok(cluster)
    }

    pub async fn update_load_score<T: Score>(&self, scorer: &T) {
        self.handle
            .with_chitchat(|chitchat| {
                let state = chitchat.self_node_state();

                state.set(LOAD_SCORE_KEY, scorer.score());
            })
            .await;
    }

    /// Return watchstream for monitoring change of `members`
    pub async fn live_nodes_watcher(&self) -> WatchStream<HashSet<NodeId>> {
        self.handle
            .with_chitchat(|chitchat| chitchat.live_nodes_watcher())
            .await
    }

    pub async fn build_members(&self, nodes: HashSet<NodeId>) -> Vec<Member> {
        self.handle
            .with_chitchat(|chitchat| {
                nodes
                    .iter()
                    .map(|node_id| Member::build(node_id, chitchat).unwrap())
                    .collect()
            })
            .await
    }

    pub async fn add_peer_node(&self, peer_addr: SocketAddr) -> Result<(), anyhow::Error> {
        self.handle.gossip(peer_addr)
    }

    /// Return `members` list
    pub async fn members(&self) -> Vec<Member> {
        let nodes: Vec<Member> = self
            .handle
            .with_chitchat(|chitchat| {
                chitchat.update_nodes_liveliness();
                chitchat
                    .live_nodes()
                    .chain([&self.id]) // NOTE: I think we can remove this line
                    .map(|node_id| Member::build(node_id, chitchat).unwrap())
                    .collect()
            })
            .await;

        nodes
    }
}
