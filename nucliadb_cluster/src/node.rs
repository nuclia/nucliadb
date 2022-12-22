use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use chitchat::transport::UdpTransport;
use chitchat::{ChitchatConfig, ChitchatHandle, FailureDetectorConfig, NodeId};
use derive_builder::Builder;
use futures::{stream, Stream, StreamExt};
use serde::{Deserialize, Serialize};
use strum::{Display as EnumDisplay, EnumString};
use uuid::Uuid;

use crate::error::Error;
use crate::key::Key;
use crate::register::Register;

const NODE_KIND_KEY: Key<NodeType> = Key::new("## nucliadb reserved ## node_type");
const REGISTER_KEY: Key<Register> = Key::new("## nucliadb reserved ## register");

/// Retrieve the information about a given list of cluster nodes.
pub async fn cluster_snapshot(nodes: Vec<NodeHandle>) -> Vec<NodeSnapshot> {
    stream::iter(nodes)
        .then(|node| async move { node.snapshot().await })
        .collect()
        .await
}

#[non_exhaustive]
#[derive(
    Debug, Copy, Clone, PartialEq, Eq, Hash, EnumString, EnumDisplay, Serialize, Deserialize,
)]
pub enum NodeType {
    Io,
    Search,
    Ingest,
    Train,
    Unknown,
}

/// Represents a single node instance in a distributed cluster based on scuttlebut protocol.
///
/// # Examples
/// ```rust,no_run
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use nucliadb_cluster::{Node, NodeType};
///
/// let node = Node::builder()
///     .register_as(NodeType::Ingest)
///     .on_local_network("0.0.0.0:4000".parse()?)
///     .build()?;
///
/// // Holds that handle to keep the node alive.
/// let node = node.start().await?;
///
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Builder)]
#[builder(pattern = "owned", setter(prefix = "with", strip_option, into))]
pub struct Node {
    /// The node identifier.
    #[builder(default = "Uuid::new_v4().to_string()")]
    id: String,
    /// The type of the node.
    #[builder(setter(name = "register_as"))]
    r#type: NodeType,
    /// The cluster identifier to join.
    #[builder(
        setter(name = "join_cluster"),
        default = "Node::DEFAULT_CLUSTER_ID.to_string()"
    )]
    cluster_id: String,
    /// A list of known nodes to connect with in order to join the cluster.
    #[builder(default)]
    seed_nodes: Vec<String>,
    /// The address the node will listen to.
    /// Note that address can be the same as the `public_address` if the node
    /// joins a local cluster.
    #[builder(setter(custom))]
    listen_address: SocketAddr,
    /// The public address to communicate with the node.
    #[builder(setter(custom))]
    public_address: SocketAddr,
    /// The interval between each node update.
    /// An update consists to send/receive the node state to/from other cluster nodes.
    #[builder(default = "Node::DEFAULT_UPDATE_INTERVAL")]
    update_interval: Duration,
    /// The initial node state.
    #[builder(setter(custom), default)]
    initial_state: Vec<(String, String)>,
}

impl NodeBuilder {
    /// Configures the node to join a local cluster network.
    pub fn on_local_network(mut self, address: SocketAddr) -> Self {
        self.listen_address = Some(address);
        self.public_address = Some(address);

        self
    }

    /// Configures the node to join a public cluster network.
    pub fn on_public_network(
        mut self,
        public_address: SocketAddr,
        listen_address: SocketAddr,
    ) -> Self {
        self.listen_address = Some(listen_address);
        self.public_address = Some(public_address);

        self
    }

    /// Inserts the given key/value to the initial node state.
    ///
    /// Note that, in case of key duplication, only the last one will be kept in the state.
    pub fn insert_to_initial_state<T: ToString>(mut self, key: Key<T>, value: T) -> Self {
        self.initial_state
            .get_or_insert_with(Vec::default)
            .push((key.to_string(), value.to_string()));

        self
    }
}

impl Node {
    const DEFAULT_CLUSTER_ID: &str = "nucliadb-cluster";
    const DEFAULT_UPDATE_INTERVAL: Duration = Duration::from_millis(100);

    /// Returns a builder to create a custom `Node`.
    pub fn builder() -> NodeBuilder {
        NodeBuilder::default()
    }

    /// Starts the node and join the cluster.
    ///
    /// The handle returning by this method must be kept in order to keep alive the node
    /// and retrieve information about the cluster.
    ///
    /// # Errors
    /// This method may fails in the node does not succeed to join the cluster.
    pub async fn start(mut self) -> Result<NodeHandle, Error> {
        let node_id = NodeId::new(self.id, self.public_address);

        let configuration = ChitchatConfig {
            node_id: node_id.clone(),
            cluster_id: self.cluster_id,
            gossip_interval: self.update_interval,
            listen_addr: self.listen_address,
            seed_nodes: self.seed_nodes,
            failure_detector_config: FailureDetectorConfig::default(),
            is_ready_predicate: None,
        };

        let register = Register::with_keys(
            self.initial_state
                .iter()
                .cloned()
                .map(|(key, _)| key)
                .collect(),
        );

        self.initial_state
            .push((REGISTER_KEY.to_string(), register.to_string()));
        self.initial_state
            .push((NODE_KIND_KEY.to_string(), self.r#type.to_string()));

        let handle = chitchat::spawn_chitchat(configuration, self.initial_state, &UdpTransport)
            .await
            .map_err(Error::Start)?;

        Ok(NodeHandle {
            node_id,
            handle: Arc::new(handle),
        })
    }
}

/// A handle of the started node.
///
/// This type permits to watch the cluster joined by the node and retrieve
/// information about the cluster nodes as well.
#[must_use]
#[derive(Clone)]
pub struct NodeHandle {
    node_id: NodeId,
    handle: Arc<ChitchatHandle>,
}

impl fmt::Debug for NodeHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NodeHandle")
            .field("node_id", &self.node_id)
            .finish()
    }
}

impl NodeHandle {
    /// Returns the node identifier.
    pub fn id(&self) -> &str {
        &self.node_id.id
    }

    /// Update the node state with the given key/value.
    ///
    /// Note that the new node state will be propagated only on the next cluster update.
    pub async fn update_state<T: ToString>(&self, key: Key<T>, value: T) {
        let chitchat = self.handle.chitchat();
        let mut chitchat = chitchat.lock().await;

        let state = chitchat.self_node_state();
        let mut register: Register = state.get(REGISTER_KEY.as_str()).unwrap_or_default().into();

        register.insert(key.to_string());

        state.set(REGISTER_KEY, register);
        state.set(key, value);
    }

    /// Shuts the node down by notifying and quitting the cluster.
    pub async fn shutdown(self) -> Result<(), Error> {
        let handle =
            Arc::try_unwrap(self.handle).map_err(|_| Error::Shutdown(anyhow!("node is in use")))?;

        handle.shutdown().await.map_err(Error::Shutdown)
    }

    /// Returns a stream over any changes in the cluster.
    pub async fn cluster_watcher(&self) -> impl Stream<Item = Vec<NodeHandle>> + 'static {
        let handle = Arc::clone(&self.handle);
        let node_id = self.node_id.clone();

        self.handle
            .chitchat()
            .lock()
            .await
            .ready_nodes_watcher()
            .map(move |nodes| {
                nodes
                    .into_iter()
                    // `ready_nodes_watcher` does not contains this node so we add it
                    // to have the complete list of live nodes.
                    .chain([node_id.clone()])
                    .map(|node_id| NodeHandle {
                        node_id,
                        handle: Arc::clone(&handle),
                    })
                    .collect()
            })
    }

    /// Returns the current list of live nodes in the cluster.
    pub async fn live_nodes(&self) -> Vec<NodeHandle> {
        self.handle
            .chitchat()
            .lock()
            .await
            .live_nodes()
            .cloned()
            // `live_nodes` does not contains this node so we add it
            // to have the complete list of live nodes.
            .chain([self.node_id.clone()])
            .map(|node_id| NodeHandle {
                node_id,
                handle: Arc::clone(&self.handle),
            })
            .collect()
    }

    /// Returns a node snapshot.
    pub async fn snapshot(&self) -> NodeSnapshot {
        self.handle
            .with_chitchat(|chitchat| {
                let state = chitchat.node_state(&self.node_id).unwrap();
                let register: Register =
                    state.get(REGISTER_KEY.as_str()).unwrap_or_default().into();

                let r#type = state
                    .get(NODE_KIND_KEY.as_str())
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(NodeType::Unknown);

                let state = register
                    .iter()
                    .filter_map(|key| {
                        state
                            .get(key)
                            .map(|value| (key.to_string(), value.to_string()))
                    })
                    .collect();

                NodeSnapshot {
                    id: self.node_id.id.to_string(),
                    address: self.node_id.gossip_public_address,
                    is_self: chitchat.self_node_id() == &self.node_id,
                    r#type,
                    state,
                }
            })
            .await
    }
}

/// A snapshot of a cluster node with its state and meta information.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeSnapshot {
    id: String,
    r#type: NodeType,
    address: SocketAddr,
    is_self: bool,
    #[serde(flatten)]
    state: HashMap<String, String>,
}

#[cfg(test)]
mod tests {
    use serde_test::{assert_tokens, Token};

    use super::*;

    #[test]
    fn it_ser_de_node_type() {
        let data = [
            (NodeType::Io, "Io"),
            (NodeType::Search, "Search"),
            (NodeType::Ingest, "Ingest"),
            (NodeType::Train, "Train"),
            (NodeType::Unknown, "Unknown"),
        ];

        for (node, variant) in data {
            assert_tokens(
                &node,
                &[Token::UnitVariant {
                    name: "NodeType",
                    variant,
                }],
            );
        }
    }

    #[tokio::test]
    #[serial_test::serial("cluster")]
    async fn it_registers_user_keys_in_state() -> Result<(), Box<dyn std::error::Error>> {
        const A_KEY: Key<u32> = Key::new("a-key");
        const B_KEY: Key<f32> = Key::new("b-key");
        const C_KEY: Key<char> = Key::new("c-key");

        let node = Node::builder()
            .on_local_network("0.0.0.0:8080".parse()?)
            .register_as(NodeType::Io)
            .insert_to_initial_state(A_KEY, 42)
            .insert_to_initial_state(B_KEY, 0.0)
            .build()?;

        let node = node.start().await?;

        let retrieve_registered_keys = || async {
            node.handle
                .chitchat()
                .lock()
                .await
                .self_node_state()
                .get(REGISTER_KEY.as_str())
                .map(Register::from)
        };

        assert_eq!(
            retrieve_registered_keys().await,
            Some(Register::with_keys(
                ["a-key".to_string(), "b-key".to_string()]
                    .into_iter()
                    .collect()
            ))
        );

        node.update_state(C_KEY, 'a').await;

        assert_eq!(
            retrieve_registered_keys().await,
            Some(Register::with_keys(
                [
                    "a-key".to_string(),
                    "b-key".to_string(),
                    "c-key".to_string()
                ]
                .into_iter()
                .collect()
            ))
        );

        Ok(())
    }

    #[tokio::test]
    #[serial_test::serial("cluster")]
    async fn it_builds_node_snapshot() -> Result<(), Box<dyn std::error::Error>> {
        const LOAD_SCORE_KEY: Key<f32> = Key::new("load-score");

        let node = Node::builder()
            .on_local_network("0.0.0.0:8080".parse()?)
            .register_as(NodeType::Io)
            .insert_to_initial_state(LOAD_SCORE_KEY, 100.0)
            .build()?;

        let node = node.start().await?;
        let snapshot = node.snapshot().await;

        assert_eq!(
            snapshot,
            NodeSnapshot {
                id: node.node_id.id,
                r#type: NodeType::Io,
                address: node.node_id.gossip_public_address,
                is_self: true,
                state: [(LOAD_SCORE_KEY.to_string(), 100.to_string())]
                    .into_iter()
                    .collect(),
            }
        );

        Ok(())
    }

    #[tokio::test]
    #[serial_test::serial("cluster")]
    async fn it_builds_cluster_snapshot() -> Result<(), Box<dyn std::error::Error>> {
        const LOAD_SCORE_KEY: Key<f32> = Key::new("load_score");

        let first_node = Node::builder()
            .on_local_network("0.0.0.0:8080".parse()?)
            .register_as(NodeType::Io)
            .insert_to_initial_state(LOAD_SCORE_KEY, 100.0)
            .build()?;

        let first_node = first_node.start().await?;

        let second_node = Node::builder()
            .on_local_network("0.0.0.0:8081".parse()?)
            .register_as(NodeType::Ingest)
            .insert_to_initial_state(LOAD_SCORE_KEY, 40.0)
            .with_seed_nodes(vec!["0.0.0.0:8080".to_string()])
            .build()?;

        let second_node = second_node.start().await?;

        tokio::time::sleep(Node::DEFAULT_UPDATE_INTERVAL * 2).await;

        let live_nodes = first_node.live_nodes().await;
        let snapshots = cluster_snapshot(live_nodes).await;

        assert_eq!(
            snapshots,
            vec![
                NodeSnapshot {
                    id: second_node.node_id.id,
                    r#type: NodeType::Ingest,
                    address: second_node.node_id.gossip_public_address,
                    is_self: false,
                    state: [(LOAD_SCORE_KEY.to_string(), 40.to_string())]
                        .into_iter()
                        .collect(),
                },
                NodeSnapshot {
                    id: first_node.node_id.id,
                    r#type: NodeType::Io,
                    address: first_node.node_id.gossip_public_address,
                    is_self: true,
                    state: [(LOAD_SCORE_KEY.to_string(), 100.to_string())]
                        .into_iter()
                        .collect(),
                },
            ]
        );

        Ok(())
    }
}
