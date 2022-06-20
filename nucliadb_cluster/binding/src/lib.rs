extern crate tokio;
#[macro_use]
extern crate log;

use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread::JoinHandle;

use anyhow;
use nucliadb_cluster::cluster::{Cluster as RustCluster, Member as RustMember, NucliaDBNodeType};
use pyo3::prelude::*;
use tokio::runtime::Runtime;

#[pyclass]
pub struct Cluster {
    tx_command: Sender<ClusterCommands>,
    rx_members: Receiver<Vec<RustMember>>,
    _t_handle: JoinHandle<()>,
}

/// A member information.
#[pyclass]
#[derive(Clone, Debug, PartialEq)]
pub struct Member {
    /// An ID that makes a member unique.
    #[pyo3(get, set)]
    pub node_id: String,

    /// Listen address.
    #[pyo3(get, set)]
    pub listen_addr: String,

    // Type of node 'l': node reader 'e': node writer 'r': reader 'w': writer
    #[pyo3(get, set)]
    pub node_type: String,

    /// If true, it means self.
    #[pyo3(get, set)]
    pub is_self: bool,
}

enum ClusterCommands {
    GetMembers,
    AddPeerNode(String),
}

impl From<RustMember> for Member {
    fn from(m: RustMember) -> Self {
        Member {
            node_id: m.node_id,
            listen_addr: m.listen_addr.to_string(),
            node_type: m.node_type.to_string(),
            is_self: m.is_self,
        }
    }
}

async fn cluster_creator(
    node_id: String,
    listen_addr: SocketAddr,
    node_type: String,
    rx_command: Receiver<ClusterCommands>,
    tx_members: Sender<Vec<RustMember>>,
) -> anyhow::Result<()> {
    info!("Starting ChitChat cluster");
    let cluster = RustCluster::new(
        node_id,
        listen_addr,
        NucliaDBNodeType::from_str(&node_type)?,
        Vec::<String>::new(),
    )
    .await?;

    loop {
        debug!("Waiting for command...");
        let command = rx_command.recv().expect("Error in channel rx get_members");
        match command {
            ClusterCommands::GetMembers => {
                debug!("GetMembers command received");
                tx_members
                    .send(cluster.members().await)
                    .expect("Error sending members through channel")
            }
            ClusterCommands::AddPeerNode(addr) => {
                debug!("AddPeerNode command receieved");
                let addr = SocketAddr::from_str(&addr)?;
                cluster
                    .add_peer_node(addr)
                    .expect("Error during send Syn message to peer");
            }
        }
    }
}

#[pymethods]
impl Cluster {
    #[new]
    fn new(node_id: String, listen_addr: &str, node_type: String) -> PyResult<Cluster> {
        let listen_addr = SocketAddr::from_str(listen_addr)?;

        let (tx_command, rx_command) = channel::<ClusterCommands>();
        let (tx_members, rx_members) = channel::<Vec<RustMember>>();

        let t_handle = std::thread::spawn(move || {
            // Create the runtime
            let rt = Runtime::new().expect("Error creating tokio runtime");
            if let Err(e) = rt.block_on(cluster_creator(
                node_id,
                listen_addr,
                node_type,
                rx_command,
                tx_members,
            )) {
                error!("{e}");
                return;
            }
        });

        Ok(Cluster {
            tx_command,
            rx_members,
            _t_handle: t_handle,
        })
    }

    fn get_members(&self) -> Vec<Member> {
        self.tx_command
            .send(ClusterCommands::GetMembers)
            .expect("Error sending command");

        self.rx_members
            .recv()
            .expect("Error receiving members from PyCluster")
            .into_iter()
            .map(Member::from)
            .collect()
    }

    fn add_peer_node(&self, addr: String) {
        self.tx_command
            .send(ClusterCommands::AddPeerNode(addr))
            .expect("Error sending command");
    }
}

/// A Python module implemented in Rust.
#[pymodule]
fn nucliadb_cluster_rust(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Cluster>()?;
    m.add_class::<Member>()?;
    Ok(())
}
