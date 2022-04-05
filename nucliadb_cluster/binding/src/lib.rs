extern crate tokio;
#[macro_use]
extern crate log;

use std::{
    net::ToSocketAddrs,
    sync::mpsc::{Receiver, Sender},
    thread::JoinHandle,
};

use nucliadb_cluster::cluster::{Cluster as RustCluster, Member as RustMember};
use pyo3::prelude::*;
use std::sync::mpsc::channel;
use tokio::runtime::Runtime;

#[pyclass]
pub struct Cluster {
    tx_command: Sender<String>,
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
    pub node_type: char,

    /// If true, it means self.
    #[pyo3(get, set)]
    pub is_self: bool,
}

impl From<RustMember> for Member {
    fn from(m: RustMember) -> Self {
        Member {
            node_id: m.node_id.to_string(),
            listen_addr: m.listen_addr.to_string(),
            node_type: m.node_type,
            is_self: m.is_self,
        }
    }
}

#[pymethods]
impl Cluster {
    #[new]
    fn new(
        node_id: String,
        listen_addr: &str,
        node_type: char,
        timeout: u64,
        interval: u64,
    ) -> Cluster {
        let listen_addr = listen_addr
            .to_socket_addrs()
            .unwrap()
            .next()
            .expect("Error parsing Socket address for swim peers addrs");

        let (tx_command, rx_command): (Sender<String>, Receiver<String>) = channel();
        let (tx_members, rx_members) = channel();

        let t_handle = std::thread::spawn(move || {
            // Create the runtime
            let rt = Runtime::new().expect("Error creating tokio runtime");
            rt.block_on(async move {
                info!("Starting SWIM cluster");
                let cluster =
                    RustCluster::new(node_id, listen_addr, node_type, timeout, interval).unwrap();

                loop {
                    debug!("Waiting for command...");
                    let command = rx_command.recv().expect("Error in channel rx get_members");
                    let command: Vec<_> = command.split('|').collect();
                    match command[0] {
                        "get_members" => {
                            debug!("get_members");
                            tx_members
                                .send(cluster.members())
                                .expect("Error sending members through channel")
                        }
                        "add_peer_node" => {
                            debug!("add_peer_node");
                            let addr = command[1]
                                .to_socket_addrs()
                                .unwrap()
                                .next()
                                .expect("Error parsing Socket address for swim peers addrs");

                            cluster.add_peer_node(addr).await;
                        }
                        _ => info!("Invalid command sent to Cluster: {}", command[0]),
                    }
                }
            });
        });

        Cluster {
            tx_command,
            rx_members,
            _t_handle: t_handle,
        }
    }

    fn get_members(&self) -> Vec<Member> {
        self.tx_command
            .send("get_members".to_string())
            .expect("Error sending command");

        self.rx_members
            .recv()
            .expect("Error receiving members from PyCluster")
            .into_iter()
            .map(Member::from)
            .collect()
    }

    fn add_peer_node(&self, addr: &str) {
        let command = format!("add_peer_node|{}", addr);
        self.tx_command
            .send(command)
            .expect("Error sending command");
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use log::{debug, LevelFilter};

    use crate::Cluster;

    fn init() {
        let _ = env_logger::builder()
            .is_test(true)
            .filter_level(LevelFilter::Trace)
            .try_init();
    }

    #[test]
    fn swim() {
        init();
        let cluster = Cluster::new("cluster1".to_string(), "localhost:4444", 'N', 2);

        let cluster2 = Cluster::new("cluster2".to_string(), "localhost:5555", 'W', 2);
        cluster2.add_peer_node("localhost:4444");

        let mut count = 0;
        while count < 100 {
            let members = cluster.get_members();
            debug!("Members: {:?}", members);

            std::thread::sleep(Duration::from_millis(1000));
            count += 1;
        }
    }
}

/// A Python module implemented in Rust.
#[pymodule]
fn nucliadb_cluster_rust(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Cluster>()?;
    m.add_class::<Member>()?;
    Ok(())
}
