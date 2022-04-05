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

extern crate tokio;
#[macro_use]
extern crate log;

use std::{
    net::ToSocketAddrs,
    sync::mpsc::{Receiver, Sender},
    thread::JoinHandle,
};

use nucliadb_node::cluster::{Cluster as RustCluster, Member as RustMember};
use pyo3::prelude::*;
use std::sync::mpsc::channel;
use tokio::runtime::Runtime;

#[pyclass]
pub struct Node {
    reader: tokio::task::JoinHandle,
    writer: Receiver<Vec<RustMember>>,
    _t_handle: JoinHandle<()>,
}

#[pymethods]
impl Node {
    #[new]
    fn new(
        node_id: String,
        listen_addr: &str,
        node_type: char,
        timeout: u64,
        interval: u64,
    ) -> Node {
        let node_reader_service = NodeReaderService::new();

        std::fs::create_dir_all(Configuration::shards_path())?;
        if !Configuration::lazy_loading() {
            node_reader_service.load_shards().await?;
        }
        let reader_task = tokio::spawn(async move {
            let addr = Configuration::reader_listen_address();
            info!("Reader listening for gRPC requests at: {:?}", addr);
            let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
            health_reporter
                .set_serving::<NodeReaderServer<NodeReaderService>>()
                .await;
            Server::builder()
                .add_service(health_service)
                .add_service(NodeReaderServer::new(node_reader_service))
                .serve(addr)
                .await
                .expect("Error starting gRPC writer");
        });
        info!("Bootstrap complete in: {:?}", start_bootstrap.elapsed());

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
