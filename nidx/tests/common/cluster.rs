// Copyright 2021 Bosutech XXI S.L.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

use nidx::Settings;
use nidx::grpc_server::GrpcServer;
use nidx::searcher::{ListNodes, SyncedSearcher, grpc::SearchServer, shard_selector::ShardSelector};
use nidx::settings::SearcherSettings;

use std::sync::Mutex;
use std::{sync::Arc, time::Duration};
use tempfile::tempdir;
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
pub struct ManualListNodes(Arc<Mutex<Vec<String>>>, usize);
impl ListNodes for ManualListNodes {
    fn list_nodes(&self) -> Vec<String> {
        self.0.lock().unwrap().clone()
    }

    fn this_node(&self) -> String {
        self.list_nodes()
            .get(self.1)
            .cloned()
            .unwrap_or("out_of_cluster".to_string())
    }
}

pub struct SearcherCluster {
    pub searchers: Vec<Option<Searcher>>,
    nodes: Arc<Mutex<Vec<String>>>,
    settings: Settings,
    shard_replication: usize,
}

pub type SearcherId = usize;

// Each searcher has it's own view on the cluster, not shared with the rest
#[derive(Debug)]
pub struct Searcher {
    port: u16,
    shutdown: CancellationToken,
}

pub struct Topology {
    // Number of nodes in the cluster
    pub nodes: usize,

    // Number of nodes that have a replica of a shard
    pub shard_replication: usize,

    // When defined, the network describes which nodes are reachable from every
    // node in the cluster. By default, everyone sees everyone.
    //
    // Changes on the network can be used to simluate network partitions, nodes
    // being down...
    pub network: Vec<NetworkAvailability>,
}

#[derive(Clone, Debug)]
pub enum NetworkAvailability {
    Connected,
    Isolated,
}

impl SearcherCluster {
    pub async fn new(settings: Settings, topology: Topology) -> anyhow::Result<Self> {
        assert_eq!(
            topology.nodes,
            topology.network.len(),
            "number of nodes and network configurations mismatch"
        );

        let mut cluster = Self {
            searchers: Vec::new(),
            nodes: Arc::new(Mutex::new(Vec::new())),
            settings,
            shard_replication: topology.shard_replication,
        };

        for availability in topology.network {
            cluster.grow(availability).await?;
        }
        tokio::time::sleep(cluster.refresh_interval()).await;

        Ok(cluster)
    }

    pub fn size(&self) -> usize {
        self.searchers.len()
    }

    pub async fn grow(&mut self, availability: NetworkAvailability) -> anyhow::Result<()> {
        let server = GrpcServer::new("localhost:0").await?;
        let port = server.port()?;

        let work_dir = tempdir()?;
        let searcher = SyncedSearcher::new(self.settings.metadata.clone(), work_dir.path());

        let address = match availability {
            NetworkAvailability::Connected => format!("localhost:{port}"),
            NetworkAvailability::Isolated => format!("unreachable:{port}"),
        };
        let node_id = {
            let mut nodes = self.nodes.lock().unwrap();
            nodes.push(address);
            nodes.len() - 1
        };
        let list_nodes = Arc::new(ManualListNodes(self.nodes.clone(), node_id));

        let searcher_api = SearchServer::new(
            self.settings.metadata.clone(),
            searcher.index_cache(),
            ShardSelector::new(Arc::clone(&list_nodes) as Arc<dyn ListNodes>, self.shard_replication),
        );
        let shutdown = CancellationToken::new();
        {
            let settings = self.settings.clone();
            let shutdown = shutdown.clone();
            let num_replicas = self.shard_replication;
            tokio::task::spawn(server.serve(searcher_api.into_router(), shutdown.clone()));
            tokio::task::spawn(async move {
                searcher
                    .run(
                        settings.storage.as_ref().unwrap().object_store.clone(),
                        settings.searcher.clone().unwrap_or_default(),
                        shutdown,
                        ShardSelector::new(Arc::clone(&list_nodes) as Arc<dyn ListNodes>, num_replicas),
                        None,
                        None,
                    )
                    .await
            });
        }

        self.searchers.push(Some(Searcher { port, shutdown }));
        Ok(())
    }

    /// Grow the cluster adding a fake node. This will cause shards to be repartitioned with the new
    /// node, but it's not accessible (there's no gRPC server)
    pub fn grow_fake(&mut self, name: String) {
        self.nodes.lock().unwrap().push(name);
        self.searchers.push(None);
    }

    /// Gracefully shutdown a searcher node. This will keep it part of the cluster but unavailable
    /// to the rest. This operation can't be reversed
    pub fn shutdown_node(&mut self, id: SearcherId) {
        self.searchers
            .get(id)
            .unwrap()
            .as_ref()
            .expect("can't shutdown a fake node")
            .shutdown
            .cancel();
    }

    /// Shrink the cluster by scaling down. This will remove a node from the cluster and shards will
    /// be repartitioned across the rest.
    pub fn shrink(&mut self) -> Option<Searcher> {
        assert!(!self.searchers.is_empty(), "can't shrink an empty cluster");
        let searcher = self.searchers.pop().unwrap();
        self.nodes.lock().unwrap().pop().unwrap();
        searcher
    }

    pub fn refresh_interval(&self) -> Duration {
        let refresh_interval = self.settings.searcher.as_ref().map_or_else(
            || SearcherSettings::default().metadata_refresh_interval,
            |s| s.metadata_refresh_interval,
        );
        Duration::from_secs_f32(refresh_interval + 0.1)
    }
}

impl Searcher {
    pub fn address(&self) -> String {
        format!("localhost:{}", self.port)
    }
}
