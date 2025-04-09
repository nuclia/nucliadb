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

use futures::TryStreamExt as _;
use k8s_openapi::api::core::v1::Pod;
use std::{
    collections::HashSet,
    future::Future,
    hash::{DefaultHasher, Hash as _, Hasher as _},
    pin::pin,
    sync::Arc,
};
use tokio_util::sync::CancellationToken;
use tracing::*;
use uuid::Uuid;

/// Trait for listing the active nodes in a cluster and identifying ourselves
pub trait ListNodes: Send + Sync {
    fn list_nodes(&self) -> Vec<String>;
    fn this_node(&self) -> String;
}

/// Implementation of `ListNodes` for single node clusters, e.g: standalone/binding
#[derive(Clone)]
pub struct SingleNodeCluster;
impl ListNodes for SingleNodeCluster {
    fn list_nodes(&self) -> Vec<String> {
        vec![self.this_node()]
    }

    fn this_node(&self) -> String {
        "single".to_string()
    }
}

/// Implementation of `ListNodes` for kubernetes clusters
#[derive(Clone)]
pub struct KubernetesCluster {
    pods: kube::runtime::reflector::store::Store<Pod>,
    my_address: String,
}

impl KubernetesCluster {
    pub async fn new_cluster_and_task(
        shutdown: CancellationToken,
    ) -> anyhow::Result<(Self, impl Future<Output = anyhow::Result<()>>)> {
        let my_address = Self::pod_address(&std::env::var("POD_IP")?);

        // Create a store that keeps an updated view of `nidx-searcher` pods
        let client = kube::Client::try_default().await?;
        let pods: kube::Api<Pod> = kube::Api::namespaced(client, "nucliadb");
        let watcher_config = kube::runtime::watcher::Config::default().labels("app=nidx-searcher");
        let (store_reader, store_writer) = kube::runtime::reflector::store();
        let stream = kube::runtime::reflector::reflector(store_writer, kube::runtime::watcher(pods, watcher_config));

        let task_reader = store_reader.clone();
        let task = async move {
            // Read the stream continuously in order to keep the store updated
            let mut stream = pin!(stream);
            let mut prev_pods = HashSet::new();
            loop {
                tokio::select! {
                    result = stream.try_next() => {
                        if let Err(e) = result {
                            return Err(e.into())
                        }
                        let new_pods = task_reader
                            .state()
                            .iter()
                            .filter(|pod| Self::pod_ready(pod))
                            .filter_map(|pod| pod.status.as_ref().map(|s| s.pod_ip.as_ref().map(Self::pod_address)))
                            .flatten()
                            .collect();
                        if new_pods != prev_pods {
                            info!(?prev_pods, ?new_pods, "Kubernetes detected cluster topology change");
                            prev_pods = new_pods;
                        }
                    }
                    _ = shutdown.cancelled() => { return Ok(()) }
                }
            }
        };

        Ok((
            Self {
                pods: store_reader,
                my_address,
            },
            task,
        ))
    }

    fn pod_ready(p: &Pod) -> bool {
        if p.metadata.deletion_timestamp.is_some() {
            // Pod is terminating
            return false;
        }
        let Some(status) = p.status.as_ref() else {
            // Pod doesn't have status
            return false;
        };
        if status.phase.as_deref() != Some("Running") {
            // Pod is not running
            return false;
        }
        let Some(container_statuses) = status.container_statuses.as_ref() else {
            // Pod doesn't have container status
            return false;
        };
        if container_statuses.iter().any(|cs| !cs.ready) {
            // Any container is not ready
            return false;
        }
        // Any condition exists and says it's not ready
        if let Some(conds) = &p.status.as_ref().unwrap().conditions {
            if conds.iter().any(|c| c.type_ == "Ready" && c.status == "False") {
                return false;
            }
        }

        true
    }

    fn pod_address(ip: &String) -> String {
        format!("{ip}:10001")
    }

    pub async fn wait_until_ready(&self) -> anyhow::Result<()> {
        Ok(self.pods.wait_until_ready().await?)
    }
}
impl ListNodes for KubernetesCluster {
    fn list_nodes(&self) -> Vec<String> {
        let mut ready_pods: Vec<_> = self
            .pods
            .state()
            .iter()
            .filter(|pod| Self::pod_ready(pod))
            .filter_map(|pod| pod.status.as_ref().map(|s| s.pod_ip.as_ref().map(Self::pod_address)))
            .flatten()
            .collect();

        // Always include ourselves, even if we are not ready
        if !ready_pods.contains(&self.my_address) {
            ready_pods.push(self.my_address.clone());
        }

        ready_pods
    }

    fn this_node(&self) -> String {
        self.my_address.clone()
    }
}

/// Helper to sort (shard, node) pairs based on rendezvous hashing score
#[derive(Hash)]
struct Scored<'a>(&'a Uuid, &'a String);
impl Scored<'_> {
    fn score(&self) -> u64 {
        let mut s = DefaultHasher::new();
        self.hash(&mut s);
        s.finish()
    }
}

#[derive(Debug)]
pub enum SearcherNode {
    This,
    Remote(String),
}

/// Tool to select which shards should be present in this node
pub struct ShardSelector {
    nodes: Arc<dyn ListNodes>,
    num_replicas: usize,
}

impl ShardSelector {
    pub fn new(nodes: Arc<dyn ListNodes>, num_replicas: usize) -> Self {
        Self { nodes, num_replicas }
    }

    pub fn new_single() -> Self {
        Self {
            nodes: Arc::new(SingleNodeCluster),
            num_replicas: 1,
        }
    }

    /// List the shards that should be stored in this node
    pub fn select_shards(&self, all_shards: Vec<Uuid>) -> Vec<Uuid> {
        let this_node = self.nodes.this_node();
        let nodes = self.nodes.list_nodes();
        all_shards
            .into_iter()
            .filter(move |shard| self._nodes_for_shard(nodes.clone(), shard).contains(&this_node))
            .collect()
    }

    /// List the nodes that contain a shard. If the local node is in the list, it will always be returned first
    pub fn nodes_for_shard(&self, shard: &Uuid) -> Vec<SearcherNode> {
        let nodes = self._nodes_for_shard(self.nodes.list_nodes(), shard);
        // See if some of this nodes are me, and put them first
        let mut searcher_nodes = Vec::new();
        let mut has_this_node = false;
        for node in nodes {
            if node == self.nodes.this_node() {
                has_this_node = true;
            } else {
                searcher_nodes.push(SearcherNode::Remote(node));
            }
        }
        if has_this_node {
            searcher_nodes.insert(0, SearcherNode::This);
        }
        searcher_nodes
    }

    fn _nodes_for_shard(&self, mut nodes: Vec<String>, shard: &Uuid) -> Vec<String> {
        nodes.sort_by_cached_key(|s| Scored(shard, s).score());
        nodes.truncate(self.num_replicas);

        nodes
    }
}
