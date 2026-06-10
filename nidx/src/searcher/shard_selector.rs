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

use futures::TryStreamExt as _;
use k8s_openapi::api::core::v1::Pod;
use std::{
    collections::HashSet,
    future::Future,
    hash::{DefaultHasher, Hash as _, Hasher as _},
    pin::pin,
    sync::Arc,
    time::Duration,
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
            let mut retries = 0;
            loop {
                tokio::select! {
                    result = stream.try_next() => {
                        if let Err(e) = result {
                            warn!(?e, "Error updating kubernetes cluster");
                            if retries > 5 {
                                return Err(e.into())
                            } else {
                                retries += 1;
                                tokio::time::sleep(Duration::from_secs(5)).await;
                                continue;
                            }
                        } else if retries > 0 {
                            retries -= 1;
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
        if let Some(conds) = &p.status.as_ref().unwrap().conditions
            && conds.iter().any(|c| c.type_ == "Ready" && c.status == "False")
        {
            return false;
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

    /// List the nodes that contain a shard in order of preference.
    /// This way we preferentially send all requests for a shard to the same node to improve caching.
    pub fn nodes_for_shard(&self, shard: &Uuid) -> Vec<SearcherNode> {
        let nodes = self._nodes_for_shard(self.nodes.list_nodes(), shard);

        nodes
            .into_iter()
            .map(|n| {
                if n == self.nodes.this_node() {
                    SearcherNode::This
                } else {
                    SearcherNode::Remote(n)
                }
            })
            .collect()
    }

    fn _nodes_for_shard(&self, mut nodes: Vec<String>, shard: &Uuid) -> Vec<String> {
        nodes.sort_by_cached_key(|s| Scored(shard, s).score());
        nodes.truncate(self.num_replicas);

        nodes
    }
}
