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

use crate::common::services::NidxFixture;
use nidx::Settings;
use nidx::grpc_server::GrpcServer;
use nidx::searcher::{ListNodes, SyncedSearcher, grpc::SearchServer, shard_selector::ShardSelector};
use nidx::settings::SearcherSettings;
use nidx_protos::SearchRequest;
use nidx_protos::nidx::nidx_searcher_client::NidxSearcherClient;
use nidx_protos::nidx_api_client::NidxApiClient;
use nidx_protos::{NewShardRequest, VectorIndexConfig};

use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Mutex;
use std::{sync::Arc, time::Duration};
use tempfile::tempdir;
use tokio_util::sync::CancellationToken;
use tonic::Request;
use tonic::transport::Channel;
use uuid::Uuid;

#[derive(Debug)]
struct ManualListNodes(Arc<Mutex<Vec<String>>>, usize);
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

#[sqlx::test]
async fn test_search_cluster_all_shards_accessible(pool: PgPool) -> anyhow::Result<()> {
    let mut fixture = NidxFixture::new(pool).await?;
    let shards = create_shards(&mut fixture.api_client, 10).await?;

    // Create a cluster with 3 nodes
    let mut cluster = SearcherCluster::new(
        fixture.settings.clone(),
        Topology {
            nodes: 3,
            shard_replication: 1,
            network: vec![NetworkAvailability::Connected; 3],
        },
    )
    .await?;

    // All shards are accessible from all nodes
    for shard in &shards {
        for searcher in &cluster.searchers {
            let searcher = searcher.as_ref().expect("we don't have any fake node configured");
            NidxSearcherClient::connect(format!("http://{}", searcher.address()))
                .await?
                .search(Request::new(SearchRequest {
                    shard_ids: vec![shard.to_string()],
                    result_per_page: 20,
                    body: "Hola".into(),
                    paragraph: true,
                    ..Default::default()
                }))
                .await?;
        }
    }

    // Remove one node from the cluster and wait for sync, searches should still work
    let _ = cluster.shrink();
    assert_eq!(cluster.size(), 2);
    tokio::time::sleep(cluster.refresh_interval()).await;
    for shard in &shards {
        for searcher in &cluster.searchers {
            let searcher = searcher.as_ref().expect("we don't have any fake node configured");
            NidxSearcherClient::connect(format!("http://{}", searcher.address()))
                .await?
                .search(Request::new(SearchRequest {
                    shard_ids: vec![shard.to_string()],
                    result_per_page: 20,
                    body: "Hola".into(),
                    paragraph: true,
                    ..Default::default()
                }))
                .await?;
        }
    }

    Ok(())
}

#[sqlx::test]
async fn test_search_cluster_shard_distribution(pool: PgPool) -> anyhow::Result<()> {
    let mut fixture = NidxFixture::new(pool).await?;
    let shards = create_shards(&mut fixture.api_client, 30).await?;

    // Create a cluster with 3 nodes, set an invalid entrypoint so they cannot communicate among them
    let mut cluster = SearcherCluster::new(
        fixture.settings.clone(),
        Topology {
            nodes: 3,
            shard_replication: 1,
            network: vec![NetworkAvailability::Isolated; 3],
        },
    )
    .await?;

    // Each shard is only accessible from a single node
    let mut node_for_shard_original = HashMap::new();
    let mut shards_per_node = vec![0, 0, 0];
    for shard in &shards {
        let mut success = 0;
        for (i, searcher) in cluster.searchers.iter().enumerate() {
            let searcher = searcher.as_ref().expect("we don't have any fake node configured");
            let result = NidxSearcherClient::connect(format!("http://{}", searcher.address()))
                .await?
                .search(Request::new(SearchRequest {
                    shard_ids: vec![shard.to_string()],
                    result_per_page: 20,
                    body: "Hola".into(),
                    paragraph: true,
                    ..Default::default()
                }))
                .await;
            if result.is_ok() {
                shards_per_node[i] += 1;
                node_for_shard_original.insert(shard, i);
                success += 1;
            }
        }
        assert_eq!(success, 1);
    }
    for shards_in_node in shards_per_node {
        assert!(shards_in_node > 0);
    }

    // Remove one node from the cluster, we expect shards to rebalance among the remaining nodes
    let node_2 = cluster.shrink();
    assert_eq!(cluster.size(), 2);
    tokio::time::sleep(cluster.refresh_interval()).await;

    let mut node_for_shard_scale_down = HashMap::new();
    for shard in &shards {
        let mut success = 0;
        for (i, searcher) in cluster.searchers[0..2].iter().enumerate() {
            let searcher = searcher.as_ref().expect("we don't have any fake node configured");
            let result = NidxSearcherClient::connect(format!("http://{}", searcher.address()))
                .await?
                .search(Request::new(SearchRequest {
                    shard_ids: vec![shard.to_string()],
                    result_per_page: 20,
                    body: "Hola".into(),
                    paragraph: true,
                    ..Default::default()
                }))
                .await;
            if result.is_ok() {
                node_for_shard_scale_down.insert(shard, i);
                success += 1;
            }
        }
        assert_eq!(success, 1);
    }

    // Only the shards from node 2 should have moved
    for (shard, &old_node) in &node_for_shard_original {
        let new_node = node_for_shard_scale_down[shard];
        if old_node == 2 {
            assert_ne!(new_node, 2, "All shards from node 2 should have moved");
        } else {
            assert_eq!(new_node, old_node, "No shards should be moved between nodes 0 and 1");
        }
    }

    // Add a new node again, shards should split up again
    // This node will have a different ID, so shard split
    // should be different than originally
    cluster.grow_fake("fake_new_node".to_string());
    assert_eq!(cluster.size(), 3);
    tokio::time::sleep(cluster.refresh_interval()).await;

    let mut node_for_shard_scale_up = HashMap::new();
    for shard in &shards {
        let mut success = 0;
        for (i, searcher) in cluster.searchers[0..2].iter().chain([&node_2]).enumerate() {
            let searcher = searcher.as_ref().expect("fake node is 2");
            let result = NidxSearcherClient::connect(format!("http://{}", searcher.address()))
                .await?
                .search(Request::new(SearchRequest {
                    shard_ids: vec![shard.to_string()],
                    result_per_page: 20,
                    body: "Hola".into(),
                    paragraph: true,
                    ..Default::default()
                }))
                .await;
            if result.is_ok() {
                node_for_shard_scale_up.insert(shard, i);
                success += 1;
            }
        }
        assert_eq!(success, 1);
    }

    // Some shards should have moved to node 2, but not between 0 and 1
    let mut moved = 0;
    for (shard, &old_node) in &node_for_shard_scale_down {
        let new_node = node_for_shard_scale_up[shard];
        if new_node == 2 {
            moved += 1;
        } else {
            assert_eq!(new_node, old_node, "No shards should be moved between nodes 0 and 1");
        }
    }
    assert!(moved > 0, "Some shards should be moved to the new node after scale up");

    // Shard distribution should be different that originally
    let mut different = 0;
    for (shard, &old_node) in &node_for_shard_original {
        let new_node = node_for_shard_scale_up[shard];
        if new_node != old_node {
            different += 1;
        }
    }
    assert!(
        different > 0,
        "Shard distribution after scale down + up should be different than originally"
    );

    Ok(())
}

#[sqlx::test]
async fn test_search_cluster_shards_not_accessible(pool: PgPool) -> anyhow::Result<()> {
    // Request a shard that does not exist, we should get a NotFound error
    // This test makes sure we don't get the searchers stuck in a loop asking each other
    // for the shard.
    let fixture = NidxFixture::new(pool).await?;

    // Create a cluster with 3 nodes and 2 replicas for each shard
    let cluster = SearcherCluster::new(
        fixture.settings.clone(),
        Topology {
            nodes: 3,
            shard_replication: 2,
            network: vec![NetworkAvailability::Connected; 3],
        },
    )
    .await?;

    // Same behaviour from all nodes
    let fake_uuid = Uuid::new_v4();
    for searcher in &cluster.searchers {
        let searcher = searcher.as_ref().expect("we don't have any fake node configured");
        let mut request = Request::new(SearchRequest {
            shard_ids: vec![fake_uuid.to_string()],
            result_per_page: 20,
            body: "Hola".into(),
            paragraph: true,
            ..Default::default()
        });
        request.set_timeout(Duration::from_secs(1));

        let response = NidxSearcherClient::connect(format!("http://{}", searcher.address()))
            .await?
            .search(request)
            .await;

        let Err(err) = response else {
            panic!("Expected error response")
        };
        assert_eq!(
            err.code(),
            tonic::Code::NotFound,
            "Expected shard NotFound. Cancelled error means timeout which means something went wrong"
        );
    }

    Ok(())
}

struct SearcherCluster {
    pub searchers: Vec<Option<Searcher>>,
    nodes: Arc<Mutex<Vec<String>>>,
    settings: Settings,
    shard_replication: usize,
}

type SearcherId = usize;

// Each searcher has it's own view on the cluster, not shared with the rest
#[derive(Debug)]
struct Searcher {
    port: u16,
    shutdown: CancellationToken,
}

struct Topology {
    // Number of nodes in the cluster
    nodes: usize,

    // Number of nodes that have a replica of a shard
    shard_replication: usize,

    // When defined, the network describes which nodes are reachable from every
    // node in the cluster. By default, everyone sees everyone.
    //
    // Changes on the network can be used to simluate network partitions, nodes
    // being down...
    network: Vec<NetworkAvailability>,
}

#[derive(Clone, Debug)]
enum NetworkAvailability {
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
    pub async fn shutdown(&mut self, id: SearcherId) {
        todo!()
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

async fn create_shards(api_client: &mut NidxApiClient<Channel>, count: usize) -> anyhow::Result<Vec<Uuid>> {
    let mut shards = Vec::with_capacity(count);
    for _ in 0..count {
        let response = api_client
            .new_shard(Request::new(NewShardRequest {
                kbid: "aabbccddeeff11223344556677889900".to_string(),
                vectorsets_configs: HashMap::from([(
                    "english".to_string(),
                    VectorIndexConfig {
                        vector_dimension: Some(3),
                        ..Default::default()
                    },
                )]),
                ..Default::default()
            }))
            .await?;
        shards.push(uuid::Uuid::parse_str(&response.into_inner().id).unwrap());
    }
    Ok(shards)
}
