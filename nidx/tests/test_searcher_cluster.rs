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

mod common;

use common::services::NidxFixture;
use nidx::grpc_server::GrpcServer;
use nidx::searcher::{ListNodes, SyncedSearcher, grpc::SearchServer, shard_selector::ShardSelector};
use nidx_protos::SearchRequest;
use nidx_protos::nidx::nidx_searcher_client::NidxSearcherClient;
use nidx_protos::{NewShardRequest, VectorIndexConfig};

use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Mutex;
use std::{sync::Arc, time::Duration};
use tempfile::tempdir;
use tokio_util::sync::CancellationToken;
use tonic::Request;

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
    let mut shards = Vec::new();
    let mut fixture = NidxFixture::new(pool).await?;
    for _ in 0..10 {
        let response = fixture
            .api_client
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

    // Create a cluster with 3 nodes
    let nodes = Arc::new(Mutex::new(Vec::new()));
    let mut searchers = Vec::new();
    for i in 0..3 {
        let searcher_server = GrpcServer::new("localhost:0").await?;
        let searcher_port = searcher_server.port()?;
        nodes.lock().unwrap().push(format!("localhost:{searcher_port}"));
        let list_nodes = ManualListNodes(nodes.clone(), i);
        let work_dir = tempdir()?;
        let searcher = SyncedSearcher::new(fixture.settings.metadata.clone(), work_dir.path());
        let arc_list_nodes = Arc::new(list_nodes);
        let searcher_api = SearchServer::new(searcher.index_cache(), ShardSelector::new(arc_list_nodes.clone(), 1));
        searchers.push(format!("localhost:{searcher_port}"));
        let settings_copy = fixture.settings.clone();
        let shutdown = CancellationToken::new();
        tokio::task::spawn(searcher_server.serve(searcher_api.into_router(), shutdown.clone()));
        tokio::task::spawn(async move {
            searcher
                .run(
                    settings_copy.storage.as_ref().unwrap().object_store.clone(),
                    settings_copy.searcher.clone().unwrap_or_default(),
                    shutdown.clone(),
                    ShardSelector::new(arc_list_nodes, 1),
                    None,
                    None,
                )
                .await
        });
    }
    tokio::time::sleep(Duration::from_secs(1)).await;

    // All shards are accessible from all nodes
    for shard in &shards {
        for searcher in &searchers {
            NidxSearcherClient::connect(format!("http://{searcher}"))
                .await?
                .search(Request::new(SearchRequest {
                    shard: shard.to_string(),
                    result_per_page: 20,
                    body: "Hola".into(),
                    paragraph: true,
                    ..Default::default()
                }))
                .await?;
        }
    }

    // Remove one node from the cluster and wait for sync, searches should still work
    nodes.lock().unwrap().remove(2);
    tokio::time::sleep(Duration::from_secs_f32(1.2)).await;
    for shard in &shards {
        for searcher in &searchers[0..2] {
            NidxSearcherClient::connect(format!("http://{searcher}"))
                .await?
                .search(Request::new(SearchRequest {
                    shard: shard.to_string(),
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
    let mut shards = Vec::new();
    let mut fixture = NidxFixture::new(pool).await?;
    for _ in 0..30 {
        let response = fixture
            .api_client
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

    // Create a cluster with 3 nodes, set an invalid entrypoint so they cannot communicate among them
    let nodes = Arc::new(Mutex::new(Vec::new()));
    let mut searchers = Vec::new();
    for i in 0..3 {
        let searcher_server = GrpcServer::new("localhost:0").await?;
        let searcher_port = searcher_server.port()?;
        nodes.lock().unwrap().push(format!("fake_{searcher_port}"));
        let list_nodes = ManualListNodes(nodes.clone(), i);
        let work_dir = tempdir()?;
        let searcher = SyncedSearcher::new(fixture.settings.metadata.clone(), work_dir.path());
        let arc_list_nodes = Arc::new(list_nodes);
        let searcher_api = SearchServer::new(searcher.index_cache(), ShardSelector::new(arc_list_nodes.clone(), 1));
        searchers.push(format!("localhost:{searcher_port}"));
        let settings_copy = fixture.settings.clone();
        let shutdown = CancellationToken::new();
        tokio::task::spawn(searcher_server.serve(searcher_api.into_router(), shutdown.clone()));
        tokio::task::spawn(async move {
            searcher
                .run(
                    settings_copy.storage.as_ref().unwrap().object_store.clone(),
                    settings_copy.searcher.clone().unwrap_or_default(),
                    shutdown.clone(),
                    ShardSelector::new(arc_list_nodes, 1),
                    None,
                    None,
                )
                .await
        });
    }
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Each shard is only accessible from a single node
    let mut node_for_shard_original = HashMap::new();
    let mut shards_per_node = vec![0, 0, 0];
    for shard in &shards {
        let mut success = 0;
        for (i, searcher) in searchers.iter().enumerate() {
            let result = NidxSearcherClient::connect(format!("http://{searcher}"))
                .await?
                .search(Request::new(SearchRequest {
                    shard: shard.to_string(),
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
    nodes.lock().unwrap().remove(2);
    tokio::time::sleep(Duration::from_secs_f32(1.2)).await;

    let mut node_for_shard_scale_down = HashMap::new();
    for shard in &shards {
        let mut success = 0;
        for (i, searcher) in searchers[0..2].iter().enumerate() {
            let result = NidxSearcherClient::connect(format!("http://{searcher}"))
                .await?
                .search(Request::new(SearchRequest {
                    shard: shard.to_string(),
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
    nodes.lock().unwrap().push("fake_new_node".to_string());
    tokio::time::sleep(Duration::from_secs_f32(1.2)).await;

    let mut node_for_shard_scale_up = HashMap::new();
    for shard in &shards {
        let mut success = 0;
        for (i, searcher) in searchers.iter().enumerate() {
            let result = NidxSearcherClient::connect(format!("http://{searcher}"))
                .await?
                .search(Request::new(SearchRequest {
                    shard: shard.to_string(),
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
