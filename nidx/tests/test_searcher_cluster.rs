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
use nidx::searcher::{grpc::SearchServer, shard_selector::ShardSelector, ListNodes, SyncedSearcher};
use nidx_protos::nidx::nidx_searcher_client::NidxSearcherClient;
use nidx_protos::SearchRequest;
use nidx_protos::{NewShardRequest, VectorIndexConfig};

use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Mutex;
use std::{sync::Arc, time::Duration};
use tempfile::tempdir;
use tokio_util::sync::CancellationToken;
use tonic::Request;

struct ManualListNodes(Arc<Mutex<Vec<String>>>, String);
impl ListNodes for ManualListNodes {
    fn list_nodes(&self) -> Vec<String> {
        self.0.lock().unwrap().clone()
    }

    fn this_node(&self) -> String {
        self.1.clone()
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
    for _ in 0..3 {
        let searcher_server = GrpcServer::new("localhost:0").await?;
        let searcher_port = searcher_server.port()?;
        nodes.lock().unwrap().push(format!("localhost:{searcher_port}"));
        let list_nodes = ManualListNodes(nodes.clone(), format!("localhost:{searcher_port}"));
        let work_dir = tempdir()?;
        let searcher = SyncedSearcher::new(fixture.settings.metadata.clone(), work_dir.path());
        let arc_list_nodes = Arc::new(list_nodes);
        let searcher_api = SearchServer::new(searcher.index_cache(), ShardSelector::new(arc_list_nodes.clone(), 1));
        searchers.push(format!("localhost:{searcher_port}"));
        let settings_copy = fixture.settings.clone();
        let shutdown = CancellationToken::new();
        tokio::task::spawn(searcher_server.serve(searcher_api.into_service(), shutdown.clone()));
        tokio::task::spawn(async move {
            searcher
                .run(
                    settings_copy.storage.as_ref().unwrap().object_store.clone(),
                    settings_copy.searcher.clone().unwrap_or_default(),
                    shutdown.clone(),
                    ShardSelector::new(arc_list_nodes, 1),
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
                    ..Default::default()
                }))
                .await?;
        }
    }

    // Remove one node from the cluster, we expect to see some failures
    nodes.lock().unwrap().remove(2);
    let mut failures = 0;
    for shard in &shards {
        for searcher in &searchers[0..2] {
            let result = NidxSearcherClient::connect(format!("http://{searcher}"))
                .await?
                .search(Request::new(SearchRequest {
                    shard: shard.to_string(),
                    ..Default::default()
                }))
                .await;
            if result.is_err() {
                failures += 1;
            }
        }
    }
    assert!(failures > 0);

    // Wait for sync, now all searches should work
    tokio::time::sleep(Duration::from_secs(1)).await;
    for shard in &shards {
        for searcher in &searchers[0..2] {
            NidxSearcherClient::connect(format!("http://{searcher}"))
                .await?
                .search(Request::new(SearchRequest {
                    shard: shard.to_string(),
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

    // Create a cluster with 3 nodes, set an invalid entrypoint so they cannot communicate among them
    let nodes = Arc::new(Mutex::new(Vec::new()));
    let mut searchers = Vec::new();
    for _ in 0..3 {
        let searcher_server = GrpcServer::new("localhost:0").await?;
        let searcher_port = searcher_server.port()?;
        nodes.lock().unwrap().push(format!("fake_{searcher_port}"));
        let list_nodes = ManualListNodes(nodes.clone(), format!("fake_{searcher_port}"));
        let work_dir = tempdir()?;
        let searcher = SyncedSearcher::new(fixture.settings.metadata.clone(), work_dir.path());
        let arc_list_nodes = Arc::new(list_nodes);
        let searcher_api = SearchServer::new(searcher.index_cache(), ShardSelector::new(arc_list_nodes.clone(), 1));
        searchers.push(format!("localhost:{searcher_port}"));
        let settings_copy = fixture.settings.clone();
        let shutdown = CancellationToken::new();
        tokio::task::spawn(searcher_server.serve(searcher_api.into_service(), shutdown.clone()));
        tokio::task::spawn(async move {
            searcher
                .run(
                    settings_copy.storage.as_ref().unwrap().object_store.clone(),
                    settings_copy.searcher.clone().unwrap_or_default(),
                    shutdown.clone(),
                    ShardSelector::new(arc_list_nodes, 1),
                    None,
                )
                .await
        });
    }
    tokio::time::sleep(Duration::from_secs(1)).await;

    // All shards are accessible from a single node
    for shard in &shards {
        let mut success = 0;
        for searcher in &searchers {
            let result = NidxSearcherClient::connect(format!("http://{searcher}"))
                .await?
                .search(Request::new(SearchRequest {
                    shard: shard.to_string(),
                    ..Default::default()
                }))
                .await;
            if result.is_ok() {
                success += 1;
            }
        }
        assert_eq!(success, 1);
    }

    // Remove one node from the cluster, we expect shards to rebalance among the remaining nodes
    let removed_node = nodes.lock().unwrap().remove(2);
    tokio::time::sleep(Duration::from_secs_f32(1.2)).await;

    for shard in &shards {
        let mut success = 0;
        for searcher in &searchers[0..2] {
            let result = NidxSearcherClient::connect(format!("http://{searcher}"))
                .await?
                .search(Request::new(SearchRequest {
                    shard: shard.to_string(),
                    ..Default::default()
                }))
                .await;
            if result.is_ok() {
                success += 1;
            }
        }
        assert_eq!(success, 1);
    }

    // Add a new node again, shards should split up again
    nodes.lock().unwrap().push(removed_node);
    tokio::time::sleep(Duration::from_secs_f32(1.2)).await;

    for shard in &shards {
        let mut success = 0;
        for searcher in &searchers {
            let result = NidxSearcherClient::connect(format!("http://{searcher}"))
                .await?
                .search(Request::new(SearchRequest {
                    shard: shard.to_string(),
                    ..Default::default()
                }))
                .await;
            if result.is_ok() {
                success += 1;
            }
        }
        assert_eq!(success, 1);
    }

    Ok(())
}
