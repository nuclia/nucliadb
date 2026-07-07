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

use nidx_protos::SearchRequest;
use nidx_protos::nidx::nidx_searcher_client::NidxSearcherClient;

use sqlx::PgPool;
use std::collections::HashMap;
use std::time::Duration;
use tonic::Request;
use uuid::Uuid;

use crate::common::cluster::{NetworkAvailability, SearcherCluster, Topology};
use crate::common::services::NidxFixture;
use crate::common::utils::create_shards;

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
