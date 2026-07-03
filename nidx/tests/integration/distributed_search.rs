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

//! Distributed (multi-shard) Search() tests
//!
//! This module tests how a nidx-searcher cluster handles Search to a batch of shards. As queries
//! are distributed across searchers, these tests try to emulate failure situations in the cluster.
//!

use itertools::Itertools;
use nidx_protos::SearchRequest;
use nidx_protos::nidx::nidx_searcher_client::NidxSearcherClient;

use sqlx::PgPool;
use tonic::{Code, Request};

use crate::common::cluster::{NetworkAvailability, SearcherCluster, Topology};
use crate::common::services::NidxFixture;
use crate::common::utils::create_shards;

#[sqlx::test]
async fn test_searchers_with_1_shard_replica(pool: PgPool) -> anyhow::Result<()> {
    let mut fixture = NidxFixture::new(pool).await?;
    let shards: Vec<String> = create_shards(&mut fixture.api_client, 10)
        .await?
        .iter()
        .map(|s| s.to_string())
        .sorted()
        .collect();

    // Create a cluster with 3 nodes and 1 replica
    let mut cluster = SearcherCluster::new(
        fixture.settings.clone(),
        Topology {
            nodes: 3,
            shard_replication: 1,
            network: vec![NetworkAvailability::Connected; 3],
        },
    )
    .await?;

    // Each searcher has 1/3 of the total shards. As everyone is available, the queried searcher
    // will request the other two for the missing 2/3 of shards
    for searcher in &cluster.searchers {
        let searcher = searcher.as_ref().expect("we don't have any fake node configured");
        let response = NidxSearcherClient::connect(format!("http://{}", searcher.address()))
            .await?
            .search(Request::new(SearchRequest {
                shard_ids: shards.clone(),
                result_per_page: 20,
                body: "Hola".into(),
                paragraph: true,
                ..Default::default()
            }))
            .await?
            .into_inner();
        assert_eq!(response.shard_ids.into_iter().sorted().collect::<Vec<_>>(), shards);
    }

    // Shutdown one of the searchers so his 1/3 of shards is not available anymore
    cluster.shutdown_node(2);

    // Each searcher has 1/3 of the total shards. As everyone is available, the queried searcher
    // will request the other two for the missing 2/3 of shards
    for searcher in &cluster.searchers[0..2] {
        let searcher = searcher.as_ref().expect("we don't have any fake node configured");
        let response = NidxSearcherClient::connect(format!("http://{}", searcher.address()))
            .await?
            .search(Request::new(SearchRequest {
                shard_ids: shards.clone(),
                result_per_page: 20,
                body: "Hola".into(),
                paragraph: true,
                ..Default::default()
            }))
            .await;
        assert!(response.is_err());
        let error = response.unwrap_err();
        assert_eq!(error.code(), Code::Internal);
        assert!(
            error
                .message()
                .contains("Error in search, exhausted all available nodes for shard"),
        );
    }

    Ok(())
}

#[sqlx::test]
async fn test_searchers_with_2_shard_replica(pool: PgPool) -> anyhow::Result<()> {
    let mut fixture = NidxFixture::new(pool).await?;
    let shards: Vec<String> = create_shards(&mut fixture.api_client, 10)
        .await?
        .iter()
        .map(|s| s.to_string())
        .sorted()
        .collect();

    // Create a cluster with 3 nodes and 2 replica
    let mut cluster = SearcherCluster::new(
        fixture.settings.clone(),
        Topology {
            nodes: 3,
            shard_replication: 2,
            network: vec![NetworkAvailability::Connected; 3],
        },
    )
    .await?;

    // Each searcher has 2/3 of the total shards. As everyone is available, the queried searcher
    // will request the other two for the missing 1/3 of shards
    for searcher in &cluster.searchers {
        let searcher = searcher.as_ref().expect("we don't have any fake node configured");
        let response = NidxSearcherClient::connect(format!("http://{}", searcher.address()))
            .await?
            .search(Request::new(SearchRequest {
                shard_ids: shards.clone(),
                result_per_page: 20,
                body: "Hola".into(),
                paragraph: true,
                ..Default::default()
            }))
            .await?
            .into_inner();
        assert_eq!(response.shard_ids.into_iter().sorted().collect::<Vec<_>>(), shards);
    }

    // Shutdown one of the searchers so his 2/3 of shards is not available anymore
    cluster.shutdown_node(2);

    // As each searcher has 2/3 of the total shards, 2 is enough to still answer for all shards.
    for searcher in &cluster.searchers[0..2] {
        let searcher = searcher.as_ref().expect("we don't have any fake node configured");
        let response = NidxSearcherClient::connect(format!("http://{}", searcher.address()))
            .await?
            .search(Request::new(SearchRequest {
                shard_ids: shards.clone(),
                result_per_page: 20,
                body: "Hola".into(),
                paragraph: true,
                ..Default::default()
            }))
            .await?
            .into_inner();
        assert_eq!(response.shard_ids.into_iter().sorted().collect::<Vec<_>>(), shards);
    }

    // If we make a second node unavilable, we won't be able to answer
    cluster.shutdown_node(1);

    let searcher = cluster.searchers[0]
        .as_ref()
        .expect("we don't have any fake node configured");
    let response = NidxSearcherClient::connect(format!("http://{}", searcher.address()))
        .await?
        .search(Request::new(SearchRequest {
            shard_ids: shards.clone(),
            result_per_page: 20,
            body: "Hola".into(),
            paragraph: true,
            ..Default::default()
        }))
        .await;
    assert!(response.is_err());
    let error = response.unwrap_err();
    assert_eq!(error.code(), Code::Internal);
    assert!(
        error
            .message()
            .contains("Error in search, exhausted all available nodes for shard")
    );

    Ok(())
}

#[sqlx::test]
async fn test_searchers_partial_results(pool: PgPool) -> anyhow::Result<()> {
    let mut fixture = NidxFixture::new(pool).await?;
    let shards: Vec<String> = create_shards(&mut fixture.api_client, 10)
        .await?
        .iter()
        .map(|s| s.to_string())
        .sorted()
        .collect();

    // Create a cluster with 2 nodes and 1 replica
    let mut cluster = SearcherCluster::new(
        fixture.settings.clone(),
        Topology {
            nodes: 2,
            shard_replication: 1,
            network: vec![NetworkAvailability::Connected; 2],
        },
    )
    .await?;

    let mut counts = Vec::with_capacity(2);
    for searcher in &cluster.searchers {
        let searcher = searcher.as_ref().expect("we don't have any fake node configured");
        let mut request = Request::new(SearchRequest {
            shard_ids: shards.clone(),
            result_per_page: 20,
            body: "Hola".into(),
            paragraph: true,
            ..Default::default()
        });
        request.metadata_mut().insert("nidx-local-only", "1".parse().unwrap());
        let response = NidxSearcherClient::connect(format!("http://{}", searcher.address()))
            .await?
            .search(request)
            .await?
            .into_inner();
        counts.push(response.shard_ids.len());
    }
    assert_eq!(counts.iter().sum::<usize>(), 10);

    // Make a one unavailable
    cluster.shutdown_node(1);

    // The remaining searcher has only ~1/2 of the shards. Performing a Search
    // for all shards will fail
    let searcher = cluster.searchers[0]
        .as_ref()
        .expect("we don't have any fake node configured");
    let mut client = NidxSearcherClient::connect(format!("http://{}", searcher.address())).await?;
    let response = client
        .search(Request::new(SearchRequest {
            shard_ids: shards.clone(),
            result_per_page: 20,
            body: "Hola".into(),
            paragraph: true,
            ..Default::default()
        }))
        .await;
    assert!(response.is_err());
    let error = response.unwrap_err();
    assert_eq!(error.code(), Code::Internal);
    assert!(error.message().contains("transport error"));

    // But a local-only request will return partial results for all its shards
    let mut request = Request::new(SearchRequest {
        shard_ids: shards.clone(),
        result_per_page: 20,
        body: "Hola".into(),
        paragraph: true,
        ..Default::default()
    });
    request.metadata_mut().insert("nidx-local-only", "1".parse().unwrap());
    let response = client.search(request).await?.into_inner();
    assert_eq!(response.shard_ids.len(), counts[0]);

    Ok(())
}
