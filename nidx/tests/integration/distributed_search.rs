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

use std::collections::HashSet;

use nidx::searcher::grpc::Sharded;
use nidx_protos::graph_query::path_query;
use nidx_protos::nidx::nidx_searcher_client::NidxSearcherClient;
use nidx_protos::relation_node::NodeType;
use nidx_protos::{GraphQuery, GraphSearchRequest, SearchRequest, SuggestFeatures, SuggestRequest, graph_query};

use rstest::rstest;
use sqlx::PgPool;
use tonic::transport::Channel;
use tonic::{Code, Request};
use uuid::Uuid;

use crate::common::cluster::{NetworkAvailability, SearcherCluster, Topology};
use crate::common::services::NidxFixture;
use crate::common::utils::create_shards;

enum GrpcOp {
    Search,
    Suggest,
    Graph,
}

enum RequestType {
    Distributed,
    Local,
}

impl GrpcOp {
    // Call a specific gRPC method with a generic request. We don't really care
    // about the exact results, we want to test distribution
    async fn call(
        &self,
        client: &mut NidxSearcherClient<Channel>,
        shards: HashSet<Uuid>,
        request_type: RequestType,
    ) -> tonic::Result<Box<dyn Sharded>> {
        let shards = shards.into_iter().map(|x| x.to_string()).collect();
        match self {
            Self::Search => {
                let mut request = Request::new(SearchRequest {
                    shard_ids: shards,
                    result_per_page: 20,
                    body: "Hola".into(),
                    paragraph: true,
                    ..Default::default()
                });
                if matches!(request_type, RequestType::Local) {
                    request.metadata_mut().insert("nidx-local-only", "1".parse().unwrap());
                }
                client
                    .search(request)
                    .await
                    .map(|response| Box::new(response.into_inner()) as Box<dyn Sharded>)
            }

            Self::Suggest => {
                let mut request = Request::new(SuggestRequest {
                    shard_ids: shards,
                    body: "Hola".into(),
                    top_k: 20,
                    features: vec![SuggestFeatures::Paragraphs.into(), SuggestFeatures::Entities.into()],
                    ..Default::default()
                });
                if matches!(request_type, RequestType::Local) {
                    request.metadata_mut().insert("nidx-local-only", "1".parse().unwrap());
                }
                client
                    .suggest(request)
                    .await
                    .map(|response| Box::new(response.into_inner()) as Box<dyn Sharded>)
            }

            Self::Graph => {
                let mut request = Request::new(GraphSearchRequest {
                    shard_ids: shards,
                    query: Some(GraphQuery {
                        path: Some(graph_query::PathQuery {
                            query: Some(path_query::Query::Path(graph_query::Path {
                                source: Some(graph_query::Node {
                                    value: Some("Blue whale".to_string()),
                                    node_type: Some(NodeType::Entity.into()),
                                    node_subtype: Some("ANIMAL".to_string()),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            })),
                        }),
                    }),
                    top_k: 30,
                    ..Default::default()
                });
                if matches!(request_type, RequestType::Local) {
                    request.metadata_mut().insert("nidx-local-only", "1".parse().unwrap());
                }
                client
                    .graph_search(request)
                    .await
                    .map(|response| Box::new(response.into_inner()) as Box<dyn Sharded>)
            }
        }
    }
}

#[rstest]
#[sqlx::test]
async fn test_searchers_with_1_shard_replica(
    #[ignore] pool: PgPool,
    #[values(GrpcOp::Search, GrpcOp::Suggest, GrpcOp::Graph)] grpc_op: GrpcOp,
) -> anyhow::Result<()> {
    let mut fixture = NidxFixture::new(pool).await?;
    let shards: HashSet<Uuid> = create_shards(&mut fixture.api_client, 10).await?.into_iter().collect();

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
        let mut client = NidxSearcherClient::connect(format!("http://{}", searcher.address())).await?;
        let response = grpc_op
            .call(&mut client, shards.clone(), RequestType::Distributed)
            .await;
        assert!(response.is_ok());
        assert_eq!(response?.shards()?.into_iter().collect::<HashSet<Uuid>>(), shards);
    }

    // Shutdown one of the searchers so his 1/3 of shards is not available anymore
    cluster.shutdown_node(2);

    // Each searcher has 1/3 of the total shards. As everyone is available, the queried searcher
    // will request the other two for the missing 2/3 of shards
    for searcher in &cluster.searchers[0..2] {
        let searcher = searcher.as_ref().expect("we don't have any fake node configured");
        let mut client = NidxSearcherClient::connect(format!("http://{}", searcher.address())).await?;
        let response = grpc_op
            .call(&mut client, shards.clone(), RequestType::Distributed)
            .await;
        assert!(response.is_err());
        let error = response.unwrap_err();
        assert_eq!(error.code(), Code::NotFound);
    }

    Ok(())
}

#[rstest]
#[sqlx::test]
async fn test_searchers_with_2_shard_replica(
    #[ignore] pool: PgPool,
    #[values(GrpcOp::Search, GrpcOp::Suggest, GrpcOp::Graph)] grpc_op: GrpcOp,
) -> anyhow::Result<()> {
    let mut fixture = NidxFixture::new(pool).await?;
    let shards: HashSet<Uuid> = create_shards(&mut fixture.api_client, 10).await?.into_iter().collect();

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
        let mut client = NidxSearcherClient::connect(format!("http://{}", searcher.address())).await?;
        let response = grpc_op
            .call(&mut client, shards.clone(), RequestType::Distributed)
            .await?;
        assert_eq!(response.shards()?.into_iter().collect::<HashSet<Uuid>>(), shards);
    }

    // Shutdown one of the searchers so his 2/3 of shards is not available anymore
    cluster.shutdown_node(2);

    // As each searcher has 2/3 of the total shards, 2 is enough to still answer for all shards.
    for searcher in &cluster.searchers[0..2] {
        let searcher = searcher.as_ref().expect("we don't have any fake node configured");
        let mut client = NidxSearcherClient::connect(format!("http://{}", searcher.address())).await?;
        let response = grpc_op
            .call(&mut client, shards.clone(), RequestType::Distributed)
            .await?;
        assert_eq!(response.shards()?.into_iter().collect::<HashSet<Uuid>>(), shards);
    }

    // If we make a second node unavilable, we won't be able to answer
    cluster.shutdown_node(1);

    let searcher = cluster.searchers[0]
        .as_ref()
        .expect("we don't have any fake node configured");

    let mut client = NidxSearcherClient::connect(format!("http://{}", searcher.address())).await?;
    let response = grpc_op
        .call(&mut client, shards.clone(), RequestType::Distributed)
        .await;
    assert!(response.is_err());
    let error = response.unwrap_err();
    assert_eq!(error.code(), Code::NotFound);

    Ok(())
}

#[rstest]
#[sqlx::test]
async fn test_searchers_partial_results(
    #[ignore] pool: PgPool,
    #[values(GrpcOp::Search, GrpcOp::Suggest, GrpcOp::Graph)] grpc_op: GrpcOp,
) -> anyhow::Result<()> {
    let mut fixture = NidxFixture::new(pool).await?;
    let shards: HashSet<Uuid> = create_shards(&mut fixture.api_client, 10).await?.into_iter().collect();

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
        let mut client = NidxSearcherClient::connect(format!("http://{}", searcher.address())).await?;
        let response = grpc_op.call(&mut client, shards.clone(), RequestType::Local).await?;
        counts.push(response.shards()?.len());
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
    let response = grpc_op
        .call(&mut client, shards.clone(), RequestType::Distributed)
        .await;
    assert!(response.is_err());
    let error = response.unwrap_err();
    assert_eq!(error.code(), Code::Internal);
    assert!(error.message().contains("transport error"));

    // But a local-only request will return partial results for all its shards
    let response = grpc_op.call(&mut client, shards.clone(), RequestType::Local).await?;
    assert_eq!(response.shards()?.len(), counts[0]);

    Ok(())
}
