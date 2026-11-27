// Copyright 2021 Bosutech XXI S.L.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use crate::common::cluster::{NetworkAvailability, SearcherCluster, Topology};
use crate::common::services::NidxFixture;
use base64::{Engine, engine::general_purpose::STANDARD, engine::general_purpose::URL_SAFE_NO_PAD};
use nidx::api::grpc::ApiServer;
use nidx::grpc_server::GrpcServer;
use nidx::settings::LpgSettings;
use nidx_protos::nidx::nidx_indexer_client::NidxIndexerClient;
use nidx_protos::nidx::nidx_indexer_server::{NidxIndexer, NidxIndexerServer};
use nidx_protos::nidx::nidx_lpg_client::NidxLpgClient;
use nidx_protos::nidx::nidx_lpg_server::NidxLpg;
use nidx_protos::nidx::nidx_searcher_client::NidxSearcherClient;
use nidx_protos::prost::*;
use nidx_protos::relation::RelationType;
use nidx_protos::{
    BulkLoadRequest, DeleteNodeRequest, DeleteRecordRequest, ExpandRequest, ExpandResponse, FindRecordsRequest,
    GetRecordsRequest, GraphQuery, GraphSearchRequest, IndexMessage, IndexResourceRequest, IndexResourcesRequest,
    LpgDirection, LpgIntegerRange, LpgOperation, LpgOperationRequest, LpgOperationResult, LpgOperationStatus,
    LpgProperty, LpgPropertyFilter, LpgRecord, LpgRecordKey, LpgRecordKind, LpgStringList, LpgTextChunks,
    LpgTextProperty, LpgTextPropertyPatch, LpgTextPropertyUpdate, LpgVector, NewShardRequest, OpStatus,
    PatchTextPropertiesRequest, SearchChunksRequest, ShortestPathRequest, UpsertRecordRequest, VectorIndexConfig,
    graph_query, lpg_operation, lpg_property, lpg_property_filter,
};
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;
use tonic::{Code, Request, Response, Status, transport::Channel};

async fn run_test_operation(
    client: &mut NidxLpgClient<Channel>,
    graph_id: String,
    operation: LpgOperation,
) -> Result<LpgOperationResult, Status> {
    let response = client
        .lpg_operation(Request::new(LpgOperationRequest {
            graph_id,
            operations: vec![operation],
        }))
        .await?
        .into_inner();
    let [result] = response.results.as_slice() else {
        return Err(Status::internal("invalid operation result count"));
    };
    match LpgOperationStatus::try_from(result.status) {
        Ok(LpgOperationStatus::Applied) => Ok(result.clone()),
        Ok(LpgOperationStatus::Failed) => {
            let code = match result.error_code.as_str() {
                "InvalidArgument" => Code::InvalidArgument,
                "DeadlineExceeded" => Code::DeadlineExceeded,
                "NotFound" => Code::NotFound,
                "AlreadyExists" => Code::AlreadyExists,
                "PermissionDenied" => Code::PermissionDenied,
                "ResourceExhausted" => Code::ResourceExhausted,
                "FailedPrecondition" => Code::FailedPrecondition,
                "Aborted" => Code::Aborted,
                "OutOfRange" => Code::OutOfRange,
                "Unimplemented" => Code::Unimplemented,
                "Internal" => Code::Internal,
                "Unavailable" => Code::Unavailable,
                "DataLoss" => Code::DataLoss,
                "Unauthenticated" => Code::Unauthenticated,
                "Cancelled" => Code::Cancelled,
                _ => Code::Unknown,
            };
            Err(Status::new(code, result.error_message.clone()))
        }
        Ok(LpgOperationStatus::Skipped) => Err(Status::failed_precondition("operation was skipped")),
        _ => Err(Status::internal("invalid operation result status")),
    }
}

#[sqlx::test]
async fn bulk_stateless_preflight_and_upsert(pool: PgPool) -> anyhow::Result<()> {
    let mut fixture = NidxFixture::new(pool.clone()).await?;
    let graph = shard(&mut fixture).await?;
    let node = LpgRecord {
        key: Some(key(LpgRecordKind::LpgNode, "node")),
        labels: vec!["Node".into()],
        revision: "first".into(),
        ..Default::default()
    };
    let edge = LpgRecord {
        key: Some(key(LpgRecordKind::LpgEdge, "edge")),
        source: Some("node".into()),
        target: Some("node".into()),
        edge_type: Some("loop".into()),
        ..Default::default()
    };
    let request = BulkLoadRequest {
        graph_id: graph.clone(),
        records: vec![edge.clone(), node.clone()],
        ..Default::default()
    };
    let mut invalid = request.clone();
    invalid.records.pop();
    assert_eq!(
        fixture.lpg_client.bulk_load(invalid).await.unwrap_err().code(),
        Code::FailedPrecondition
    );
    let mut invalid = request.clone();
    invalid.records[1].text_properties.insert(
        "text".into(),
        LpgTextProperty {
            chunks: vec!["hello".into()],
            embeddings: vec![LpgVector { values: vec![1.0, 0.0] }],
        },
    );
    assert_eq!(
        fixture.lpg_client.bulk_load(invalid).await.unwrap_err().code(),
        Code::InvalidArgument
    );

    // The test searcher's independently synced indexes can briefly be incomplete.
    // Bulk upserts are safe to retry after an uncertain outcome.
    let results = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            match fixture.lpg_client.bulk_load(request.clone()).await {
                Ok(response) => break Ok(response.into_inner().results),
                Err(error)
                    if error.code() == Code::Internal
                        && error
                            .message()
                            .contains("text searcher is required when text prefilter is needed") =>
                {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(error) => break Err(error),
            }
        }
    })
    .await??;
    assert_eq!(results.len(), 2);
    assert!(
        results
            .iter()
            .all(|result| result.status == LpgOperationStatus::Applied as i32)
    );
    let mut updated = node.clone();
    updated.revision = "second".into();
    fixture
        .lpg_client
        .bulk_load(BulkLoadRequest {
            graph_id: graph.clone(),
            records: vec![updated.clone()],
            ..Default::default()
        })
        .await?;
    wait_for(
        &mut fixture,
        &graph,
        vec![node.key.unwrap(), edge.key.clone().unwrap()],
        vec![updated, edge],
    )
    .await;
    Ok(())
}

#[sqlx::test]
async fn bulk_returns_after_indexer_acknowledgement_without_visibility(pool: PgPool) -> anyhow::Result<()> {
    let mut fixture = NidxFixture::new(pool).await?;
    let graph = shard(&mut fixture).await?;
    let mut node = LpgRecord {
        key: Some(key(LpgRecordKind::LpgNode, "node")),
        labels: vec!["Node".into()],
        revision: "visible".into(),
        ..Default::default()
    };
    fixture
        .lpg_client
        .upsert_record(UpsertRecordRequest {
            graph_id: graph.clone(),
            record: Some(node.clone()),
        })
        .await?;
    let calls = Arc::new(AtomicUsize::new(0));
    let indexer = AcknowledgeOnlyIndexer(calls.clone());
    let server = GrpcServer::new("127.0.0.1:0").await?;
    let mut settings = fixture.settings.settings.lpg.clone().unwrap();
    settings.indexer_address = format!("http://127.0.0.1:{}", server.port()?);
    settings.searcher_address = "http://127.0.0.1:1".into();
    let shutdown = CancellationToken::new();
    let router = tonic::service::Routes::new(NidxIndexerServer::new(indexer)).into_axum_router();
    tokio::spawn(server.serve(router, shutdown.clone()));
    let lpg = nidx::api::lpg::LpgServer::new(fixture.settings.metadata.clone(), Some(settings));
    node.revision = "acknowledged-but-not-visible".into();
    let mut other = node.clone();
    other.key.as_mut().unwrap().id = "other".into();
    let response = tokio::time::timeout(
        Duration::from_secs(5),
        lpg.bulk_load(Request::new(BulkLoadRequest {
            graph_id: graph.clone(),
            records: vec![node.clone(), other],
            ..Default::default()
        })),
    )
    .await??;
    shutdown.cancel();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let results = response.into_inner().results;
    assert_eq!(results.len(), 2);
    assert!(
        results
            .iter()
            .all(|result| result.status == LpgOperationStatus::Applied as i32)
    );
    let stored = fixture
        .lpg_client
        .get_records(GetRecordsRequest {
            graph_id: graph,
            keys: vec![node.key.unwrap()],
        })
        .await?
        .into_inner();
    assert_eq!(stored.records[0].revision, "visible");
    Ok(())
}

struct AcknowledgeOnlyIndexer(Arc<AtomicUsize>);

#[sqlx::test]
async fn bulk_shared_segments_preserve_resource_ownership(pool: PgPool) -> anyhow::Result<()> {
    let mut fixture = NidxFixture::new(pool.clone()).await?;
    let graph = shard(&mut fixture).await?;
    let node = |id: &str, text: &str, vector: Vec<f32>| LpgRecord {
        key: Some(key(LpgRecordKind::LpgNode, id)),
        labels: vec!["Node".into()],
        text_properties: HashMap::from([(
            "text".into(),
            LpgTextProperty {
                chunks: vec![text.into()],
                embeddings: vec![LpgVector { values: vector }],
            },
        )]),
        ..Default::default()
    };
    let a = node("a", "red apple", vec![1.0, 0.0, 0.0]);
    let b = node("b", "blue berry", vec![0.0, 1.0, 0.0]);
    let edge = |id: &str| LpgRecord {
        key: Some(key(LpgRecordKind::LpgEdge, id)),
        source: Some("a".into()),
        target: Some("b".into()),
        edge_type: Some("LINK".into()),
        ..Default::default()
    };
    let e1 = edge("e1");
    let e2 = edge("e2");
    let records = vec![e1.clone(), b.clone(), a.clone(), e2.clone()];
    fixture
        .lpg_client
        .bulk_load(BulkLoadRequest {
            graph_id: graph.clone(),
            records: records.clone(),
            ..Default::default()
        })
        .await?;
    let indexes = nidx::metadata::Index::for_shard(&pool, uuid::Uuid::parse_str(&graph)?).await?;
    assert_eq!(indexes.len(), 5);
    for index in &indexes {
        let segments = index.segments(&pool).await?;
        assert_eq!(segments.len(), 1, "{:?} should share a segment", index.kind);
        assert_eq!(segments[0].delete_at, None);
        let expected_records = match index.kind {
            nidx::metadata::IndexKind::Text | nidx::metadata::IndexKind::Paragraph => 6,
            nidx::metadata::IndexKind::Json => 4,
            _ => 2,
        };
        assert_eq!(segments[0].records, expected_records, "{:?}", index.kind);
    }
    wait_for(
        &mut fixture,
        &graph,
        records.iter().map(|record| record.key.clone().unwrap()).collect(),
        records,
    )
    .await;

    let a = node("a", "green pear", vec![0.0, 0.0, 1.0]);
    let mut e1 = e1;
    e1.target = Some("a".into());
    fixture
        .lpg_client
        .bulk_load(BulkLoadRequest {
            graph_id: graph.clone(),
            records: vec![a.clone(), e1.clone()],
            endpoint_labels: HashMap::from([(
                "a".into(),
                LpgStringList {
                    values: vec!["Node".into()],
                },
            )]),
        })
        .await?;
    let records = vec![a.clone(), b.clone(), e1.clone(), e2.clone()];
    wait_for(
        &mut fixture,
        &graph,
        records.iter().map(|record| record.key.clone().unwrap()).collect(),
        records,
    )
    .await;
    fixture.wait_sync().await;
    for (text, expected) in [("apple", 0), ("pear", 1), ("berry", 1)] {
        let response = fixture
            .lpg_client
            .search_chunks(SearchChunksRequest {
                graph_id: graph.clone(),
                text: Some(text.into()),
                top_k: 10,
                ..Default::default()
            })
            .await?
            .into_inner();
        assert_eq!(response.hits.len(), expected, "text: {text}");
    }
    let vectors = fixture
        .lpg_client
        .search_chunks(SearchChunksRequest {
            graph_id: graph.clone(),
            vector: vec![0.0, 0.0, 1.0],
            top_k: 1,
            ..Default::default()
        })
        .await?
        .into_inner();
    assert_eq!(vectors.hits[0].record.as_ref().unwrap().key.as_ref().unwrap().id, "a");
    fixture
        .lpg_client
        .delete_record(DeleteRecordRequest {
            graph_id: graph.clone(),
            key: e1.key,
        })
        .await?;
    wait_for(
        &mut fixture,
        &graph,
        vec![a.key.clone().unwrap(), b.key.clone().unwrap(), e2.key.clone().unwrap()],
        vec![a, b, e2.clone()],
    )
    .await;
    fixture.wait_sync().await;
    let edges = fixture
        .lpg_client
        .find_records(FindRecordsRequest {
            graph_id: graph.clone(),
            kind: LpgRecordKind::LpgEdge as i32,
            page_size: 10,
            ..Default::default()
        })
        .await?
        .into_inner();
    assert_eq!(edges.records, vec![e2]);
    let relations = fixture
        .searcher_client
        .graph_search(GraphSearchRequest {
            shard_ids: vec![graph],
            query: Some(GraphQuery {
                path: Some(graph_query::PathQuery {
                    query: Some(graph_query::path_query::Query::Facet(graph_query::FacetFilter {
                        facet: format!("/lpg/type/edge/{}", URL_SAFE_NO_PAD.encode("LINK")),
                    })),
                }),
            }),
            top_k: 10,
            ..Default::default()
        })
        .await?
        .into_inner();
    assert_eq!(relations.graph.len(), 1);
    Ok(())
}

#[sqlx::test]
async fn bulk_endpoint_references_do_not_rewrite_nodes(pool: PgPool) -> anyhow::Result<()> {
    let mut fixture = NidxFixture::new(pool.clone()).await?;
    let graph = shard(&mut fixture).await?;
    let node = LpgRecord {
        key: Some(key(LpgRecordKind::LpgNode, "node")),
        labels: vec!["Node".into()],
        revision: "original".into(),
        ..Default::default()
    };
    fixture
        .lpg_client
        .bulk_load(BulkLoadRequest {
            graph_id: graph.clone(),
            records: vec![node.clone()],
            ..Default::default()
        })
        .await?;
    let edge = LpgRecord {
        key: Some(key(LpgRecordKind::LpgEdge, "edge")),
        source: Some("node".into()),
        target: Some("node".into()),
        edge_type: Some("loop".into()),
        ..Default::default()
    };
    let response = fixture
        .lpg_client
        .bulk_load(BulkLoadRequest {
            graph_id: graph.clone(),
            records: vec![edge.clone()],
            endpoint_labels: HashMap::from([(
                "node".into(),
                LpgStringList {
                    values: node.labels.clone(),
                },
            )]),
        })
        .await?
        .into_inner();
    assert_eq!(response.results.len(), 1);
    assert_eq!(response.results[0].status, LpgOperationStatus::Applied as i32);
    let text_segments: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM segments JOIN indexes ON segments.index_id = indexes.id \
         WHERE indexes.shard_id = $1 AND indexes.kind = 'text'",
    )
    .bind(uuid::Uuid::parse_str(&graph)?)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        text_segments, 2,
        "one segment per explicit record, with no endpoint rewrite"
    );
    wait_for(
        &mut fixture,
        &graph,
        vec![node.key.clone().unwrap(), edge.key.clone().unwrap()],
        vec![node, edge.clone()],
    )
    .await;
    fixture.wait_sync().await;
    let relations = fixture
        .searcher_client
        .graph_search(GraphSearchRequest {
            shard_ids: vec![graph],
            query: Some(GraphQuery {
                path: Some(graph_query::PathQuery {
                    query: Some(graph_query::path_query::Query::Facet(graph_query::FacetFilter {
                        facet: format!("/lpg/type/source/{}", URL_SAFE_NO_PAD.encode("Node")),
                    })),
                }),
            }),
            top_k: 10,
            ..Default::default()
        })
        .await?;
    assert_eq!(relations.into_inner().graph.len(), 1);
    Ok(())
}

#[tonic::async_trait]
impl NidxIndexer for AcknowledgeOnlyIndexer {
    async fn index_resources(&self, request: Request<IndexResourcesRequest>) -> Result<Response<OpStatus>, Status> {
        assert_eq!(request.get_ref().resources.len(), 2);
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Response::new(OpStatus::default()))
    }

    async fn index(&self, _: Request<IndexMessage>) -> Result<Response<OpStatus>, Status> {
        Err(Status::unimplemented("use IndexResource"))
    }

    async fn index_resource(&self, _: Request<IndexResourceRequest>) -> Result<Response<OpStatus>, Status> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Response::new(OpStatus {
            status: nidx_protos::op_status::Status::Ok as i32,
            ..Default::default()
        }))
    }
}

#[sqlx::test]
async fn bulk_and_ordinary_failures_require_client_retries(pool: PgPool) -> anyhow::Result<()> {
    let mut fixture = NidxFixture::new(pool.clone()).await?;
    let graph = shard(&mut fixture).await?;
    let mut settings = fixture.settings.settings.lpg.clone().unwrap();
    let upstream = NidxIndexerClient::connect(settings.indexer_address.clone()).await?;
    let server = GrpcServer::new("127.0.0.1:0").await?;
    settings.indexer_address = format!("http://127.0.0.1:{}", server.port()?);
    let calls = Arc::new(AtomicUsize::new(0));
    let kinds = Arc::new(std::sync::Mutex::new(Vec::new()));
    let shutdown = CancellationToken::new();
    let proxy = FailSecondIndex {
        upstream,
        calls: calls.clone(),
        kinds: kinds.clone(),
    };
    let router = tonic::service::Routes::new(NidxIndexerServer::new(proxy)).into_axum_router();
    tokio::spawn(server.serve(router, shutdown.clone()));
    let broken = nidx::api::lpg::LpgServer::new(fixture.settings.metadata.clone(), Some(settings.clone()));
    let invalid_edge = LpgRecord {
        key: Some(key(LpgRecordKind::LpgEdge, "edge")),
        source: Some("one".into()),
        target: Some("missing".into()),
        edge_type: Some("link".into()),
        ..Default::default()
    };
    let invalid = BulkLoadRequest {
        graph_id: graph.clone(),
        records: vec![
            LpgRecord {
                key: Some(key(LpgRecordKind::LpgNode, "one")),
                labels: vec!["Node".into()],
                ..Default::default()
            },
            invalid_edge,
        ],
        ..Default::default()
    };
    assert_eq!(
        broken.bulk_load(Request::new(invalid)).await.unwrap_err().code(),
        Code::FailedPrecondition
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let records = vec![
        LpgRecord {
            key: Some(key(LpgRecordKind::LpgEdge, "edge")),
            source: Some("one".into()),
            target: Some("one".into()),
            edge_type: Some("loop".into()),
            ..Default::default()
        },
        LpgRecord {
            key: Some(key(LpgRecordKind::LpgNode, "one")),
            labels: vec!["Node".into()],
            ..Default::default()
        },
    ];
    let error = broken
        .bulk_load(Request::new(BulkLoadRequest {
            graph_id: graph.clone(),
            records: records.clone(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::Unavailable);
    assert!(error.message().contains("partially applied"));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        *kinds.lock().unwrap(),
        [LpgRecordKind::LpgNode as i32, LpgRecordKind::LpgEdge as i32]
    );
    broken
        .bulk_load(Request::new(BulkLoadRequest {
            graph_id: graph.clone(),
            records: records.clone(),
            ..Default::default()
        }))
        .await?;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let keys = records.iter().map(|record| record.key.clone().unwrap()).collect();
    wait_for(&mut fixture, &graph, keys, records).await;

    calls.store(0, Ordering::SeqCst);
    let node = |id: &str| LpgRecord {
        key: Some(key(LpgRecordKind::LpgNode, id)),
        labels: vec!["Node".into()],
        ..Default::default()
    };
    let operations = |ids: &[&str]| {
        Request::new(LpgOperationRequest {
            graph_id: graph.clone(),
            operations: ids
                .iter()
                .map(|id| LpgOperation {
                    operation: Some(lpg_operation::Operation::UpsertRecord(node(id))),
                })
                .collect(),
        })
    };
    let results = broken
        .lpg_operation(operations(&["a", "b", "c"]))
        .await?
        .into_inner()
        .results;
    assert_eq!(
        results.iter().map(|result| result.status).collect::<Vec<_>>(),
        [
            LpgOperationStatus::Applied as i32,
            LpgOperationStatus::Failed as i32,
            LpgOperationStatus::Skipped as i32,
        ],
        "{results:?}"
    );
    assert_eq!(results[1].error_code, "Unavailable");
    assert_eq!(calls.load(Ordering::SeqCst), 2);

    let restarted = nidx::api::lpg::LpgServer::new(fixture.settings.metadata.clone(), Some(settings));
    let result = restarted.lpg_operation(operations(&["c"])).await?.into_inner();
    assert_eq!(result.results[0].status, LpgOperationStatus::Applied as i32);
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    let keys: Vec<_> = ["a", "b", "c"].map(|id| key(LpgRecordKind::LpgNode, id)).into();
    wait_for(&mut fixture, &graph, keys.clone(), vec![node("a"), node("c")]).await;

    let result = restarted.lpg_operation(operations(&["b"])).await?.into_inner();
    assert_eq!(result.results[0].status, LpgOperationStatus::Applied as i32);
    assert_eq!(calls.load(Ordering::SeqCst), 4);
    wait_for(&mut fixture, &graph, keys, vec![node("a"), node("b"), node("c")]).await;
    shutdown.cancel();
    Ok(())
}

struct FailSecondIndex {
    upstream: NidxIndexerClient<Channel>,
    calls: Arc<AtomicUsize>,
    kinds: Arc<std::sync::Mutex<Vec<i32>>>,
}

#[tonic::async_trait]
impl NidxIndexer for FailSecondIndex {
    async fn index_resources(&self, request: Request<IndexResourcesRequest>) -> Result<Response<OpStatus>, Status> {
        for resource in &request.get_ref().resources {
            let bytes = STANDARD.decode(&resource.texts["a/lpg_record_v1"].text).unwrap();
            let record = LpgRecord::decode(bytes.as_slice()).unwrap();
            self.kinds.lock().unwrap().push(record.key.unwrap().kind);
        }
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        let response = self.upstream.clone().index_resources(request).await?;
        if call == 0 {
            return Err(Status::unavailable("injected failure after batch publication"));
        }
        Ok(response)
    }

    async fn index(&self, request: Request<IndexMessage>) -> Result<Response<OpStatus>, Status> {
        self.upstream.clone().index(request).await
    }

    async fn index_resource(&self, request: Request<IndexResourceRequest>) -> Result<Response<OpStatus>, Status> {
        let text = &request.get_ref().resource.as_ref().unwrap().texts["a/lpg_record_v1"].text;
        let bytes = STANDARD.decode(text).unwrap();
        let record = LpgRecord::decode(bytes.as_slice()).unwrap();
        self.kinds.lock().unwrap().push(record.key.unwrap().kind);
        if self.calls.fetch_add(1, Ordering::SeqCst) == 1 {
            return Err(Status::unavailable("injected partial indexing failure"));
        }
        self.upstream.clone().index_resource(request).await
    }
}

#[tonic::async_trait]
trait LpgWriteTestExt {
    async fn upsert_record(
        &mut self,
        request: UpsertRecordRequest,
    ) -> Result<Response<nidx_protos::noderesources::EmptyQuery>, Status>;
    async fn delete_record(
        &mut self,
        request: DeleteRecordRequest,
    ) -> Result<Response<nidx_protos::noderesources::EmptyQuery>, Status>;
    async fn delete_node(
        &mut self,
        request: DeleteNodeRequest,
    ) -> Result<Response<nidx_protos::noderesources::EmptyQuery>, Status>;
    async fn patch_text_properties(
        &mut self,
        request: PatchTextPropertiesRequest,
    ) -> Result<Response<nidx_protos::PatchTextPropertiesResponse>, Status>;
}

#[tonic::async_trait]
impl LpgWriteTestExt for NidxLpgClient<Channel> {
    async fn upsert_record(
        &mut self,
        request: UpsertRecordRequest,
    ) -> Result<Response<nidx_protos::noderesources::EmptyQuery>, Status> {
        run_test_operation(
            self,
            request.graph_id,
            LpgOperation {
                operation: request.record.map(lpg_operation::Operation::UpsertRecord),
            },
        )
        .await?;
        Ok(Response::new(nidx_protos::noderesources::EmptyQuery {}))
    }

    async fn delete_record(
        &mut self,
        request: DeleteRecordRequest,
    ) -> Result<Response<nidx_protos::noderesources::EmptyQuery>, Status> {
        run_test_operation(
            self,
            request.graph_id,
            LpgOperation {
                operation: request.key.map(lpg_operation::Operation::DeleteRecord),
            },
        )
        .await?;
        Ok(Response::new(nidx_protos::noderesources::EmptyQuery {}))
    }

    async fn delete_node(
        &mut self,
        request: DeleteNodeRequest,
    ) -> Result<Response<nidx_protos::noderesources::EmptyQuery>, Status> {
        run_test_operation(
            self,
            request.graph_id,
            LpgOperation {
                operation: Some(lpg_operation::Operation::DeleteNode(nidx_protos::LpgNodeDeletion {
                    id: request.id,
                })),
            },
        )
        .await?;
        Ok(Response::new(nidx_protos::noderesources::EmptyQuery {}))
    }

    async fn patch_text_properties(
        &mut self,
        request: PatchTextPropertiesRequest,
    ) -> Result<Response<nidx_protos::PatchTextPropertiesResponse>, Status> {
        let result = run_test_operation(
            self,
            request.graph_id,
            LpgOperation {
                operation: Some(lpg_operation::Operation::PatchTextProperties(LpgTextPropertyUpdate {
                    key: request.key,
                    properties: request.properties,
                    vectorset: request.vectorset,
                })),
            },
        )
        .await?;
        Ok(Response::new(nidx_protos::PatchTextPropertiesResponse {
            record: result.record,
        }))
    }
}

async fn shard(fixture: &mut NidxFixture) -> anyhow::Result<String> {
    Ok(fixture
        .api_client
        .new_shard(NewShardRequest {
            kbid: uuid::Uuid::new_v4().to_string(),
            vectorsets_configs: HashMap::from([(
                "default".into(),
                VectorIndexConfig {
                    vector_dimension: Some(3),
                    ..Default::default()
                },
            )]),
            ..Default::default()
        })
        .await?
        .into_inner()
        .id)
}

fn key(kind: LpgRecordKind, id: &str) -> LpgRecordKey {
    LpgRecordKey {
        kind: kind as i32,
        id: id.into(),
    }
}

fn type_facet(position: &str, value: &str) -> String {
    format!("/lpg/type/{position}/{}", URL_SAFE_NO_PAD.encode(value.as_bytes()))
}

async fn search_graph(
    fixture: &NidxFixture,
    graph: &str,
    query: GraphQuery,
) -> Result<nidx_protos::GraphSearchResponse, tonic::Status> {
    fixture
        .searcher_client
        .clone()
        .graph_search(GraphSearchRequest {
            shard_ids: vec![graph.into()],
            query: Some(query),
            top_k: 100,
            ..Default::default()
        })
        .await
        .map(|response| response.into_inner())
}

async fn wait_for(fixture: &mut NidxFixture, graph: &str, keys: Vec<LpgRecordKey>, expected: Vec<LpgRecord>) {
    for _ in 0..100 {
        let result = fixture
            .lpg_client
            .get_records(GetRecordsRequest {
                graph_id: graph.into(),
                keys: keys.clone(),
            })
            .await;
        if let Ok(response) = result
            && response.into_inner().records == expected
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("records did not become visible: {expected:?}");
}

#[sqlx::test]
async fn lpg_get_records_searcher_local_and_remote(pool: PgPool) -> anyhow::Result<()> {
    let mut fixture = NidxFixture::new(pool).await?;
    let graph = shard(&mut fixture).await?;
    let other_graph = shard(&mut fixture).await?;
    let node_key = key(LpgRecordKind::LpgNode, "same/id 🦊");
    let node = LpgRecord {
        key: Some(node_key.clone()),
        labels: vec!["Node".into()],
        revision: "original".into(),
        ..Default::default()
    };
    fixture
        .lpg_client
        .upsert_record(UpsertRecordRequest {
            graph_id: graph.clone(),
            record: Some(node.clone()),
        })
        .await?;
    let request = GetRecordsRequest {
        graph_id: graph.clone(),
        keys: vec![
            node_key.clone(),
            key(LpgRecordKind::LpgEdge, &node_key.id),
            key(LpgRecordKind::LpgNode, "missing"),
            node_key.clone(),
        ],
    };
    let expected = vec![node.clone(), node.clone()];
    wait_for(&mut fixture, &graph, request.keys.clone(), expected.clone()).await;
    assert_eq!(
        fixture
            .searcher_client
            .lpg_get_records(request.clone())
            .await?
            .into_inner()
            .records,
        expected
    );
    fixture.wait_sync().await;
    assert!(
        fixture
            .searcher_client
            .lpg_get_records(GetRecordsRequest {
                graph_id: other_graph,
                keys: vec![node_key],
            })
            .await?
            .into_inner()
            .records
            .is_empty()
    );
    let cluster = SearcherCluster::new(
        fixture.settings.clone(),
        Topology {
            nodes: 2,
            shard_replication: 1,
            network: vec![NetworkAvailability::Connected; 2],
        },
    )
    .await?;
    let mut local_replicas = 0;
    for searcher in &cluster.searchers {
        let mut client =
            NidxSearcherClient::connect(format!("http://{}", searcher.as_ref().unwrap().address())).await?;
        assert_eq!(
            client.lpg_get_records(request.clone()).await?.into_inner().records,
            expected
        );
        let mut local_request = Request::new(request.clone());
        local_request.metadata_mut().insert("nidx-local-only", "1".parse()?);
        if client.lpg_get_records(local_request).await.is_ok() {
            local_replicas += 1;
        }
        for invalid_key in [key(LpgRecordKind::Unspecified, "n"), key(LpgRecordKind::LpgNode, "")] {
            assert_eq!(
                client
                    .lpg_get_records(GetRecordsRequest {
                        graph_id: graph.clone(),
                        keys: vec![invalid_key],
                    })
                    .await
                    .unwrap_err()
                    .code(),
                Code::InvalidArgument
            );
        }
        assert_eq!(
            client
                .lpg_get_records(GetRecordsRequest {
                    graph_id: uuid::Uuid::new_v4().to_string(),
                    keys: vec![],
                })
                .await
                .unwrap_err()
                .code(),
            Code::NotFound
        );
    }
    assert_eq!(local_replicas, 1, "the other node must have forwarded its read");
    Ok(())
}

#[sqlx::test]
async fn lpg_record_lifecycle(pool: PgPool) -> anyhow::Result<()> {
    let mut fixture = NidxFixture::new(pool).await?;
    let graph = shard(&mut fixture).await?;
    let other_graph = shard(&mut fixture).await?;
    let node_key = key(LpgRecordKind::LpgNode, "same/id 🦊");
    let edge_key = key(LpgRecordKind::LpgEdge, "same/id 🦊");
    let missing = key(LpgRecordKind::LpgNode, "missing");
    let mut node = LpgRecord {
        key: Some(node_key.clone()),
        labels: vec!["product".into()],
        properties: HashMap::from([
            (
                "count".into(),
                LpgProperty {
                    value: Some(lpg_property::Value::Integer(i64::MAX)),
                },
            ),
            (
                "null".into(),
                LpgProperty {
                    value: Some(lpg_property::Value::NullValue(true)),
                },
            ),
            (
                "tags".into(),
                LpgProperty {
                    value: Some(lpg_property::Value::Strings(LpgStringList {
                        values: vec!["one".into(), "two".into()],
                    })),
                },
            ),
        ]),
        ..Default::default()
    };
    let edge = LpgRecord {
        key: Some(edge_key.clone()),
        source: Some("a".into()),
        target: Some("b".into()),
        edge_type: Some("links".into()),
        ..Default::default()
    };
    for id in ["a", "b"] {
        fixture
            .lpg_client
            .upsert_record(UpsertRecordRequest {
                graph_id: graph.clone(),
                record: Some(LpgRecord {
                    key: Some(key(LpgRecordKind::LpgNode, id)),
                    labels: vec!["Node".into()],
                    ..Default::default()
                }),
            })
            .await?;
    }
    for record in [node.clone(), edge.clone()] {
        fixture
            .lpg_client
            .upsert_record(UpsertRecordRequest {
                graph_id: graph.clone(),
                record: Some(record),
            })
            .await?;
    }
    wait_for(
        &mut fixture,
        &graph,
        vec![missing.clone(), edge_key.clone(), node_key.clone()],
        vec![edge.clone(), node.clone()],
    )
    .await;
    wait_for(&mut fixture, &other_graph, vec![node_key.clone()], vec![]).await;

    let mut changed_types = node.clone();
    changed_types.labels = vec!["replacement".into()];
    assert_eq!(
        fixture
            .lpg_client
            .upsert_record(UpsertRecordRequest {
                graph_id: graph.clone(),
                record: Some(changed_types),
            })
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    let mut changed_edge_type = edge.clone();
    changed_edge_type.edge_type = Some("replaced".into());
    assert_eq!(
        fixture
            .lpg_client
            .upsert_record(UpsertRecordRequest {
                graph_id: graph.clone(),
                record: Some(changed_edge_type),
            })
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    node.properties.clear();
    node.revision = "updated".into();
    fixture
        .lpg_client
        .upsert_record(UpsertRecordRequest {
            graph_id: graph.clone(),
            record: Some(node.clone()),
        })
        .await?;
    wait_for(
        &mut fixture,
        &graph,
        vec![node_key.clone(), edge_key.clone()],
        vec![node.clone(), edge.clone()],
    )
    .await;
    fixture
        .lpg_client
        .delete_record(DeleteRecordRequest {
            graph_id: graph.clone(),
            key: Some(node_key.clone()),
        })
        .await?;
    wait_for(
        &mut fixture,
        &graph,
        vec![node_key.clone(), edge_key.clone()],
        vec![edge.clone()],
    )
    .await;
    // Deletion is idempotent and never cascades to edges.
    fixture
        .lpg_client
        .delete_record(DeleteRecordRequest {
            graph_id: graph.clone(),
            key: Some(node_key),
        })
        .await?;
    wait_for(&mut fixture, &graph, vec![edge_key], vec![edge]).await;
    Ok(())
}

#[sqlx::test]
async fn lpg_relations_filter_traversals_by_source_edge_and_target_types(pool: PgPool) -> anyhow::Result<()> {
    let mut fixture = NidxFixture::new(pool).await?;
    let graph = shard(&mut fixture).await?;
    let node = |id: &str, labels: &[&str]| LpgRecord {
        key: Some(key(LpgRecordKind::LpgNode, id)),
        labels: labels.iter().map(|label| (*label).into()).collect(),
        ..Default::default()
    };
    for record in [
        node("p", &["Product", "Sellable"]),
        node("c", &["Client"]),
        node("w1", &["Warehouse"]),
        node("w2", &["Warehouse"]),
        node("d", &["Distributor"]),
    ] {
        fixture
            .lpg_client
            .upsert_record(UpsertRecordRequest {
                graph_id: graph.clone(),
                record: Some(record),
            })
            .await?;
    }
    let edge = |id: &str, source: &str, target: &str| LpgRecord {
        key: Some(key(LpgRecordKind::LpgEdge, id)),
        source: Some(source.into()),
        target: Some(target.into()),
        edge_type: Some("PURCHASES".into()),
        ..Default::default()
    };
    let mut matching_edge = edge("matches", "p", "w1");
    let nonmatching_source = edge("wrong-source", "c", "w1");
    let nonmatching_target = edge("wrong-target", "p", "d");
    for record in [matching_edge.clone(), nonmatching_source, nonmatching_target] {
        fixture
            .lpg_client
            .upsert_record(UpsertRecordRequest {
                graph_id: graph.clone(),
                record: Some(record),
            })
            .await?;
    }

    let facet = |position: &str, value: &str| graph_query::PathQuery {
        query: Some(graph_query::path_query::Query::Facet(graph_query::FacetFilter {
            facet: type_facet(position, value),
        })),
    };
    let query = GraphQuery {
        path: Some(graph_query::PathQuery {
            query: Some(graph_query::path_query::Query::BoolAnd(graph_query::BoolQuery {
                operands: vec![
                    graph_query::PathQuery {
                        query: Some(graph_query::path_query::Query::Path(graph_query::Path {
                            relation: Some(graph_query::Relation {
                                value: Some("PURCHASES".into()),
                                relation_type: Some(RelationType::Other.into()),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })),
                    },
                    facet("source", "Product"),
                    facet("source", "Sellable"),
                    facet("edge", "PURCHASES"),
                    facet("target", "Warehouse"),
                ],
            })),
        }),
    };
    let response = search_graph(&fixture, &graph, query.clone()).await?;
    assert_eq!(response.graph.len(), 1);
    let path = &response.graph[0];
    assert_eq!(response.nodes[path.source as usize].value, "p");
    assert_eq!(response.nodes[path.destination as usize].value, "w1");
    assert_eq!(response.relations[path.relation as usize].label, "PURCHASES");

    matching_edge.target = Some("w2".into());
    fixture
        .lpg_client
        .upsert_record(UpsertRecordRequest {
            graph_id: graph.clone(),
            record: Some(matching_edge.clone()),
        })
        .await?;
    let response = search_graph(&fixture, &graph, query).await?;
    assert_eq!(response.graph.len(), 1);
    let path = &response.graph[0];
    assert_eq!(response.nodes[path.source as usize].value, "p");
    assert_eq!(response.nodes[path.destination as usize].value, "w2");
    Ok(())
}

#[sqlx::test]
async fn lpg_operation_batch_stops_after_first_failure(pool: PgPool) -> anyhow::Result<()> {
    let mut fixture = NidxFixture::new(pool).await?;
    let graph = shard(&mut fixture).await?;
    let operations = vec![
        LpgOperation {
            operation: Some(lpg_operation::Operation::UpsertRecord(LpgRecord {
                key: Some(key(LpgRecordKind::LpgNode, "applied")),
                labels: vec!["Node".into()],
                ..Default::default()
            })),
        },
        LpgOperation {
            operation: Some(lpg_operation::Operation::UpsertRecord(LpgRecord {
                key: Some(key(LpgRecordKind::LpgNode, "other")),
                labels: vec!["Node".into()],
                ..Default::default()
            })),
        },
        LpgOperation {
            operation: Some(lpg_operation::Operation::UpsertRecord(LpgRecord {
                key: Some(key(LpgRecordKind::LpgEdge, "linked")),
                source: Some("applied".into()),
                target: Some("other".into()),
                edge_type: Some("links".into()),
                ..Default::default()
            })),
        },
        LpgOperation {
            operation: Some(lpg_operation::Operation::UpsertRecord(LpgRecord {
                key: Some(key(LpgRecordKind::LpgEdge, "failed")),
                source: Some("missing".into()),
                target: Some("also-missing".into()),
                edge_type: Some("links".into()),
                ..Default::default()
            })),
        },
        LpgOperation {
            operation: Some(lpg_operation::Operation::UpsertRecord(LpgRecord {
                key: Some(key(LpgRecordKind::LpgNode, "skipped")),
                labels: vec!["Node".into()],
                ..Default::default()
            })),
        },
    ];
    let results = fixture
        .lpg_client
        .lpg_operation(Request::new(LpgOperationRequest {
            graph_id: graph.clone(),
            operations,
        }))
        .await?
        .into_inner()
        .results;
    assert_eq!(results.len(), 5);
    assert_eq!(results[0].status, LpgOperationStatus::Applied as i32);
    assert_eq!(results[1].status, LpgOperationStatus::Applied as i32);
    assert_eq!(results[2].status, LpgOperationStatus::Applied as i32);
    assert_eq!(results[3].status, LpgOperationStatus::Failed as i32);
    assert_eq!(results[3].error_code, "FailedPrecondition");
    assert_eq!(results[4].status, LpgOperationStatus::Skipped as i32);

    wait_for(
        &mut fixture,
        &graph,
        vec![
            key(LpgRecordKind::LpgNode, "applied"),
            key(LpgRecordKind::LpgNode, "other"),
            key(LpgRecordKind::LpgEdge, "linked"),
            key(LpgRecordKind::LpgEdge, "failed"),
            key(LpgRecordKind::LpgNode, "skipped"),
        ],
        vec![
            LpgRecord {
                key: Some(key(LpgRecordKind::LpgNode, "applied")),
                labels: vec!["Node".into()],
                ..Default::default()
            },
            LpgRecord {
                key: Some(key(LpgRecordKind::LpgNode, "other")),
                labels: vec!["Node".into()],
                ..Default::default()
            },
            LpgRecord {
                key: Some(key(LpgRecordKind::LpgEdge, "linked")),
                source: Some("applied".into()),
                target: Some("other".into()),
                edge_type: Some("links".into()),
                ..Default::default()
            },
        ],
    )
    .await;
    Ok(())
}

#[sqlx::test]
async fn lpg_rejects_invalid_records_and_missing_graphs(pool: PgPool) -> anyhow::Result<()> {
    let mut fixture = NidxFixture::new(pool).await?;
    let graph = shard(&mut fixture).await?;
    let invalid_edge = LpgRecord {
        key: Some(key(LpgRecordKind::LpgEdge, "e")),
        ..Default::default()
    };
    assert_eq!(
        fixture
            .lpg_client
            .upsert_record(UpsertRecordRequest {
                graph_id: graph.clone(),
                record: Some(invalid_edge),
            })
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    let invalid_property = LpgRecord {
        key: Some(key(LpgRecordKind::LpgNode, "n")),
        labels: vec!["Node".into()],
        properties: HashMap::from([(
            "x".into(),
            LpgProperty {
                value: Some(lpg_property::Value::Float(f64::NAN)),
            },
        )]),
        ..Default::default()
    };
    assert_eq!(
        fixture
            .lpg_client
            .upsert_record(UpsertRecordRequest {
                graph_id: graph,
                record: Some(invalid_property),
            })
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    assert_eq!(
        fixture
            .lpg_client
            .get_records(GetRecordsRequest {
                graph_id: uuid::Uuid::new_v4().to_string(),
                keys: vec![],
            })
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );
    Ok(())
}

#[sqlx::test]
async fn lpg_find_records_with_filters_and_pagination(pool: PgPool) -> anyhow::Result<()> {
    let mut fixture = NidxFixture::new(pool).await?;
    let graph = shard(&mut fixture).await?;
    let mut expected = Vec::new();
    for (id, price, category) in [("a", 10, "fruit"), ("b", 20, "fruit"), ("c", 30, "other")] {
        let record = LpgRecord {
            key: Some(key(LpgRecordKind::LpgNode, id)),
            labels: vec!["Item".into()],
            properties: HashMap::from([
                (
                    "price".into(),
                    LpgProperty {
                        value: Some(lpg_property::Value::Integer(price)),
                    },
                ),
                (
                    "category.with.dot".into(),
                    LpgProperty {
                        value: Some(lpg_property::Value::Text(category.into())),
                    },
                ),
            ]),
            ..Default::default()
        };
        fixture
            .lpg_client
            .upsert_record(UpsertRecordRequest {
                graph_id: graph.clone(),
                record: Some(record.clone()),
            })
            .await?;
        expected.push(record);
    }
    wait_for(
        &mut fixture,
        &graph,
        expected.iter().map(|r| r.key.clone().unwrap()).collect(),
        expected,
    )
    .await;
    fixture.wait_sync().await;
    let request = FindRecordsRequest {
        graph_id: graph,
        kind: LpgRecordKind::LpgNode as i32,
        page_size: 1,
        filters: vec![
            LpgPropertyFilter {
                property: "category.with.dot".into(),
                predicate: Some(lpg_property_filter::Predicate::Text("fruit".into())),
            },
            LpgPropertyFilter {
                property: "price".into(),
                predicate: Some(lpg_property_filter::Predicate::IntegerRange(LpgIntegerRange {
                    lower: Some(10),
                    upper: Some(20),
                })),
            },
        ],
        ..Default::default()
    };
    let cluster = SearcherCluster::new(
        fixture.settings.clone(),
        Topology {
            nodes: 2,
            shard_replication: 1,
            network: vec![NetworkAvailability::Connected; 2],
        },
    )
    .await?;
    let mut searcher_clients = Vec::new();
    for node in &cluster.searchers {
        searcher_clients
            .push(NidxSearcherClient::connect(format!("http://{}", node.as_ref().unwrap().address())).await?);
    }
    let mut collected = std::collections::BTreeSet::new();
    let mut token = String::new();
    for page in 0..3 {
        let next_request = FindRecordsRequest {
            page_token: token,
            ..request.clone()
        };
        let response = if page == 0 {
            fixture.lpg_client.find_records(next_request).await?.into_inner()
        } else {
            searcher_clients[(page - 1) % 2]
                .lpg_find_records(next_request)
                .await?
                .into_inner()
        };
        if page == 0 {
            assert!(!response.next_page_token.is_empty());
            for client in &mut searcher_clients {
                let direct = client.lpg_find_records(request.clone()).await?.into_inner();
                assert_eq!(direct, response);
                let next = client
                    .lpg_find_records(FindRecordsRequest {
                        page_token: direct.next_page_token,
                        ..request.clone()
                    })
                    .await?
                    .into_inner();
                assert_eq!(next.records.len(), 1);
                assert!(next.next_page_token.is_empty());
                assert_ne!(next.records[0].key, response.records[0].key);
            }
            let invalid = FindRecordsRequest {
                page_token: response.next_page_token.clone(),
                label: Some("another-label".into()),
                ..request.clone()
            };
            assert_eq!(
                searcher_clients[0].lpg_find_records(invalid).await.unwrap_err().code(),
                Code::InvalidArgument
            );
        }
        for record in response.records {
            assert!(collected.insert(record.key.unwrap().id));
        }
        token = response.next_page_token;
        if token.is_empty() {
            break;
        }
    }
    assert!(token.is_empty());
    assert_eq!(collected, ["a".to_string(), "b".to_string()].into());
    for client in &mut searcher_clients {
        assert_eq!(
            client
                .lpg_find_records(FindRecordsRequest {
                    page_size: 0,
                    ..request.clone()
                })
                .await
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );
        assert_eq!(
            client
                .lpg_find_records(FindRecordsRequest {
                    graph_id: uuid::Uuid::new_v4().to_string(),
                    ..request.clone()
                })
                .await
                .unwrap_err()
                .code(),
            Code::NotFound
        );
    }
    Ok(())
}

#[sqlx::test]
async fn lpg_expand_filters_batches_and_routes_to_shard_owner(pool: PgPool) -> anyhow::Result<()> {
    let mut fixture = NidxFixture::new(pool).await?;
    let graph = shard(&mut fixture).await?;
    let mut nodes = Vec::new();
    for (id, label, country) in [("p", "Product", "US"), ("w", "Warehouse", "ES"), ("t", "Other", "US")] {
        let node = LpgRecord {
            key: Some(key(LpgRecordKind::LpgNode, id)),
            labels: vec![label.into()],
            properties: HashMap::from([(
                "country".into(),
                LpgProperty {
                    value: Some(lpg_property::Value::Text(country.into())),
                },
            )]),
            ..Default::default()
        };
        fixture
            .lpg_client
            .upsert_record(UpsertRecordRequest {
                graph_id: graph.clone(),
                record: Some(node.clone()),
            })
            .await?;
        nodes.push(node);
    }
    wait_for(
        &mut fixture,
        &graph,
        nodes.iter().map(|node| node.key.clone().unwrap()).collect(),
        nodes,
    )
    .await;
    let mut edges = Vec::new();
    for (id, source, target, edge_type, confirmed) in [
        ("e1", "p", "w", "STOCKED_AT", true),
        ("e2", "p", "w", "STOCKED_AT", false),
        ("e3", "w", "p", "RETURNED_FROM", true),
        ("loop", "p", "p", "STOCKED_AT", true),
        ("other", "p", "t", "STOCKED_AT", true),
    ] {
        let edge = LpgRecord {
            key: Some(key(LpgRecordKind::LpgEdge, id)),
            source: Some(source.into()),
            target: Some(target.into()),
            edge_type: Some(edge_type.into()),
            properties: HashMap::from([(
                "confirmed".into(),
                LpgProperty {
                    value: Some(lpg_property::Value::Boolean(confirmed)),
                },
            )]),
            ..Default::default()
        };
        fixture
            .lpg_client
            .upsert_record(UpsertRecordRequest {
                graph_id: graph.clone(),
                record: Some(edge.clone()),
            })
            .await?;
        edges.push(edge);
    }
    wait_for(
        &mut fixture,
        &graph,
        edges.iter().map(|edge| edge.key.clone().unwrap()).collect(),
        edges,
    )
    .await;
    fixture.wait_sync().await;
    let request = ExpandRequest {
        graph_id: graph.clone(),
        node_ids: vec!["p".into(), "p".into()],
        direction: LpgDirection::LpgBoth as i32,
        ..Default::default()
    };
    let bindings = |response: &ExpandResponse| {
        response
            .matches
            .iter()
            .map(|m| {
                (
                    m.node_id.clone(),
                    response.edges[m.edge_index as usize].key.as_ref().unwrap().id.clone(),
                    response.neighbors[m.neighbor_index as usize]
                        .key
                        .as_ref()
                        .unwrap()
                        .id
                        .clone(),
                )
            })
            .collect::<std::collections::BTreeSet<_>>()
    };
    let public = fixture.lpg_client.expand(request.clone()).await?.into_inner();
    assert_eq!(
        (
            public.examined_edges,
            public.matches.len(),
            public.edges.len(),
            public.neighbors.len()
        ),
        (5, 5, 5, 3)
    );
    let path_request = ShortestPathRequest {
        graph_id: graph.clone(),
        source: "w".into(),
        target: "t".into(),
        direction: LpgDirection::LpgBoth as i32,
        edge_types: vec!["STOCKED_AT".into()],
        edge_filters: vec![LpgPropertyFilter {
            property: "confirmed".into(),
            predicate: Some(lpg_property_filter::Predicate::Boolean(true)),
        }],
        max_hops: 2,
        ..Default::default()
    };
    let public_path = fixture
        .lpg_client
        .shortest_path(path_request.clone())
        .await?
        .into_inner();
    assert!(public_path.found);
    assert_eq!(
        public_path
            .nodes
            .iter()
            .map(|node| node.key.as_ref().unwrap().id.as_str())
            .collect::<Vec<_>>(),
        ["w", "p", "t"]
    );
    assert_eq!(
        public_path
            .edges
            .iter()
            .map(|edge| edge.key.as_ref().unwrap().id.as_str())
            .collect::<Vec<_>>(),
        ["e1", "other"]
    );
    let cluster = SearcherCluster::new(
        fixture.settings.clone(),
        Topology {
            nodes: 2,
            shard_replication: 1,
            network: vec![NetworkAvailability::Connected; 2],
        },
    )
    .await?;
    for node in &cluster.searchers {
        let mut client = NidxSearcherClient::connect(format!("http://{}", node.as_ref().unwrap().address())).await?;
        let direct_path = client.lpg_shortest_path(path_request.clone()).await?.into_inner();
        assert_eq!(direct_path.nodes, public_path.nodes);
        assert_eq!(direct_path.edges, public_path.edges);
        assert!(
            !client
                .lpg_shortest_path(ShortestPathRequest {
                    max_hops: 1,
                    ..path_request.clone()
                })
                .await?
                .into_inner()
                .found
        );
        for (invalid, code) in [
            (
                ShortestPathRequest {
                    direction: 99,
                    ..path_request.clone()
                },
                Code::InvalidArgument,
            ),
            (
                ShortestPathRequest {
                    node_filter_scope: 99,
                    ..path_request.clone()
                },
                Code::InvalidArgument,
            ),
            (
                ShortestPathRequest {
                    max_hops: 65,
                    ..path_request.clone()
                },
                Code::InvalidArgument,
            ),
            (
                ShortestPathRequest {
                    source: "missing".into(),
                    ..path_request.clone()
                },
                Code::NotFound,
            ),
            (
                ShortestPathRequest {
                    max_visited_nodes: 1,
                    ..path_request.clone()
                },
                Code::ResourceExhausted,
            ),
            (
                ShortestPathRequest {
                    graph_id: uuid::Uuid::new_v4().to_string(),
                    ..path_request.clone()
                },
                Code::NotFound,
            ),
        ] {
            assert_eq!(client.lpg_shortest_path(invalid).await.unwrap_err().code(), code);
        }
        let direct = client.lpg_expand(request.clone()).await?.into_inner();
        assert_eq!(bindings(&direct), bindings(&public));
        let batched = client
            .lpg_expand(ExpandRequest {
                node_ids: vec!["p".into(), "w".into(), "p".into()],
                ..request.clone()
            })
            .await?
            .into_inner();
        assert_eq!(
            (batched.examined_edges, batched.matches.len(), batched.edges.len()),
            (5, 8, 5)
        );
        let incoming = client
            .lpg_expand(ExpandRequest {
                direction: LpgDirection::LpgIncoming as i32,
                ..request.clone()
            })
            .await?
            .into_inner();
        assert_eq!((incoming.examined_edges, incoming.matches.len()), (2, 2));
        let filtered = client
            .lpg_expand(ExpandRequest {
                edge_types: vec!["STOCKED_AT".into()],
                edge_filters: vec![LpgPropertyFilter {
                    property: "confirmed".into(),
                    predicate: Some(lpg_property_filter::Predicate::Boolean(true)),
                }],
                neighbor_label: Some("Warehouse".into()),
                neighbor_filters: vec![LpgPropertyFilter {
                    property: "country".into(),
                    predicate: Some(lpg_property_filter::Predicate::Text("ES".into())),
                }],
                ..request.clone()
            })
            .await?
            .into_inner();
        assert_eq!(filtered.examined_edges, 3);
        assert_eq!(bindings(&filtered), [("p".into(), "e1".into(), "w".into())].into());
        for limited in [
            ExpandRequest {
                candidate_limit: 4,
                neighbor_label: Some("Warehouse".into()),
                ..request.clone()
            },
            ExpandRequest {
                max_results: 4,
                ..request.clone()
            },
        ] {
            assert_eq!(
                client.lpg_expand(limited).await.unwrap_err().code(),
                Code::ResourceExhausted
            );
        }
        assert_eq!(
            client
                .lpg_expand(ExpandRequest {
                    node_ids: vec!["missing".into()],
                    ..request.clone()
                })
                .await
                .unwrap_err()
                .code(),
            Code::NotFound
        );
        assert_eq!(
            client
                .lpg_expand(ExpandRequest {
                    direction: 99,
                    ..request.clone()
                })
                .await
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );
        assert_eq!(
            client
                .lpg_expand(ExpandRequest {
                    graph_id: uuid::Uuid::new_v4().to_string(),
                    ..request.clone()
                })
                .await
                .unwrap_err()
                .code(),
            Code::NotFound
        );
    }
    Ok(())
}

#[sqlx::test]
async fn lpg_paginated_incident_edges_include_parallel_edges_and_self_loops(pool: PgPool) -> anyhow::Result<()> {
    let mut fixture = NidxFixture::new(pool).await?;
    let graph = shard(&mut fixture).await?;
    for id in ["a", "b"] {
        fixture
            .lpg_client
            .upsert_record(UpsertRecordRequest {
                graph_id: graph.clone(),
                record: Some(LpgRecord {
                    key: Some(key(LpgRecordKind::LpgNode, id)),
                    labels: vec!["Node".into()],
                    ..Default::default()
                }),
            })
            .await?;
    }
    let mut edges = Vec::new();
    for (id, source, target) in [("e1", "a", "b"), ("e2", "a", "b"), ("e3", "a", "a"), ("e4", "b", "b")] {
        let edge = LpgRecord {
            key: Some(key(LpgRecordKind::LpgEdge, id)),
            source: Some(source.into()),
            target: Some(target.into()),
            edge_type: Some("link".into()),
            ..Default::default()
        };
        fixture
            .lpg_client
            .upsert_record(UpsertRecordRequest {
                graph_id: graph.clone(),
                record: Some(edge.clone()),
            })
            .await?;
        edges.push(edge);
    }
    wait_for(
        &mut fixture,
        &graph,
        edges.iter().map(|r| r.key.clone().unwrap()).collect(),
        edges,
    )
    .await;
    fixture.wait_sync().await;
    let request = FindRecordsRequest {
        graph_id: graph,
        kind: LpgRecordKind::LpgEdge as i32,
        incident_node: Some("a".into()),
        page_size: 2,
        ..Default::default()
    };
    let mut collected = std::collections::BTreeSet::new();
    let mut token = String::new();
    for _ in 0..4 {
        let response = fixture
            .lpg_client
            .find_records(FindRecordsRequest {
                page_token: token,
                ..request.clone()
            })
            .await?
            .into_inner();
        for record in response.records {
            assert!(collected.insert(record.key.unwrap().id));
        }
        token = response.next_page_token;
        if token.is_empty() {
            break;
        }
    }
    assert!(token.is_empty());
    assert_eq!(collected, ["e1".to_string(), "e2".to_string(), "e3".to_string()].into());
    Ok(())
}

#[sqlx::test]
async fn lpg_node_deletion_requires_removing_incident_edges(pool: PgPool) -> anyhow::Result<()> {
    let mut fixture = NidxFixture::new(pool).await?;
    let graph = shard(&mut fixture).await?;
    let node = LpgRecord {
        key: Some(key(LpgRecordKind::LpgNode, "a")),
        labels: vec!["Node".into()],
        revision: "client-revision".into(),
        ..Default::default()
    };
    let edge = |id: &str, source: &str, target: &str| LpgRecord {
        key: Some(key(LpgRecordKind::LpgEdge, id)),
        source: Some(source.into()),
        target: Some(target.into()),
        edge_type: Some("link".into()),
        ..Default::default()
    };
    let upsert = |record| UpsertRecordRequest {
        graph_id: graph.clone(),
        record: Some(record),
    };
    assert_eq!(
        fixture
            .lpg_client
            .upsert_record(upsert(edge("missing", "a", "b")))
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    fixture.lpg_client.upsert_record(upsert(node.clone())).await?;
    fixture
        .lpg_client
        .upsert_record(upsert(LpgRecord {
            key: Some(key(LpgRecordKind::LpgNode, "b")),
            labels: vec!["Node".into()],
            ..Default::default()
        }))
        .await?;
    for record in [
        edge("e1", "a", "b"),
        edge("e2", "a", "b"),
        edge("e3", "b", "a"),
        edge("loop", "a", "a"),
        edge("keep", "b", "b"),
    ] {
        fixture.lpg_client.upsert_record(upsert(record)).await?;
    }
    wait_for(
        &mut fixture,
        &graph,
        vec![
            node.key.clone().unwrap(),
            key(LpgRecordKind::LpgEdge, "e1"),
            key(LpgRecordKind::LpgEdge, "e2"),
            key(LpgRecordKind::LpgEdge, "e3"),
            key(LpgRecordKind::LpgEdge, "loop"),
        ],
        vec![
            node,
            edge("e1", "a", "b"),
            edge("e2", "a", "b"),
            edge("e3", "b", "a"),
            edge("loop", "a", "a"),
        ],
    )
    .await;
    assert_eq!(
        fixture
            .lpg_client
            .delete_node(DeleteNodeRequest {
                graph_id: graph.clone(),
                id: "a".into(),
            })
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    assert_eq!(
        fixture
            .lpg_client
            .delete_record(DeleteRecordRequest {
                graph_id: graph.clone(),
                key: Some(key(LpgRecordKind::LpgNode, "a"))
            })
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    for id in ["e1", "e2", "loop"] {
        fixture
            .lpg_client
            .delete_record(DeleteRecordRequest {
                graph_id: graph.clone(),
                key: Some(key(LpgRecordKind::LpgEdge, id)),
            })
            .await?;
        assert_eq!(
            fixture
                .lpg_client
                .delete_node(DeleteNodeRequest {
                    graph_id: graph.clone(),
                    id: "a".into(),
                })
                .await
                .unwrap_err()
                .code(),
            Code::FailedPrecondition
        );
    }
    let operations = ["e3"]
        .into_iter()
        .map(|id| LpgOperation {
            operation: Some(lpg_operation::Operation::DeleteRecord(key(LpgRecordKind::LpgEdge, id))),
        })
        .chain([LpgOperation {
            operation: Some(lpg_operation::Operation::DeleteRecord(key(LpgRecordKind::LpgNode, "a"))),
        }])
        .collect();
    let results = fixture
        .lpg_client
        .lpg_operation(LpgOperationRequest {
            graph_id: graph.clone(),
            operations,
        })
        .await?
        .into_inner()
        .results;
    assert_eq!(results.len(), 2);
    assert!(
        results
            .iter()
            .all(|result| result.status == LpgOperationStatus::Applied as i32)
    );
    fixture
        .lpg_client
        .delete_node(DeleteNodeRequest {
            graph_id: graph.clone(),
            id: "a".into(),
        })
        .await?;
    let keys = vec![
        key(LpgRecordKind::LpgNode, "a"),
        key(LpgRecordKind::LpgEdge, "e1"),
        key(LpgRecordKind::LpgEdge, "e2"),
        key(LpgRecordKind::LpgEdge, "e3"),
        key(LpgRecordKind::LpgEdge, "loop"),
        key(LpgRecordKind::LpgEdge, "keep"),
    ];
    wait_for(&mut fixture, &graph, keys, vec![edge("keep", "b", "b")]).await;
    assert_eq!(
        fixture
            .lpg_client
            .delete_node(DeleteNodeRequest {
                graph_id: graph.clone(),
                id: "b".into(),
            })
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    assert_eq!(
        fixture
            .lpg_client
            .upsert_record(upsert(edge("new", "a", "b")))
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    Ok(())
}

#[sqlx::test]
async fn lpg_edge_upsert_and_node_deletion_are_serialized(pool: PgPool) -> anyhow::Result<()> {
    let mut fixture = NidxFixture::new(pool).await?;
    let graph = shard(&mut fixture).await?;
    let node = LpgRecord {
        key: Some(key(LpgRecordKind::LpgNode, "a")),
        labels: vec!["Node".into()],
        ..Default::default()
    };
    fixture
        .lpg_client
        .upsert_record(UpsertRecordRequest {
            graph_id: graph.clone(),
            record: Some(node),
        })
        .await?;
    let edge = LpgRecord {
        key: Some(key(LpgRecordKind::LpgEdge, "loop")),
        source: Some("a".into()),
        target: Some("a".into()),
        edge_type: Some("link".into()),
        ..Default::default()
    };
    let mut other = fixture.lpg_client.clone();
    let (deleted, inserted) = tokio::join!(
        fixture.lpg_client.delete_node(DeleteNodeRequest {
            graph_id: graph.clone(),
            id: "a".into(),
        }),
        other.upsert_record(UpsertRecordRequest {
            graph_id: graph.clone(),
            record: Some(edge.clone())
        }),
    );
    match (deleted, inserted) {
        (Ok(_), Err(error)) => {
            assert_eq!(error.code(), Code::FailedPrecondition);
            wait_for(
                &mut fixture,
                &graph,
                vec![key(LpgRecordKind::LpgNode, "a"), edge.key.unwrap()],
                vec![],
            )
            .await;
        }
        (Err(error), Ok(_)) => {
            assert_eq!(error.code(), Code::FailedPrecondition);
            let node = LpgRecord {
                key: Some(key(LpgRecordKind::LpgNode, "a")),
                labels: vec!["Node".into()],
                ..Default::default()
            };
            wait_for(
                &mut fixture,
                &graph,
                vec![node.key.clone().unwrap(), edge.key.clone().unwrap()],
                vec![node, edge],
            )
            .await;
        }
        _ => panic!("exactly one concurrent operation must succeed"),
    }
    Ok(())
}

#[sqlx::test]
async fn lpg_search_top_k_accepts_more_than_a_thousand_matches(pool: PgPool) -> anyhow::Result<()> {
    let mut fixture = NidxFixture::new(pool).await?;
    let graph = shard(&mut fixture).await?;
    let chunks = LpgTextProperty {
        chunks: vec!["apple".into(); 1002],
        embeddings: vec![
            LpgVector {
                values: vec![1.0, 0.0, 0.0]
            };
            1002
        ],
    };
    let records: Vec<_> = [("keep", true), ("exclude", false)]
        .into_iter()
        .map(|(id, eligible)| LpgRecord {
            key: Some(key(LpgRecordKind::LpgNode, id)),
            labels: vec!["Node".into()],
            properties: HashMap::from([(
                "eligible".into(),
                LpgProperty {
                    value: Some(lpg_property::Value::Boolean(eligible)),
                },
            )]),
            text_properties: HashMap::from([("description".into(), chunks.clone()), ("other".into(), chunks.clone())]),
            ..Default::default()
        })
        .collect();
    fixture
        .lpg_client
        .bulk_load(BulkLoadRequest {
            graph_id: graph.clone(),
            records: records.clone(),
            ..Default::default()
        })
        .await?;
    wait_for(
        &mut fixture,
        &graph,
        records.iter().map(|record| record.key.clone().unwrap()).collect(),
        records,
    )
    .await;
    fixture.wait_sync().await;
    for (text, vector) in [
        (Some("apple".to_string()), vec![]),
        (None, vec![1.0, 0.0, 0.0]),
        (Some("apple".to_string()), vec![1.0, 0.0, 0.0]),
    ] {
        let response = fixture
            .lpg_client
            .search_chunks(SearchChunksRequest {
                graph_id: graph.clone(),
                text: text.clone(),
                vector: vector.clone(),
                kind: Some(LpgRecordKind::LpgNode as i32),
                property: Some("description".into()),
                filters: vec![LpgPropertyFilter {
                    property: "eligible".into(),
                    predicate: Some(lpg_property_filter::Predicate::Boolean(true)),
                }],
                top_k: 10,
                ..Default::default()
            })
            .await?
            .into_inner();
        assert_eq!(response.hits.len(), 10);
        for hit in response.hits {
            assert_eq!(hit.record.unwrap().key.unwrap().id, "keep");
            assert_eq!(hit.property, "description");
            assert_eq!(hit.text, "apple");
            assert!(hit.text_rank.is_some() || hit.vector_rank.is_some());
            if text.is_none() {
                assert!(hit.text_rank.is_none());
                assert!(hit.vector_rank.is_some());
            }
            if vector.is_empty() {
                assert!(hit.text_rank.is_some());
                assert!(hit.vector_rank.is_none());
            }
        }
    }
    Ok(())
}

#[sqlx::test]
async fn lpg_text_chunks_search_and_patch(pool: PgPool) -> anyhow::Result<()> {
    let mut fixture = NidxFixture::new(pool).await?;
    let graph = shard(&mut fixture).await?;
    let node_key = key(LpgRecordKind::LpgNode, "node");
    let record = LpgRecord {
        key: Some(node_key.clone()),
        labels: vec!["Node".into()],
        text_properties: HashMap::from([
            (
                "description".into(),
                LpgTextProperty {
                    chunks: vec!["red apple".into()],
                    embeddings: vec![LpgVector {
                        values: vec![1.0, 0.0, 0.0],
                    }],
                },
            ),
            (
                "features".into(),
                LpgTextProperty {
                    chunks: vec!["sweet fruit".into()],
                    embeddings: vec![LpgVector {
                        values: vec![0.0, 1.0, 0.0],
                    }],
                },
            ),
        ]),
        ..Default::default()
    };
    fixture
        .lpg_client
        .upsert_record(UpsertRecordRequest {
            graph_id: graph.clone(),
            record: Some(record.clone()),
        })
        .await?;
    wait_for(&mut fixture, &graph, vec![node_key.clone()], vec![record]).await;
    fixture.wait_sync().await;
    let hybrid = fixture
        .lpg_client
        .search_chunks(SearchChunksRequest {
            graph_id: graph.clone(),
            text: Some("red apple".into()),
            vector: vec![1.0, 0.0, 0.0],
            property: Some("description".into()),
            top_k: 10,
            ..Default::default()
        })
        .await?
        .into_inner();
    assert_eq!(hybrid.hits.len(), 1);
    assert_eq!(hybrid.hits[0].text, "red apple");
    assert_eq!(hybrid.hits[0].text_rank, Some(1));
    assert_eq!(hybrid.hits[0].vector_rank, Some(1));

    let search = SearchChunksRequest {
        graph_id: graph.clone(),
        text: Some("red apple".into()),
        vector: vec![1.0, 0.0, 0.0],
        property: Some("description".into()),
        top_k: 10,
        ..Default::default()
    };
    for (semantic, keyword, text_rank, vector_rank) in [
        (0.5, 0.0, Some(1), Some(1)),
        (2.0, 0.0, Some(1), None),
        (0.0, 1_000_000.0, None, Some(1)),
    ] {
        let response = fixture
            .lpg_client
            .search_chunks(SearchChunksRequest {
                min_score_semantic: semantic,
                min_score_keyword: keyword,
                ..search.clone()
            })
            .await?
            .into_inner();
        assert_eq!(response.hits.len(), 1);
        assert_eq!(response.hits[0].text_rank, text_rank);
        assert_eq!(response.hits[0].vector_rank, vector_rank);
    }
    let excluded = fixture
        .lpg_client
        .search_chunks(SearchChunksRequest {
            min_score_semantic: 2.0,
            min_score_keyword: 1_000_000.0,
            ..search
        })
        .await?
        .into_inner();
    assert!(excluded.hits.is_empty());

    let updated = fixture
        .lpg_client
        .patch_text_properties(PatchTextPropertiesRequest {
            graph_id: graph.clone(),
            key: Some(node_key.clone()),
            properties: HashMap::from([(
                "description".into(),
                LpgTextPropertyPatch {
                    chunks: Some(LpgTextChunks {
                        chunks: vec!["blue berry".into()],
                    }),
                    embeddings: None,
                },
            )]),
            ..Default::default()
        })
        .await?
        .into_inner()
        .record
        .unwrap();
    wait_for(&mut fixture, &graph, vec![node_key], vec![updated]).await;
    fixture.wait_sync().await;
    let old = fixture
        .lpg_client
        .search_chunks(SearchChunksRequest {
            graph_id: graph.clone(),
            vector: vec![1.0, 0.0, 0.0],
            property: Some("description".into()),
            top_k: 10,
            ..Default::default()
        })
        .await?
        .into_inner();
    assert!(old.hits.is_empty());
    let kept = fixture
        .lpg_client
        .search_chunks(SearchChunksRequest {
            graph_id: graph,
            vector: vec![0.0, 1.0, 0.0],
            property: Some("features".into()),
            top_k: 10,
            ..Default::default()
        })
        .await?
        .into_inner();
    assert_eq!(kept.hits[0].text, "sweet fruit");
    Ok(())
}

#[sqlx::test]
async fn lpg_surfaces_downstream_errors(pool: PgPool) -> anyhow::Result<()> {
    let mut fixture = NidxFixture::new(pool).await?;
    let graph = shard(&mut fixture).await?;
    let mut settings = fixture.settings.clone();
    settings.settings.lpg = Some(LpgSettings {
        indexer_address: "http://127.0.0.1:1".into(),
        searcher_address: "http://127.0.0.1:1".into(),
    });
    let server = GrpcServer::new("localhost:0").await?;
    let port = server.port()?;
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(server.serve(ApiServer::new(&settings).into_router(), shutdown.clone()));
    let mut client = NidxLpgClient::connect(format!("http://localhost:{port}")).await?;
    let node_key = key(LpgRecordKind::LpgNode, "n");
    let error = client
        .upsert_record(UpsertRecordRequest {
            graph_id: graph.clone(),
            record: Some(LpgRecord {
                key: Some(node_key.clone()),
                labels: vec!["Node".into()],
                ..Default::default()
            }),
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::Unavailable);
    let error = client
        .get_records(GetRecordsRequest {
            graph_id: graph,
            keys: vec![node_key],
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::Unavailable);
    shutdown.cancel();
    task.await??;
    Ok(())
}
