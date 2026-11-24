// Copyright 2021 Bosutech XXI S.L.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use crate::common::services::NidxFixture;
use base64::{Engine, engine::general_purpose::STANDARD, engine::general_purpose::URL_SAFE_NO_PAD};
use nidx::api::grpc::ApiServer;
use nidx::grpc_server::GrpcServer;
use nidx::settings::LpgSettings;
use nidx_protos::nidx::nidx_indexer_client::NidxIndexerClient;
use nidx_protos::nidx::nidx_indexer_server::{NidxIndexer, NidxIndexerServer};
use nidx_protos::nidx::nidx_lpg_client::NidxLpgClient;
use nidx_protos::nidx::nidx_lpg_server::NidxLpg;
use nidx_protos::prost::*;
use nidx_protos::relation::RelationType;
use nidx_protos::{
    BulkLoadRequest, DeleteNodeRequest, DeleteRecordRequest, FindRecordsRequest, GetRecordsRequest, GraphQuery,
    GraphSearchRequest, IndexMessage, IndexMetadata, IndexParagraph, IndexParagraphs, IndexResourceRequest,
    JsonFieldValue, LpgIntegerRange, LpgOperation, LpgOperationRequest, LpgOperationResult, LpgOperationStatus,
    LpgProperty, LpgPropertyFilter, LpgRecord, LpgRecordKey, LpgRecordKind, LpgStringList, LpgTextChunks,
    LpgTextProperty, LpgTextPropertyPatch, LpgTextPropertyUpdate, LpgVector, NewShardRequest, OpStatus,
    PatchTextPropertiesRequest, Resource, ResourceId, SearchChunksRequest, TextInformation, UpsertRecordRequest,
    VectorIndexConfig, graph_query, lpg_operation, lpg_property, lpg_property_filter,
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
    };
    let mut invalid = request.clone();
    invalid.records.pop();
    assert_eq!(
        fixture.lpg_client.bulk_load(invalid).await.unwrap_err().code(),
        Code::FailedPrecondition
    );
    let mut invalid = request.clone();
    invalid.records[1].deleting = true;
    assert_eq!(
        fixture.lpg_client.bulk_load(invalid).await.unwrap_err().code(),
        Code::InvalidArgument
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
        }))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::Unavailable);
    assert!(error.message().contains("partially applied"));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        *kinds.lock().unwrap(),
        [LpgRecordKind::LpgNode as i32, LpgRecordKind::LpgEdge as i32]
    );
    broken
        .bulk_load(Request::new(BulkLoadRequest {
            graph_id: graph.clone(),
            records: records.clone(),
        }))
        .await?;
    assert_eq!(calls.load(Ordering::SeqCst), 4);
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
        ]
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
                    detach: request.detach,
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
        if let Ok(response) = result {
            if response.into_inner().records == expected {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("records did not become visible: {expected:?}");
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
    let mut collected = std::collections::BTreeSet::new();
    let mut token = String::new();
    for _ in 0..3 {
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
    assert_eq!(collected, ["a".to_string(), "b".to_string()].into());
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
async fn lpg_node_deletion_validates_endpoints_and_detaches(pool: PgPool) -> anyhow::Result<()> {
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
        edge("loop", "a", "a"),
        edge("keep", "b", "b"),
    ] {
        fixture.lpg_client.upsert_record(upsert(record)).await?;
    }
    wait_for(&mut fixture, &graph, vec![node.key.clone().unwrap()], vec![node]).await;
    assert_eq!(
        fixture
            .lpg_client
            .delete_node(DeleteNodeRequest {
                graph_id: graph.clone(),
                id: "a".into(),
                detach: false,
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
    fixture
        .lpg_client
        .delete_node(DeleteNodeRequest {
            graph_id: graph.clone(),
            id: "a".into(),
            detach: true,
        })
        .await?;
    fixture
        .lpg_client
        .delete_node(DeleteNodeRequest {
            graph_id: graph.clone(),
            id: "a".into(),
            detach: true,
        })
        .await?;
    let keys = vec![
        key(LpgRecordKind::LpgNode, "a"),
        key(LpgRecordKind::LpgEdge, "e1"),
        key(LpgRecordKind::LpgEdge, "e2"),
        key(LpgRecordKind::LpgEdge, "loop"),
        key(LpgRecordKind::LpgEdge, "keep"),
    ];
    wait_for(&mut fixture, &graph, keys, vec![edge("keep", "b", "b")]).await;
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
async fn lpg_indexed_deletion_marker_blocks_writes_and_cascade_resumes(pool: PgPool) -> anyhow::Result<()> {
    let mut fixture = NidxFixture::new(pool.clone()).await?;
    let graph = shard(&mut fixture).await?;
    let node_key = key(LpgRecordKind::LpgNode, "a");
    let node = LpgRecord {
        key: Some(node_key.clone()),
        labels: vec!["Node".into()],
        revision: "before".into(),
        ..Default::default()
    };
    fixture
        .lpg_client
        .upsert_record(UpsertRecordRequest {
            graph_id: graph.clone(),
            record: Some(node.clone()),
        })
        .await?;
    let edge = LpgRecord {
        key: Some(key(LpgRecordKind::LpgEdge, "loop")),
        source: Some("a".into()),
        target: Some("a".into()),
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

    // Simulate interruption after indexing the marker, before deleting incident edges.
    let namespace = uuid::Uuid::from_u128(0x958ee941_b31c_4d0b_9226_f28b07f98180);
    let rid = uuid::Uuid::new_v5(&namespace, format!("{graph}/{}:{}:a", node_key.kind, 1).as_bytes())
        .simple()
        .to_string();
    let marked = LpgRecord {
        deleting: true,
        revision: "deletion".into(),
        ..node
    };
    let encoded = STANDARD.encode(marked.encode_to_vec());
    let field = "a/lpg_record_v1";
    let end = encoded.len() as i32;
    let timestamp = nidx_protos::prost_types::Timestamp { seconds: 1, nanos: 0 };
    let resource = Resource {
        resource: Some(ResourceId { shard_id: graph.clone(), uuid: rid.clone() }),
        shard_id: graph.clone(),
        metadata: Some(IndexMetadata { created: Some(timestamp), modified: Some(timestamp) }),
        texts: HashMap::from([(field.into(), TextInformation { text: encoded, labels: vec![] })]),
        json_fields: HashMap::from([(field.into(), JsonFieldValue {
            value: r#"{"kind":1,"labels":["Node"],"source":null,"target":null,"edge_type":null,"properties":{},"fence":"marker-test"}"#.into()
        })]),
        paragraphs: HashMap::from([(field.into(), IndexParagraphs {
            paragraphs: HashMap::from([(format!("{rid}/{field}/0-{end}"), IndexParagraph {
                field: field.into(), start: 0, end, ..Default::default()
            })]),
        })]),
        texts_to_delete: vec![rid.clone()], paragraphs_to_delete: vec![rid.clone()],
        json_fields_to_delete: vec![rid.clone()], vectors_to_delete_in_all_vectorsets: vec![rid],
        ..Default::default()
    };
    let mut indexer =
        NidxIndexerClient::connect(fixture.settings.settings.lpg.as_ref().unwrap().indexer_address.clone()).await?;
    let status = indexer
        .index_resource(IndexResourceRequest {
            shard_id: graph.clone(),
            resource: Some(resource),
        })
        .await?
        .into_inner();
    assert_eq!(status.status, nidx_protos::op_status::Status::Ok as i32);
    wait_for(&mut fixture, &graph, vec![node_key.clone()], vec![marked]).await;
    assert_eq!(
        fixture
            .lpg_client
            .upsert_record(UpsertRecordRequest {
                graph_id: graph.clone(),
                record: Some(edge.clone())
            })
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    assert_eq!(
        fixture
            .lpg_client
            .patch_text_properties(PatchTextPropertiesRequest {
                graph_id: graph.clone(),
                key: Some(node_key.clone()),
                ..Default::default()
            })
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    assert_eq!(
        fixture
            .lpg_client
            .upsert_record(UpsertRecordRequest {
                graph_id: graph.clone(),
                record: Some(LpgRecord {
                    key: Some(node_key.clone()),
                    labels: vec!["Node".into()],
                    ..Default::default()
                }),
            })
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    // An already-started cascade resumes even if the retry omits detach.
    fixture
        .lpg_client
        .delete_node(DeleteNodeRequest {
            graph_id: graph.clone(),
            id: "a".into(),
            detach: false,
        })
        .await?;
    wait_for(&mut fixture, &graph, vec![node_key, edge.key.unwrap()], vec![]).await;
    Ok(())
}

#[sqlx::test]
async fn lpg_edge_upsert_and_detach_are_serialized(pool: PgPool) -> anyhow::Result<()> {
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
            detach: true
        }),
        other.upsert_record(UpsertRecordRequest {
            graph_id: graph.clone(),
            record: Some(edge.clone())
        }),
    );
    deleted?;
    if let Err(error) = inserted {
        assert_eq!(error.code(), Code::FailedPrecondition);
    }
    wait_for(
        &mut fixture,
        &graph,
        vec![key(LpgRecordKind::LpgNode, "a"), edge.key.unwrap()],
        vec![],
    )
    .await;
    Ok(())
}

#[sqlx::test]
async fn lpg_text_chunks_search_and_patch(pool: PgPool) -> anyhow::Result<()> {
    let mut fixture = NidxFixture::new(pool).await?;
    let graph = shard(&mut fixture).await?;
    let node_key = key(LpgRecordKind::LpgNode, "node");
    let record = LpgRecord {
        key: Some(node_key.clone()),
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
