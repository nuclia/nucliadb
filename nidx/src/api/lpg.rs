// Copyright 2021 Bosutech XXI S.L.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::{
    Engine,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use nidx_protos::nidx::nidx_indexer_client::NidxIndexerClient;
use nidx_protos::nidx::nidx_lpg_server::NidxLpg;
use nidx_protos::nidx::nidx_searcher_client::NidxSearcherClient;
use nidx_protos::prost::*;
use nidx_protos::relation::RelationType;
use nidx_protos::relation_node::NodeType;
use nidx_protos::*;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status, transport::Endpoint};
use uuid::Uuid;

use crate::{
    NidxMetadata,
    errors::NidxError,
    metadata::{Index, IndexKind, Shard},
    settings::LpgSettings,
};
use nidx_vector::config::VectorConfig;

const FIELD: &str = "a/lpg_record_v1";
const RELATION_FIELD: &str = "a/metadata";
const NAMESPACE: Uuid = Uuid::from_u128(0x958ee941_b31c_4d0b_9226_f28b07f98180);
const MAX_LPG_OPERATIONS: usize = 1000;
const MAX_EXPAND_RESPONSE_BYTES: usize = 3 * 1024 * 1024;

#[derive(Clone)]
struct PendingUpsert {
    request: IndexResourceRequest,
    rid: String,
    record: LpgRecord,
    fence: String,
}

fn pending_upsert(request: IndexResourceRequest) -> Result<PendingUpsert, Status> {
    let resource = request
        .resource
        .as_ref()
        .ok_or_else(|| Status::data_loss("missing pending resource"))?;
    let rid = resource
        .resource
        .as_ref()
        .ok_or_else(|| Status::data_loss("missing pending ID"))?
        .uuid
        .clone();
    let text = &resource
        .texts
        .get(FIELD)
        .ok_or_else(|| Status::data_loss("missing pending record"))?
        .text;
    let bytes = STANDARD
        .decode(text)
        .map_err(|_| Status::data_loss("invalid pending record"))?;
    let record = LpgRecord::decode(bytes.as_slice()).map_err(|_| Status::data_loss("invalid pending record"))?;
    let json: serde_json::Value = serde_json::from_str(
        &resource
            .json_fields
            .get(FIELD)
            .ok_or_else(|| Status::data_loss("missing pending fence"))?
            .value,
    )
    .map_err(|_| Status::data_loss("invalid pending fence"))?;
    let fence = json["fence"]
        .as_str()
        .ok_or_else(|| Status::data_loss("missing pending fence"))?
        .to_owned();
    Ok(PendingUpsert {
        request,
        rid,
        record,
        fence,
    })
}

async fn submit_then_wait<T: Clone, S, SF, V, VF>(
    items: &[T],
    visibility_budget: Duration,
    mut submit: S,
    mut visible: V,
) -> Result<(), Status>
where
    S: FnMut(T) -> SF,
    SF: std::future::Future<Output = Result<(), Status>>,
    V: FnMut(T) -> VF,
    VF: std::future::Future<Output = Result<(), Status>>,
{
    for item in items {
        submit(item.clone()).await?;
    }
    tokio::time::timeout(visibility_budget, async {
        for item in items {
            visible(item.clone()).await?;
        }
        Ok(())
    })
    .await
    .map_err(|_| Status::deadline_exceeded("LPG mutation was indexed but is not yet visible"))?
}

#[derive(Clone)]
pub struct LpgServer {
    meta: NidxMetadata,
    settings: Option<LpgSettings>,
    graph_locks: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
}

impl LpgServer {
    pub fn new(meta: NidxMetadata, settings: Option<LpgSettings>) -> Self {
        Self {
            meta,
            settings,
            graph_locks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn graph_lock(&self, graph: &str) -> Arc<Mutex<()>> {
        let mut locks = self.graph_locks.lock().await;
        locks.retain(|_, lock| Arc::strong_count(lock) > 1);
        locks.entry(graph.to_owned()).or_default().clone()
    }

    async fn graph(&self, id: &str) -> Result<String, Status> {
        let id = Uuid::parse_str(id).map_err(|_| Status::invalid_argument("invalid graph ID"))?;
        Shard::get(&self.meta.pool, id).await.map_err(NidxError::from)?;
        Ok(id.to_string())
    }

    fn endpoints(&self) -> Result<&LpgSettings, Status> {
        self.settings
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("LPG clients are not configured"))
    }

    async fn vectorset(&self, graph: &str, record: &LpgRecord) -> Result<Option<String>, Status> {
        if !record
            .text_properties
            .values()
            .any(|property| !property.embeddings.is_empty())
        {
            return Ok(None);
        }
        let (name, dimension) = self.vector_configuration(graph, &record.vectorset).await?;
        if record
            .text_properties
            .values()
            .flat_map(|p| &p.embeddings)
            .any(|v| v.values.len() != dimension)
        {
            return Err(Status::invalid_argument(format!(
                "embeddings must have {dimension} dimensions"
            )));
        }
        Ok(Some(name))
    }

    async fn vector_configuration(&self, graph: &str, requested: &str) -> Result<(String, usize), Status> {
        let shard_id = Uuid::parse_str(graph).map_err(|_| Status::invalid_argument("invalid graph ID"))?;
        let indexes = Index::for_shard(&self.meta.pool, shard_id)
            .await
            .map_err(NidxError::from)?;
        let indexes: Vec<_> = indexes
            .into_iter()
            .filter(|index| index.kind == IndexKind::Vector && (requested.is_empty() || index.name == requested))
            .collect();
        if indexes.len() != 1 {
            return Err(Status::invalid_argument("select an existing paragraph vectorset"));
        }
        let index = &indexes[0];
        let config: VectorConfig = index.config().map_err(NidxError::from)?;
        Ok((index.name.clone(), config.vector_type.dimension()))
    }

    async fn save_pending(&self, graph: &str, kind: i16, payload: Vec<u8>) -> Result<(), Status> {
        let id = Uuid::parse_str(graph).map_err(|_| Status::invalid_argument("invalid graph ID"))?;
        sqlx::query("INSERT INTO lpg_pending_mutations (graph_id, kind, payload) VALUES ($1, $2, $3)")
            .bind(id)
            .bind(kind)
            .bind(payload)
            .execute(&self.meta.pool)
            .await
            .map_err(NidxError::from)?;
        Ok(())
    }

    async fn recover_pending(&self, graph: &str) -> Result<(), Status> {
        let id = Uuid::parse_str(graph).map_err(|_| Status::invalid_argument("invalid graph ID"))?;
        let row: Option<(i16, Vec<u8>)> =
            sqlx::query_as("SELECT kind, payload FROM lpg_pending_mutations WHERE graph_id = $1")
                .bind(id)
                .fetch_optional(&self.meta.pool)
                .await
                .map_err(NidxError::from)?;
        let Some((kind, payload)) = row else {
            return Ok(());
        };
        let mut client = NidxIndexerClient::new(endpoint(&self.endpoints()?.indexer_address)?.connect_lazy());
        match kind {
            1 => {
                let request = IndexResourceRequest::decode(payload.as_slice())
                    .map_err(|_| Status::data_loss("invalid pending LPG upsert"))?;
                let pending = pending_upsert(request)?;
                check_index_status(client.index_resource(pending.request).await?.into_inner())?;
                self.wait_visible(graph, &pending.rid, Some((&pending.record, &pending.fence)))
                    .await?;
            }
            2 => {
                let request = IndexMessage::decode(payload.as_slice())
                    .map_err(|_| Status::data_loss("invalid pending LPG deletion"))?;
                check_index_status(client.index(request.clone()).await?.into_inner())?;
                self.wait_visible(graph, &request.resource, None).await?;
            }
            _ => return Err(Status::data_loss("unknown pending LPG mutation")),
        }
        sqlx::query("DELETE FROM lpg_pending_mutations WHERE graph_id = $1 AND kind = $2 AND payload = $3")
            .bind(id)
            .bind(kind)
            .bind(payload)
            .execute(&self.meta.pool)
            .await
            .map_err(NidxError::from)?;
        Ok(())
    }

    async fn wait_visible(&self, graph: &str, rid: &str, expected: Option<(&LpgRecord, &str)>) -> Result<(), Status> {
        let mut client = NidxSearcherClient::new(endpoint(&self.endpoints()?.searcher_address)?.connect_lazy());
        let wait = async {
            loop {
                let check = async {
                    let texts = client
                        .extracted_texts(ExtractedTextsRequest {
                            shard_id: graph.into(),
                            field_ids: vec![extracted_texts_request::FieldId {
                                rid: rid.into(),
                                field_type: "a".into(),
                                field_name: "lpg_record_v1".into(),
                                split: None,
                            }],
                            paragraph_ids: vec![],
                        })
                        .await?
                        .into_inner();
                    let value = texts.fields.get(&format!("{rid}/{FIELD}")).filter(|v| !v.is_empty());
                    let json_filter = expected.map(|(_, fence)| JsonFilterExpression {
                        expr: Some(json_filter_expression::Expr::Path(JsonFieldPathFilter {
                            field_id: FIELD.into(),
                            json_path: "fence".into(),
                            predicate: Some(json_field_path_filter::Predicate::Text(fence.into())),
                        })),
                    });
                    let search = client
                        .search(SearchRequest {
                            shard_ids: vec![graph.into()],
                            paragraph: true,
                            result_per_page: 1,
                            field_filter: Some(FilterExpression {
                                expr: Some(filter_expression::Expr::Resource(filter_expression::ResourceFilter {
                                    resource_id: rid.into(),
                                })),
                            }),
                            json_filter,
                            ..Default::default()
                        })
                        .await?
                        .into_inner();
                    if search.shard_ids != [graph.to_string()] {
                        return Err(Status::unavailable("incomplete shard search"));
                    }
                    let found = search.paragraph.as_ref().is_some_and(|p| !p.results.is_empty());
                    let relation_search = client
                        .graph_search(GraphSearchRequest {
                            shard_ids: vec![graph.into()],
                            query: Some(GraphQuery {
                                path: Some(graph_query::PathQuery {
                                    query: Some(graph_query::path_query::Query::Facet(graph_query::FacetFilter {
                                        facet: format!("/lpg/edge/{rid}"),
                                    })),
                                }),
                            }),
                            top_k: 2,
                            ..Default::default()
                        })
                        .await?
                        .into_inner();
                    if relation_search.shard_ids != [graph.to_string()] {
                        return Err(Status::unavailable("incomplete relation-index search"));
                    }
                    let relation_visible = match expected {
                        Some((record, _))
                            if record
                                .key
                                .as_ref()
                                .is_some_and(|key| key.kind == LpgRecordKind::LpgEdge as i32) =>
                        {
                            let Some(path) = relation_search.graph.first() else {
                                return Ok(false);
                            };
                            let Some(source) = relation_search.nodes.get(path.source as usize) else {
                                return Err(Status::data_loss("relation index returned an invalid source"));
                            };
                            let Some(target) = relation_search.nodes.get(path.destination as usize) else {
                                return Err(Status::data_loss("relation index returned an invalid target"));
                            };
                            let Some(relation) = relation_search.relations.get(path.relation as usize) else {
                                return Err(Status::data_loss("relation index returned an invalid edge"));
                            };
                            relation_search.graph.len() == 1
                                && source.value == record.source.as_deref().unwrap_or_default()
                                && target.value == record.target.as_deref().unwrap_or_default()
                                && relation.label == record.edge_type.as_deref().unwrap_or_default()
                                && path
                                    .facets
                                    .contains(&lpg_type_facet("edge", record.edge_type.as_deref().unwrap_or_default()))
                        }
                        _ => relation_search.graph.is_empty(),
                    };
                    match (expected, value) {
                        (None, None) => Ok(!found && relation_visible),
                        (Some((record, _)), Some(value)) => {
                            let bytes = STANDARD
                                .decode(value)
                                .map_err(|_| Status::data_loss("invalid LPG envelope"))?;
                            let actual = LpgRecord::decode(bytes.as_slice())
                                .map_err(|_| Status::data_loss("invalid LPG record"))?;
                            Ok(found && actual == *record && relation_visible)
                        }
                        _ => Ok(false),
                    }
                }
                .await;
                match check {
                    Ok(true) => return Ok(()),
                    Err(error) if !matches!(error.code(), tonic::Code::NotFound | tonic::Code::Unavailable) => {
                        return Err(error);
                    }
                    _ => tokio::time::sleep(Duration::from_millis(100)).await,
                }
            }
        };
        tokio::time::timeout(Duration::from_secs(30), wait)
            .await
            .map_err(|_| Status::deadline_exceeded("LPG mutation was indexed but is not yet visible"))?
    }

    async fn get_one(&self, graph: &str, key: LpgRecordKey) -> Result<Option<LpgRecord>, Status> {
        let wait = async {
            loop {
                match self
                    .get_records(Request::new(GetRecordsRequest {
                        graph_id: graph.into(),
                        keys: vec![key.clone()],
                    }))
                    .await
                {
                    Ok(response) => return Ok(response.into_inner().records.into_iter().next()),
                    Err(error) if error.code() == tonic::Code::NotFound => {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    Err(error) => return Err(error),
                }
            }
        };
        tokio::time::timeout(Duration::from_secs(30), wait)
            .await
            .map_err(|_| Status::deadline_exceeded("searcher did not become ready"))?
    }

    async fn edge_endpoint_labels(
        &self,
        graph: &str,
        id: &str,
        known_node_labels: Option<&HashMap<String, Vec<String>>>,
    ) -> Result<Vec<String>, Status> {
        if let Some(labels) = known_node_labels.and_then(|nodes| nodes.get(id)) {
            return Ok(labels.clone());
        }
        self.get_one(
            graph,
            LpgRecordKey {
                kind: LpgRecordKind::LpgNode as i32,
                id: id.into(),
            },
        )
        .await?
        .filter(|node| !node.deleting)
        .map(|node| node.labels)
        .ok_or_else(|| Status::failed_precondition("edge endpoint is missing or being deleted"))
    }

    async fn prepare_record(
        &self,
        graph: &str,
        record: &LpgRecord,
        known_node_labels: Option<&HashMap<String, Vec<String>>>,
    ) -> Result<IndexResourceRequest, Status> {
        let key = validate_record(record)?;
        let rid = resource_id(graph, key)?;
        let vectorset = self.vectorset(graph, record).await?;
        let endpoint_labels = if key.kind == LpgRecordKind::LpgEdge as i32 {
            Some((
                self.edge_endpoint_labels(graph, record.source.as_ref().unwrap(), known_node_labels)
                    .await?,
                self.edge_endpoint_labels(graph, record.target.as_ref().unwrap(), known_node_labels)
                    .await?,
            ))
        } else {
            None
        };
        let fence = Uuid::new_v4().simple().to_string();
        let resource = project(
            graph,
            record,
            &rid,
            vectorset.as_deref(),
            &fence,
            endpoint_labels
                .as_ref()
                .map(|(source, target)| (source.as_slice(), target.as_slice())),
        )?;
        let request = IndexResourceRequest {
            shard_id: graph.into(),
            resource: Some(resource),
        };
        let payload = request.encode_to_vec();
        if payload.len() > 4 * 1024 * 1024 {
            return Err(Status::resource_exhausted(
                "indexed resource exceeds gRPC message limit",
            ));
        }
        Ok(request)
    }

    async fn write_prepared(&self, graph: &str, request: IndexResourceRequest) -> Result<(), Status> {
        self.save_pending(graph, 1, request.encode_to_vec()).await?;
        self.recover_pending(graph).await
    }

    async fn write_record(&self, graph: &str, record: &LpgRecord) -> Result<(), Status> {
        let request = self.prepare_record(graph, record, None).await?;
        self.write_prepared(graph, request).await
    }

    async fn delete_one(&self, graph: &str, key: &LpgRecordKey) -> Result<(), Status> {
        let rid = resource_id(graph, key)?;
        let request = IndexMessage {
            shard: graph.into(),
            resource: rid,
            typemessage: TypeMessage::Deletion as i32,
            ..Default::default()
        };
        self.save_pending(graph, 2, request.encode_to_vec()).await?;
        self.recover_pending(graph).await
    }

    async fn put_locked(&self, graph: &str, record: &LpgRecord) -> Result<(), Status> {
        let key = validate_record(record)?;
        if record.deleting {
            return Err(Status::invalid_argument("deleting is reserved for DeleteNode"));
        }
        if let Some(existing) = self.get_one(graph, key.clone()).await? {
            if key.kind == LpgRecordKind::LpgNode as i32 && existing.deleting {
                return Err(Status::failed_precondition("node is being deleted"));
            }
            validate_immutable_types(&existing, record)?;
        }
        self.write_record(graph, record).await
    }

    async fn patch_text_properties_locked(
        &self,
        graph: &str,
        update: LpgTextPropertyUpdate,
    ) -> Result<LpgRecord, Status> {
        let key = update
            .key
            .ok_or_else(|| Status::invalid_argument("record key required"))?;
        let response = self
            .get_records(Request::new(GetRecordsRequest {
                graph_id: graph.into(),
                keys: vec![key.clone()],
            }))
            .await?
            .into_inner();
        let mut record = response
            .records
            .into_iter()
            .next()
            .ok_or_else(|| Status::not_found("record not found"))?;
        if record.deleting {
            return Err(Status::failed_precondition("node is being deleted"));
        }
        apply_text_patch(&mut record, update.properties)?;
        if let Some(vectorset) = update.vectorset {
            record.vectorset = vectorset;
        }
        self.put_locked(graph, &record).await?;
        Ok(record)
    }

    async fn apply_operation_locked(&self, graph: &str, operation: LpgOperation) -> Result<Option<LpgRecord>, Status> {
        match operation.operation {
            Some(lpg_operation::Operation::UpsertRecord(record)) => {
                self.put_locked(graph, &record).await?;
                Ok(None)
            }
            Some(lpg_operation::Operation::DeleteRecord(key)) => {
                resource_id(graph, &key)?;
                if key.kind == LpgRecordKind::LpgNode as i32 {
                    self.delete_node_locked(graph, &key.id, false).await?;
                } else {
                    self.delete_one(graph, &key).await?;
                }
                Ok(None)
            }
            Some(lpg_operation::Operation::DeleteNode(deletion)) => {
                self.delete_node_locked(graph, &deletion.id, deletion.detach).await?;
                Ok(None)
            }
            Some(lpg_operation::Operation::PatchTextProperties(update)) => {
                self.patch_text_properties_locked(graph, update).await.map(Some)
            }
            None => Err(Status::invalid_argument("operation required")),
        }
    }

    async fn incident_edges(&self, graph: &str, id: &str, page_size: u32) -> Result<FindRecordsResponse, Status> {
        self.find_records(Request::new(FindRecordsRequest {
            graph_id: graph.into(),
            kind: LpgRecordKind::LpgEdge as i32,
            incident_node: Some(id.into()),
            page_size,
            ..Default::default()
        }))
        .await
        .map(Response::into_inner)
    }

    async fn delete_node_locked(&self, graph: &str, id: &str, detach: bool) -> Result<(), Status> {
        let key = LpgRecordKey {
            kind: LpgRecordKind::LpgNode as i32,
            id: id.into(),
        };
        resource_id(graph, &key)?;
        let Some(mut node) = self.get_one(graph, key.clone()).await? else {
            return Ok(());
        };
        if !node.deleting {
            if !detach && !self.incident_edges(graph, id, 1).await?.records.is_empty() {
                return Err(Status::failed_precondition("node has incident edges; detach required"));
            }
            node.deleting = true;
            node.revision = Uuid::new_v4().simple().to_string();
            self.write_record(graph, &node).await?;
        }
        loop {
            let edges = self.incident_edges(graph, id, 1000).await?.records;
            if edges.is_empty() {
                break;
            }
            for edge in edges {
                let key = edge.key.ok_or_else(|| Status::data_loss("edge key missing"))?;
                self.delete_one(graph, &key).await?;
            }
        }
        self.delete_one(graph, &key).await
    }
}

fn endpoint(uri: &str) -> Result<Endpoint, Status> {
    Endpoint::from_shared(uri.to_owned())
        .map(|e| {
            e.connect_timeout(Duration::from_secs(5))
                .timeout(Duration::from_secs(30))
        })
        .map_err(|e| Status::failed_precondition(format!("invalid LPG endpoint: {e}")))
}

fn check_index_status(status: OpStatus) -> Result<(), Status> {
    if status.status != op_status::Status::Ok as i32 {
        return Err(Status::internal(format!(
            "indexer rejected operation: {}",
            status.detail
        )));
    }
    Ok(())
}

fn resource_id(graph: &str, key: &LpgRecordKey) -> Result<String, Status> {
    let kind = LpgRecordKind::try_from(key.kind).map_err(|_| Status::invalid_argument("invalid record kind"))?;
    if kind == LpgRecordKind::Unspecified || key.id.is_empty() {
        return Err(Status::invalid_argument("record kind and nonempty ID required"));
    }
    // Length prefix makes the kind and ID mapping unambiguous.
    let name = format!("{graph}/{}:{}:{}", key.kind, key.id.len(), key.id);
    Ok(Uuid::new_v5(&NAMESPACE, name.as_bytes()).simple().to_string())
}

fn validate_record(record: &LpgRecord) -> Result<&LpgRecordKey, Status> {
    let key = record
        .key
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("record key required"))?;
    match LpgRecordKind::try_from(key.kind) {
        Ok(LpgRecordKind::LpgNode)
            if record.source.is_none() && record.target.is_none() && record.edge_type.is_none() =>
        {
            if record.labels.is_empty() {
                return Err(Status::invalid_argument("nodes require at least one label"));
            }
        }
        Ok(LpgRecordKind::LpgEdge)
            if record.labels.is_empty()
                && [&record.source, &record.target, &record.edge_type]
                    .iter()
                    .all(|v| v.as_ref().is_some_and(|s| !s.is_empty())) => {}
        Ok(LpgRecordKind::LpgEdge) if !record.labels.is_empty() => {
            return Err(Status::invalid_argument("edges use edge_type and cannot have labels"));
        }
        _ => return Err(Status::invalid_argument("invalid node or edge fields")),
    }
    if record.labels.iter().any(String::is_empty) {
        return Err(Status::invalid_argument("labels must be nonempty"));
    }
    if record.labels.iter().collect::<HashSet<_>>().len() != record.labels.len() {
        return Err(Status::invalid_argument("node labels must be unique"));
    }
    for property in record.properties.values() {
        use lpg_property::Value;
        match property.value.as_ref() {
            None | Some(Value::NullValue(false)) => return Err(Status::invalid_argument("property value required")),
            Some(Value::Float(v)) if !v.is_finite() => return Err(Status::invalid_argument("float must be finite")),
            Some(Value::FloatRange(v))
                if v.lower.is_some_and(|n| !n.is_finite())
                    || v.upper.is_some_and(|n| !n.is_finite())
                    || matches!((v.lower, v.upper), (Some(a), Some(b)) if a > b) =>
            {
                return Err(Status::invalid_argument("invalid float range"));
            }
            Some(Value::IntegerRange(v)) if matches!((v.lower, v.upper), (Some(a), Some(b)) if a > b) => {
                return Err(Status::invalid_argument("invalid integer range"));
            }
            _ => {}
        }
    }
    for property in record.text_properties.values() {
        if property.chunks.is_empty() || property.chunks.iter().any(|chunk| chunk.trim().is_empty()) {
            return Err(Status::invalid_argument("text properties require nonempty chunks"));
        }
        if !property.embeddings.is_empty() && property.embeddings.len() != property.chunks.len() {
            return Err(Status::invalid_argument("one embedding per chunk is required"));
        }
        for embedding in &property.embeddings {
            if embedding.values.is_empty()
                || embedding.values.iter().any(|v| !v.is_finite())
                || embedding.values.iter().all(|v| *v == 0.0)
            {
                return Err(Status::invalid_argument("embedding must be finite and nonzero"));
            }
        }
    }
    Ok(key)
}

fn validate_immutable_types(existing: &LpgRecord, incoming: &LpgRecord) -> Result<(), Status> {
    let kind = existing.key.as_ref().map(|key| key.kind);
    if kind == Some(LpgRecordKind::LpgNode as i32)
        && existing.labels.iter().map(String::as_str).collect::<HashSet<_>>()
            != incoming.labels.iter().map(String::as_str).collect::<HashSet<_>>()
    {
        return Err(Status::failed_precondition(
            "node types are immutable; delete and recreate the node",
        ));
    }
    if kind == Some(LpgRecordKind::LpgEdge as i32) && existing.edge_type != incoming.edge_type {
        return Err(Status::failed_precondition(
            "edge type is immutable; delete and recreate the edge",
        ));
    }
    Ok(())
}

fn bulk_order(records: &[LpgRecord]) -> Result<Vec<usize>, Status> {
    if records.is_empty() || records.len() > MAX_LPG_OPERATIONS {
        return Err(Status::invalid_argument(format!(
            "records must contain between 1 and {MAX_LPG_OPERATIONS} items"
        )));
    }
    let mut keys = HashSet::new();
    let mut nodes = HashSet::new();
    let mut order = Vec::with_capacity(records.len());
    for (index, record) in records.iter().enumerate() {
        let key = validate_record(record)?;
        resource_id("", key)?;
        if record.deleting {
            return Err(Status::invalid_argument("deleting is reserved for DeleteNode"));
        }
        if !keys.insert((key.kind, key.id.clone())) {
            return Err(Status::invalid_argument("duplicate record key in bulk request"));
        }
        if key.kind == LpgRecordKind::LpgNode as i32 {
            nodes.insert(key.id.clone());
            order.push(index);
        }
    }
    for (index, record) in records.iter().enumerate() {
        if record.key.as_ref().unwrap().kind == LpgRecordKind::LpgEdge as i32 {
            for id in [&record.source, &record.target] {
                let id = id.as_ref().unwrap();
                if !nodes.contains(id) {
                    return Err(Status::failed_precondition(
                        "bulk edge endpoints must be nodes in this request",
                    ));
                }
            }
            order.push(index);
        }
    }
    Ok(order)
}

fn property_path(name: &str) -> String {
    let hex: String = name.as_bytes().iter().map(|b| format!("{b:02x}")).collect();
    format!("p_{hex}")
}

fn indexed_properties(record: &LpgRecord) -> serde_json::Value {
    use lpg_property::Value;
    let properties = record
        .properties
        .iter()
        .filter_map(|(name, property)| {
            let value = match property.value.as_ref()? {
                Value::Text(v) => serde_json::json!(v),
                Value::Integer(v) => serde_json::json!(v),
                Value::Float(v) => serde_json::json!(v),
                Value::Boolean(v) => serde_json::json!(v),
                Value::Strings(v) => serde_json::json!(v.values),
                _ => return None,
            };
            Some((property_path(name), value))
        })
        .collect::<serde_json::Map<_, _>>();
    serde_json::json!({
        "kind": record.key.as_ref().map(|key| key.kind).unwrap_or_default(),
        "labels": record.labels,
        "source": record.source,
        "target": record.target,
        "edge_type": record.edge_type,
        "properties": properties,
    })
}

fn project(
    graph: &str,
    record: &LpgRecord,
    rid: &str,
    vectorset: Option<&str>,
    fence: &str,
    endpoint_labels: Option<(&[String], &[String])>,
) -> Result<Resource, Status> {
    let encoded = STANDARD.encode(record.encode_to_vec());
    let end = i32::try_from(encoded.len()).map_err(|_| Status::invalid_argument("record is too large"))?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| Status::internal(e.to_string()))?;
    let timestamp = nidx_protos::prost_types::Timestamp {
        seconds: now.as_secs() as i64,
        nanos: now.subsec_nanos() as i32,
    };
    let mut field_relations = HashMap::new();
    let mut relation_fields_to_delete = Vec::new();
    if record.key.as_ref().unwrap().kind == LpgRecordKind::LpgEdge as i32 {
        let (source_labels, target_labels) =
            endpoint_labels.ok_or_else(|| Status::failed_precondition("edge endpoint labels are required"))?;
        let edge_type = record.edge_type.as_ref().unwrap();
        let mut facets = vec![format!("/lpg/edge/{rid}"), lpg_type_facet("edge", edge_type)];
        facets.extend(source_labels.iter().map(|label| lpg_type_facet("source", label)));
        facets.extend(target_labels.iter().map(|label| lpg_type_facet("target", label)));
        field_relations.insert(
            RELATION_FIELD.into(),
            IndexRelations {
                relations: vec![IndexRelation {
                    relation: Some(Relation {
                        source: Some(RelationNode {
                            value: record.source.as_ref().unwrap().clone(),
                            ntype: NodeType::Resource as i32,
                            ..Default::default()
                        }),
                        to: Some(RelationNode {
                            value: record.target.as_ref().unwrap().clone(),
                            ntype: NodeType::Resource as i32,
                            ..Default::default()
                        }),
                        relation: RelationType::Other as i32,
                        relation_label: edge_type.clone(),
                        ..Default::default()
                    }),
                    facets,
                    ..Default::default()
                }],
            },
        );
        relation_fields_to_delete.push(RELATION_FIELD.into());
    }
    let mut resource = Resource {
        resource: Some(ResourceId {
            shard_id: graph.to_owned(),
            uuid: rid.to_owned(),
        }),
        shard_id: graph.to_owned(),
        metadata: Some(IndexMetadata {
            created: Some(timestamp),
            modified: Some(timestamp),
        }),
        texts: HashMap::from([(
            FIELD.to_owned(),
            TextInformation {
                text: encoded,
                labels: vec![],
            },
        )]),
        json_fields: HashMap::from([(
            FIELD.to_owned(),
            JsonFieldValue {
                value: {
                    let mut json = indexed_properties(record);
                    json["fence"] = serde_json::Value::String(fence.into());
                    json.to_string()
                },
            },
        )]),
        paragraphs: HashMap::from([(
            FIELD.to_owned(),
            IndexParagraphs {
                paragraphs: HashMap::from([(
                    format!("{rid}/{FIELD}/0-{end}"),
                    IndexParagraph {
                        field: FIELD.to_owned(),
                        start: 0,
                        end,
                        ..Default::default()
                    },
                )]),
            },
        )]),
        texts_to_delete: vec![rid.to_owned()],
        paragraphs_to_delete: vec![rid.to_owned()],
        relation_fields_to_delete,
        field_relations,
        json_fields_to_delete: vec![rid.to_owned()],
        vectors_to_delete_in_all_vectorsets: vec![rid.to_owned()],
        ..Default::default()
    };
    for (property_name, property) in &record.text_properties {
        let field = format!("t/{}", property_path(property_name));
        resource.texts.insert(
            field.clone(),
            TextInformation {
                text: property.chunks.join("\n"),
                labels: vec![],
            },
        );
        let mut paragraphs = HashMap::new();
        let mut start = 0usize;
        for (index, chunk) in property.chunks.iter().enumerate() {
            let end = start
                .checked_add(chunk.chars().count())
                .ok_or_else(|| Status::invalid_argument("text too large"))?;
            let pstart = i32::try_from(start).map_err(|_| Status::invalid_argument("text too large"))?;
            let pend = i32::try_from(end).map_err(|_| Status::invalid_argument("text too large"))?;
            let id = format!("{rid}/{field}/{start}-{end}");
            let mut paragraph = IndexParagraph {
                field: field.clone(),
                start: pstart,
                end: pend,
                index: index as u64,
                ..Default::default()
            };
            if let Some(embedding) = property.embeddings.get(index) {
                let vectorset =
                    vectorset.ok_or_else(|| Status::invalid_argument("vectorset required for embeddings"))?;
                paragraph.vectorsets_sentences.insert(
                    vectorset.into(),
                    VectorsetSentences {
                        sentences: HashMap::from([(
                            id.clone(),
                            VectorSentence {
                                vector: embedding.values.clone(),
                                ..Default::default()
                            },
                        )]),
                    },
                );
            }
            paragraphs.insert(id, paragraph);
            start = end
                .checked_add(1)
                .ok_or_else(|| Status::invalid_argument("text too large"))?;
        }
        resource.paragraphs.insert(field, IndexParagraphs { paragraphs });
    }
    Ok(resource)
}

fn lpg_type_facet(position: &str, value: &str) -> String {
    format!("/lpg/type/{position}/{}", URL_SAFE_NO_PAD.encode(value.as_bytes()))
}

fn filter_expression(kind: i32, filters: &[LpgPropertyFilter]) -> Result<JsonFilterExpression, Status> {
    use json_field_path_filter::{FloatRangePredicate, IntegerRangePredicate, Predicate};
    use json_filter_expression::{Expr, List};
    use lpg_property_filter::Predicate as Filter;
    let kind = LpgRecordKind::try_from(kind).map_err(|_| Status::invalid_argument("invalid record kind"))?;
    let path = |json_path, predicate| JsonFilterExpression {
        expr: Some(Expr::Path(JsonFieldPathFilter {
            field_id: FIELD.into(),
            json_path,
            predicate: Some(predicate),
        })),
    };
    let mut operands = Vec::new();
    if kind != LpgRecordKind::Unspecified {
        operands.push(path("kind".into(), Predicate::Int(kind as i64)));
    }
    for filter in filters {
        let predicate = match filter.predicate.as_ref() {
            Some(Filter::Text(v)) => Predicate::Text(v.clone()),
            Some(Filter::Integer(v)) => Predicate::Int(*v),
            Some(Filter::Float(v)) if v.is_finite() => Predicate::Float(*v),
            Some(Filter::Boolean(v)) => Predicate::Boolean(*v),
            Some(Filter::IntegerRange(v))
                if (v.lower.is_some() || v.upper.is_some())
                    && !matches!((v.lower, v.upper), (Some(a), Some(b)) if a > b) =>
            {
                Predicate::IntRange(IntegerRangePredicate {
                    lower: v.lower,
                    upper: v.upper,
                })
            }
            Some(Filter::FloatRange(v))
                if (v.lower.is_some() || v.upper.is_some())
                    && v.lower.is_none_or(f64::is_finite)
                    && v.upper.is_none_or(f64::is_finite)
                    && !matches!((v.lower, v.upper), (Some(a), Some(b)) if a > b) =>
            {
                Predicate::FloatRange(FloatRangePredicate {
                    lower: v.lower,
                    upper: v.upper,
                })
            }
            _ => return Err(Status::invalid_argument("invalid property filter")),
        };
        operands.push(path(
            format!("properties.{}", property_path(&filter.property)),
            predicate,
        ));
    }
    Ok(JsonFilterExpression {
        expr: Some(Expr::BoolAnd(List { operands })),
    })
}

fn find_filter(req: &FindRecordsRequest) -> Result<JsonFilterExpression, Status> {
    use json_field_path_filter::Predicate;
    use json_filter_expression::{Expr, List};
    if req.kind == LpgRecordKind::Unspecified as i32 {
        return Err(Status::invalid_argument("record kind required"));
    }
    let mut expression = filter_expression(req.kind, &req.filters)?;
    let Some(Expr::BoolAnd(ref mut operands)) = expression.expr else {
        return Err(Status::internal("invalid filter expression"));
    };
    let path = |name: &str, value: &str| JsonFilterExpression {
        expr: Some(Expr::Path(JsonFieldPathFilter {
            field_id: FIELD.into(),
            json_path: name.into(),
            predicate: Some(Predicate::Text(value.into())),
        })),
    };
    for (name, value) in [
        ("labels", &req.label),
        ("source", &req.source),
        ("target", &req.target),
        ("edge_type", &req.edge_type),
    ] {
        if let Some(value) = value {
            if value.is_empty() {
                return Err(Status::invalid_argument("filter values must be nonempty"));
            }
            if name == "labels" && req.kind != LpgRecordKind::LpgNode as i32 {
                return Err(Status::invalid_argument("label filters require node kind"));
            }
            if name != "labels" && req.kind != LpgRecordKind::LpgEdge as i32 {
                return Err(Status::invalid_argument("endpoint and type filters require edge kind"));
            }
            operands.operands.push(path(name, value));
        }
    }
    if let Some(node) = &req.incident_node {
        if node.is_empty() || req.kind != LpgRecordKind::LpgEdge as i32 {
            return Err(Status::invalid_argument(
                "incident_node requires edge kind and a nonempty ID",
            ));
        }
        operands.operands.push(JsonFilterExpression {
            expr: Some(Expr::BoolOr(List {
                operands: vec![path("source", node), path("target", node)],
            })),
        });
    }
    Ok(expression)
}

fn expand_filter(req: &ExpandRequest) -> Result<JsonFilterExpression, Status> {
    use json_filter_expression::{Expr, List};
    let direction =
        LpgDirection::try_from(req.direction).map_err(|_| Status::invalid_argument("invalid expansion direction"))?;
    if direction == LpgDirection::Unspecified {
        return Err(Status::invalid_argument("expansion direction required"));
    }
    if req.node_ids.is_empty() || req.node_ids.len() > 1000 || req.node_ids.iter().any(String::is_empty) {
        return Err(Status::invalid_argument("node_ids requires 1-1000 nonempty IDs"));
    }
    if req.edge_types.len() > 1000 || req.edge_types.iter().any(String::is_empty) {
        return Err(Status::invalid_argument(
            "edge_types requires at most 1000 nonempty types",
        ));
    }
    if req.neighbor_label.as_ref().is_some_and(String::is_empty) {
        return Err(Status::invalid_argument("neighbor_label must be nonempty"));
    }
    filter_expression(LpgRecordKind::LpgNode as i32, &req.neighbor_filters)?;
    let mut expression = filter_expression(LpgRecordKind::LpgEdge as i32, &req.edge_filters)?;
    let Some(Expr::BoolAnd(ref mut operands)) = expression.expr else {
        return Err(Status::internal("invalid expansion filter"));
    };
    let text = |name: &str, value: &str| JsonFilterExpression {
        expr: Some(Expr::Path(JsonFieldPathFilter {
            field_id: FIELD.into(),
            json_path: name.into(),
            predicate: Some(json_field_path_filter::Predicate::Text(value.into())),
        })),
    };
    let mut endpoints = Vec::new();
    for id in req.node_ids.iter().collect::<HashSet<_>>() {
        if matches!(direction, LpgDirection::LpgOutgoing | LpgDirection::LpgBoth) {
            endpoints.push(text("source", id));
        }
        if matches!(direction, LpgDirection::LpgIncoming | LpgDirection::LpgBoth) {
            endpoints.push(text("target", id));
        }
    }
    operands.operands.push(JsonFilterExpression {
        expr: Some(Expr::BoolOr(List { operands: endpoints })),
    });
    if !req.edge_types.is_empty() {
        operands.operands.push(JsonFilterExpression {
            expr: Some(Expr::BoolOr(List {
                operands: req.edge_types.iter().map(|value| text("edge_type", value)).collect(),
            })),
        });
    }
    Ok(expression)
}

fn matches_property(record: &LpgRecord, filter: &LpgPropertyFilter) -> bool {
    use lpg_property::Value;
    use lpg_property_filter::Predicate;
    let value = record
        .properties
        .get(&filter.property)
        .and_then(|property| property.value.as_ref());
    match (value, filter.predicate.as_ref()) {
        (Some(Value::Text(a)), Some(Predicate::Text(b))) => a == b,
        (Some(Value::Strings(a)), Some(Predicate::Text(b))) => a.values.contains(b),
        (Some(Value::Boolean(a)), Some(Predicate::Boolean(b))) => a == b,
        (Some(Value::Integer(a)), Some(Predicate::Integer(b))) => a == b,
        (Some(Value::Float(a)), Some(Predicate::Float(b))) => a == b,
        (Some(Value::Integer(a)), Some(Predicate::IntegerRange(b))) => {
            b.lower.is_none_or(|lower| *a >= lower) && b.upper.is_none_or(|upper| *a <= upper)
        }
        (Some(Value::Float(a)), Some(Predicate::FloatRange(b))) => {
            b.lower.is_none_or(|lower| *a >= lower) && b.upper.is_none_or(|upper| *a <= upper)
        }
        _ => false,
    }
}

fn apply_text_patch(record: &mut LpgRecord, patches: HashMap<String, LpgTextPropertyPatch>) -> Result<(), Status> {
    if patches.is_empty() {
        return Err(Status::invalid_argument("text property patch required"));
    }
    for (name, patch) in patches {
        if patch.chunks.is_none() && patch.embeddings.is_none() {
            return Err(Status::invalid_argument("chunks or embeddings required"));
        }
        match patch.chunks {
            Some(chunks) if chunks.chunks.is_empty() => {
                if patch
                    .embeddings
                    .as_ref()
                    .is_some_and(|vectors| !vectors.vectors.is_empty())
                {
                    return Err(Status::invalid_argument("removed property cannot have embeddings"));
                }
                record.text_properties.remove(&name);
            }
            Some(chunks) => {
                record.text_properties.insert(
                    name,
                    LpgTextProperty {
                        chunks: chunks.chunks,
                        embeddings: patch.embeddings.map_or_else(Vec::new, |vectors| vectors.vectors),
                    },
                );
            }
            None => {
                let property = record
                    .text_properties
                    .get_mut(&name)
                    .ok_or_else(|| Status::not_found("embedding target has no text property"))?;
                property.embeddings = patch.embeddings.unwrap().vectors;
            }
        }
    }
    validate_record(record)?;
    Ok(())
}

#[derive(Default)]
struct ChunkCandidate {
    rid: String,
    field: String,
    start: usize,
    end: usize,
    text_rank: Option<u32>,
    vector_rank: Option<u32>,
}

fn chunk_candidate(id: &str) -> Result<ChunkCandidate, Status> {
    let parts: Vec<_> = id.split('/').collect();
    if parts.len() != 4 || parts[1] != "t" || !parts[2].starts_with("p_") {
        return Err(Status::data_loss("invalid chunk ID"));
    }
    let (start, end) = parts[3]
        .split_once('-')
        .ok_or_else(|| Status::data_loss("invalid chunk ID"))?;
    let start = start.parse().map_err(|_| Status::data_loss("invalid chunk offset"))?;
    let end = end.parse().map_err(|_| Status::data_loss("invalid chunk offset"))?;
    if start >= end {
        return Err(Status::data_loss("invalid chunk offset"));
    }
    Ok(ChunkCandidate {
        rid: parts[0].into(),
        field: format!("{}/{}", parts[1], parts[2]),
        start,
        end,
        ..Default::default()
    })
}

#[tonic::async_trait]
impl NidxLpg for LpgServer {
    async fn bulk_load(&self, request: Request<BulkLoadRequest>) -> Result<Response<LpgOperationResponse>, Status> {
        let req = request.into_inner();
        let graph = self.graph(&req.graph_id).await?;
        let lock = self.graph_lock(&graph).await;
        let _guard = lock.lock().await;
        let graph_id = Uuid::parse_str(&graph).unwrap();
        let pending: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM lpg_pending_mutations WHERE graph_id = $1)")
                .bind(graph_id)
                .fetch_one(&self.meta.pool)
                .await
                .map_err(NidxError::from)?;
        if pending {
            return Err(Status::failed_precondition(
                "ordinary LPG mutation is pending; recover it before bulk loading",
            ));
        }
        let order = bulk_order(&req.records)?;
        let node_labels = req
            .records
            .iter()
            .filter_map(|record| {
                let key = record.key.as_ref()?;
                (key.kind == LpgRecordKind::LpgNode as i32).then(|| (key.id.clone(), record.labels.clone()))
            })
            .collect::<HashMap<_, _>>();
        let mut prepared = Vec::with_capacity(req.records.len());
        for record in &req.records {
            let key = record.key.as_ref().unwrap();
            if let Some(existing) = self.get_one(&graph, key.clone()).await? {
                validate_immutable_types(&existing, record)?;
            }
            prepared.push(pending_upsert(
                self.prepare_record(&graph, record, Some(&node_labels)).await?,
            )?);
        }
        let prepared: Vec<_> = order.into_iter().map(|index| prepared[index].clone()).collect();
        let client = NidxIndexerClient::new(endpoint(&self.endpoints()?.indexer_address)?.connect_lazy());
        submit_then_wait(
            &prepared,
            Duration::from_secs(30),
            |item| {
                let mut client = client.clone();
                async move { check_index_status(client.index_resource(item.request).await?.into_inner()) }
            },
            |item| {
                let server = self.clone();
                let graph = graph.clone();
                async move {
                    server
                        .wait_visible(&graph, &item.rid, Some((&item.record, &item.fence)))
                        .await
                }
            },
        )
        .await
        .map_err(|error| {
            Status::new(
                error.code(),
                format!(
                    "bulk load may be partially applied or have an uncertain outcome: {}",
                    error.message()
                ),
            )
        })?;
        let results = vec![
            LpgOperationResult {
                status: LpgOperationStatus::Applied as i32,
                ..Default::default()
            };
            req.records.len()
        ];
        Ok(Response::new(LpgOperationResponse { results }))
    }

    async fn search_chunks(
        &self,
        request: Request<SearchChunksRequest>,
    ) -> Result<Response<SearchChunksResponse>, Status> {
        let req = request.into_inner();
        if req.text.as_ref().is_some_and(|text| text.trim().is_empty()) || (req.text.is_none() && req.vector.is_empty())
        {
            return Err(Status::invalid_argument("nonempty text or vector required"));
        }
        if !req.vector.is_empty() && (req.vector.iter().any(|v| !v.is_finite()) || req.vector.iter().all(|v| *v == 0.0))
        {
            return Err(Status::invalid_argument("vector must be finite and nonzero"));
        }
        if req.kind == Some(LpgRecordKind::Unspecified as i32) {
            return Err(Status::invalid_argument("invalid record kind"));
        }
        let limit = if req.candidate_limit == 0 {
            1000
        } else {
            req.candidate_limit
        };
        if limit > 10000 || req.top_k == 0 || req.top_k > limit {
            return Err(Status::invalid_argument("invalid top_k or candidate_limit"));
        }
        let rrf_k = if req.rrf_k == 0 { 60 } else { req.rrf_k };
        let filter = if req.kind.is_some() || !req.filters.is_empty() {
            Some(filter_expression(req.kind.unwrap_or_default(), &req.filters)?)
        } else {
            None
        };
        let graph = self.graph(&req.graph_id).await?;
        let vectorset = if req.vector.is_empty() {
            String::new()
        } else {
            let (name, dimension) = self.vector_configuration(&graph, &req.vectorset).await?;
            if req.vector.len() != dimension {
                return Err(Status::invalid_argument(format!(
                    "vector must have {dimension} dimensions"
                )));
            }
            name
        };
        let field_filter = req.property.as_ref().map(|property| FilterExpression {
            expr: Some(filter_expression::Expr::Field(filter_expression::FieldFilter {
                field_type: "t".into(),
                field_id: Some(property_path(property)),
            })),
        });
        let mut client = NidxSearcherClient::new(endpoint(&self.endpoints()?.searcher_address)?.connect_lazy());
        let response = client
            .search(SearchRequest {
                shard_ids: vec![graph.clone()],
                body: req.text.clone().unwrap_or_default(),
                paragraph: req.text.is_some(),
                vector: req.vector.clone(),
                vectorset,
                result_per_page: (limit + 1) as i32,
                with_duplicates: true,
                json_filter: filter,
                field_filter,
                ..Default::default()
            })
            .await?
            .into_inner();
        if response.shard_ids != [graph.clone()] {
            return Err(Status::unavailable("incomplete shard search"));
        }
        let mut candidates: HashMap<String, ChunkCandidate> = HashMap::new();
        if let Some(paragraphs) = response.paragraph {
            if paragraphs.next_page {
                return Err(Status::resource_exhausted("text candidate limit exceeded"));
            }
            let mut rank = 0u32;
            for hit in paragraphs.results {
                let field = hit.field.trim_matches('/');
                if !field.starts_with("t/p_") {
                    continue;
                }
                rank += 1;
                if rank > limit {
                    return Err(Status::resource_exhausted("text candidate limit exceeded"));
                }
                let id = format!("{}/{field}/{}-{}", hit.uuid, hit.start, hit.end);
                let mut candidate = chunk_candidate(&id)?;
                candidate.text_rank = Some(rank);
                candidates.insert(id, candidate);
            }
        }
        if let Some(vectors) = response.vector {
            if vectors.documents.len() > limit as usize {
                return Err(Status::resource_exhausted("vector candidate limit exceeded"));
            }
            for (index, hit) in vectors.documents.into_iter().enumerate() {
                let id = hit.doc_id.ok_or_else(|| Status::data_loss("missing vector ID"))?.id;
                if !candidates.contains_key(&id) {
                    candidates.insert(id.clone(), chunk_candidate(&id)?);
                }
                candidates.get_mut(&id).unwrap().vector_rank = Some(index as u32 + 1);
            }
        }
        if candidates.is_empty() {
            return Ok(Response::new(SearchChunksResponse::default()));
        }
        let ids: std::collections::HashSet<_> = candidates.values().map(|c| c.rid.clone()).collect();
        let texts = client
            .extracted_texts(ExtractedTextsRequest {
                shard_id: graph.clone(),
                field_ids: ids
                    .iter()
                    .map(|rid| extracted_texts_request::FieldId {
                        rid: rid.clone(),
                        field_type: "a".into(),
                        field_name: "lpg_record_v1".into(),
                        split: None,
                    })
                    .collect(),
                paragraph_ids: vec![],
            })
            .await?
            .into_inner();
        let mut records = HashMap::new();
        for rid in ids {
            let value = texts
                .fields
                .get(&format!("{rid}/{FIELD}"))
                .filter(|v| !v.is_empty())
                .ok_or_else(|| Status::unavailable("search candidate disappeared"))?;
            let bytes = STANDARD
                .decode(value)
                .map_err(|_| Status::data_loss("invalid LPG envelope"))?;
            let record = LpgRecord::decode(bytes.as_slice()).map_err(|_| Status::data_loss("invalid LPG record"))?;
            let key = record
                .key
                .as_ref()
                .ok_or_else(|| Status::data_loss("record key missing"))?;
            if resource_id(&graph, key).map_err(|_| Status::data_loss("invalid record key"))? != rid
                || req.kind.is_some_and(|kind| key.kind != kind)
            {
                return Err(Status::data_loss("search candidate identity mismatch"));
            }
            records.insert(rid, record);
        }
        let mut hits = Vec::new();
        for candidate in candidates.into_values() {
            let record = &records[&candidate.rid];
            let mut matched = None;
            for (name, property) in &record.text_properties {
                if format!("t/{}", property_path(name)) != candidate.field {
                    continue;
                }
                let mut start = 0usize;
                for (index, chunk) in property.chunks.iter().enumerate() {
                    let end = start + chunk.chars().count();
                    if start == candidate.start && end == candidate.end {
                        matched = Some((name.clone(), index as u32, chunk.clone()));
                        break;
                    }
                    start = end + 1;
                }
            }
            let (property, chunk_index, text) = matched.ok_or_else(|| Status::unavailable("stale search candidate"))?;
            if req.property.as_ref().is_some_and(|requested| requested != &property) {
                return Err(Status::data_loss("search returned unexpected property"));
            }
            let score = candidate
                .text_rank
                .map_or(0.0, |rank| 1.0 / (rrf_k as f64 + rank as f64))
                + candidate
                    .vector_rank
                    .map_or(0.0, |rank| 1.0 / (rrf_k as f64 + rank as f64));
            hits.push(LpgChunkHit {
                record: Some(record.clone()),
                property,
                chunk_index,
                text,
                score,
                text_rank: candidate.text_rank,
                vector_rank: candidate.vector_rank,
            });
        }
        hits.sort_by(|a, b| {
            b.score
                .total_cmp(&a.score)
                .then_with(|| {
                    a.record
                        .as_ref()
                        .unwrap()
                        .key
                        .as_ref()
                        .unwrap()
                        .id
                        .cmp(&b.record.as_ref().unwrap().key.as_ref().unwrap().id)
                })
                .then_with(|| a.property.cmp(&b.property))
                .then_with(|| a.chunk_index.cmp(&b.chunk_index))
        });
        hits.truncate(req.top_k as usize);
        Ok(Response::new(SearchChunksResponse { hits }))
    }

    async fn find_records(
        &self,
        request: Request<FindRecordsRequest>,
    ) -> Result<Response<FindRecordsResponse>, Status> {
        let req = request.into_inner();
        let filter = find_filter(&req)?;
        if !(1..=1000).contains(&req.page_size) {
            return Err(Status::invalid_argument("page_size must be between 1 and 1000"));
        }
        Ok(Response::new(self.find_page(req, filter).await?))
    }

    async fn expand(&self, request: Request<ExpandRequest>) -> Result<Response<ExpandResponse>, Status> {
        let req = request.into_inner();
        let filter = expand_filter(&req)?;
        let candidate_limit = if req.candidate_limit == 0 {
            1000
        } else {
            req.candidate_limit
        };
        let max_results = if req.max_results == 0 { 1000 } else { req.max_results };
        if candidate_limit > 10000 || max_results > 10000 {
            return Err(Status::invalid_argument("expansion limits must not exceed 10000"));
        }
        let graph = self.graph(&req.graph_id).await?;
        let starts: HashSet<_> = req.node_ids.iter().cloned().collect();
        let start_keys = starts
            .iter()
            .map(|id| LpgRecordKey {
                kind: LpgRecordKind::LpgNode as i32,
                id: id.clone(),
            })
            .collect();
        let mut nodes: HashMap<_, _> = self
            .get_records(Request::new(GetRecordsRequest {
                graph_id: graph.clone(),
                keys: start_keys,
            }))
            .await?
            .into_inner()
            .records
            .into_iter()
            .map(|record| (record.key.as_ref().unwrap().id.clone(), record))
            .collect();
        if nodes.len() != starts.len() {
            return Err(Status::not_found("expansion starting node not found"));
        }
        if nodes.values().any(|node| node.deleting) {
            return Err(Status::failed_precondition("expansion starting node is being deleted"));
        }
        let candidates = self
            .find_page(
                FindRecordsRequest {
                    graph_id: graph.clone(),
                    kind: LpgRecordKind::LpgEdge as i32,
                    page_size: candidate_limit + 1,
                    ..Default::default()
                },
                filter,
            )
            .await?;
        if !candidates.next_page_token.is_empty() || candidates.records.len() > candidate_limit as usize {
            return Err(Status::resource_exhausted("expansion candidate limit exceeded"));
        }
        let direction = LpgDirection::try_from(req.direction).unwrap();
        let mut bindings = Vec::new();
        let mut neighbors = HashSet::new();
        for (index, edge) in candidates.records.iter().enumerate() {
            let source = edge
                .source
                .as_ref()
                .ok_or_else(|| Status::data_loss("edge source missing"))?;
            let target = edge
                .target
                .as_ref()
                .ok_or_else(|| Status::data_loss("edge target missing"))?;
            let outgoing =
                matches!(direction, LpgDirection::LpgOutgoing | LpgDirection::LpgBoth) && starts.contains(source);
            let incoming =
                matches!(direction, LpgDirection::LpgIncoming | LpgDirection::LpgBoth) && starts.contains(target);
            if outgoing {
                bindings.push((source.clone(), index, target.clone()));
                neighbors.insert(target.clone());
            }
            if incoming && !(outgoing && source == target) {
                bindings.push((target.clone(), index, source.clone()));
                neighbors.insert(source.clone());
            }
            if !outgoing && !incoming {
                return Err(Status::data_loss("expansion returned an unrelated edge"));
            }
        }
        let keys: Vec<_> = neighbors
            .iter()
            .filter(|id| !nodes.contains_key(*id))
            .map(|id| LpgRecordKey {
                kind: LpgRecordKind::LpgNode as i32,
                id: id.clone(),
            })
            .collect();
        for chunk in keys.chunks(128) {
            let response = self
                .get_records(Request::new(GetRecordsRequest {
                    graph_id: graph.clone(),
                    keys: chunk.to_vec(),
                }))
                .await?
                .into_inner();
            if response.records.len() != chunk.len() {
                return Err(Status::unavailable("expansion neighbor disappeared"));
            }
            for record in response.records {
                nodes.insert(record.key.as_ref().unwrap().id.clone(), record);
            }
        }
        if neighbors.iter().any(|id| nodes[id].deleting) {
            return Err(Status::unavailable("expansion neighbor is being deleted"));
        }
        let mut response = ExpandResponse {
            examined_edges: candidates.records.len() as u32,
            ..Default::default()
        };
        let mut edge_indexes = HashMap::new();
        let mut neighbor_indexes = HashMap::new();
        // All response tables use a one-byte field tag and a varint message length.
        let entry_len = |len: usize| {
            let mut length_bytes = 1;
            let mut remaining = len;
            while remaining >= 128 {
                length_bytes += 1;
                remaining >>= 7;
            }
            1 + length_bytes + len
        };
        let mut response_bytes = response.encoded_len();
        for (node_id, index, neighbor_id) in bindings {
            let neighbor = &nodes[&neighbor_id];
            if req
                .neighbor_label
                .as_ref()
                .is_some_and(|label| !neighbor.labels.contains(label))
                || !req
                    .neighbor_filters
                    .iter()
                    .all(|filter| matches_property(neighbor, filter))
            {
                continue;
            }
            if response.matches.len() == max_results as usize {
                return Err(Status::resource_exhausted("expansion result limit exceeded"));
            }
            let edge_index = *edge_indexes.entry(index).or_insert_with(|| {
                let i = response.edges.len() as u32;
                response_bytes += entry_len(candidates.records[index].encoded_len());
                response.edges.push(candidates.records[index].clone());
                i
            });
            let neighbor_index = *neighbor_indexes.entry(neighbor_id).or_insert_with(|| {
                let i = response.neighbors.len() as u32;
                response_bytes += entry_len(neighbor.encoded_len());
                response.neighbors.push(neighbor.clone());
                i
            });
            let binding = ExpandMatch {
                node_id,
                edge_index,
                neighbor_index,
            };
            response_bytes += entry_len(binding.encoded_len());
            if response_bytes > MAX_EXPAND_RESPONSE_BYTES {
                return Err(Status::resource_exhausted("expansion response byte limit exceeded"));
            }
            response.matches.push(binding);
        }
        Ok(Response::new(response))
    }

    async fn lpg_operation(
        &self,
        request: Request<LpgOperationRequest>,
    ) -> Result<Response<LpgOperationResponse>, Status> {
        let req = request.into_inner();
        if req.operations.is_empty() || req.operations.len() > MAX_LPG_OPERATIONS {
            return Err(Status::invalid_argument(format!(
                "operations must contain between 1 and {MAX_LPG_OPERATIONS} items"
            )));
        }
        let graph = self.graph(&req.graph_id).await?;
        let lock = self.graph_lock(&graph).await;
        let _guard = lock.lock().await;
        self.recover_pending(&graph).await?;
        let mut results = Vec::with_capacity(req.operations.len());
        let mut failed = false;
        for operation in req.operations {
            if failed {
                results.push(LpgOperationResult {
                    status: LpgOperationStatus::Skipped as i32,
                    ..Default::default()
                });
                continue;
            }
            match self.apply_operation_locked(&graph, operation).await {
                Ok(record) => results.push(LpgOperationResult {
                    status: LpgOperationStatus::Applied as i32,
                    record,
                    ..Default::default()
                }),
                Err(error) => {
                    failed = true;
                    results.push(LpgOperationResult {
                        status: LpgOperationStatus::Failed as i32,
                        error_code: format!("{:?}", error.code()),
                        error_message: error.message().to_string(),
                        ..Default::default()
                    });
                }
            }
        }
        Ok(Response::new(LpgOperationResponse { results }))
    }

    async fn get_records(&self, request: Request<GetRecordsRequest>) -> Result<Response<GetRecordsResponse>, Status> {
        let req = request.into_inner();
        let graph = self.graph(&req.graph_id).await?;
        let ids = req
            .keys
            .iter()
            .map(|key| resource_id(&graph, key))
            .collect::<Result<Vec<_>, _>>()?;
        let mut client = NidxSearcherClient::new(endpoint(&self.endpoints()?.searcher_address)?.connect_lazy());
        let fields = ids
            .iter()
            .map(|rid| extracted_texts_request::FieldId {
                rid: rid.clone(),
                field_type: "a".into(),
                field_name: "lpg_record_v1".into(),
                split: None,
            })
            .collect();
        let response = client
            .extracted_texts(ExtractedTextsRequest {
                shard_id: graph,
                field_ids: fields,
                paragraph_ids: vec![],
            })
            .await?
            .into_inner();
        let mut records = Vec::new();
        for (rid, key) in ids.iter().zip(&req.keys) {
            if let Some(value) = response.fields.get(&format!("{rid}/{FIELD}")).filter(|v| !v.is_empty()) {
                let bytes = STANDARD
                    .decode(value)
                    .map_err(|_| Status::data_loss("invalid LPG envelope"))?;
                let record =
                    LpgRecord::decode(bytes.as_slice()).map_err(|_| Status::data_loss("invalid LPG record"))?;
                if record.key.as_ref() != Some(key) {
                    return Err(Status::data_loss("LPG record key mismatch"));
                }

                records.push(record);
            }
        }
        Ok(Response::new(GetRecordsResponse { records }))
    }
}

impl LpgServer {
    async fn find_page(
        &self,
        req: FindRecordsRequest,
        filter: JsonFilterExpression,
    ) -> Result<FindRecordsResponse, Status> {
        let cursor = if req.page_token.is_empty() {
            None
        } else {
            let bytes = STANDARD
                .decode(&req.page_token)
                .map_err(|_| Status::invalid_argument("invalid page token"))?;
            let cursor =
                SearchAfter::decode(bytes.as_slice()).map_err(|_| Status::invalid_argument("invalid page token"))?;
            if !cursor.score.is_finite() {
                return Err(Status::invalid_argument("invalid page token"));
            }
            Some(cursor)
        };
        let graph = self.graph(&req.graph_id).await?;
        let mut client = NidxSearcherClient::new(endpoint(&self.endpoints()?.searcher_address)?.connect_lazy());
        let response = client
            .search(SearchRequest {
                shard_ids: vec![graph.clone()],
                paragraph: true,
                result_per_page: req.page_size as i32,
                json_filter: Some(filter),
                field_filter: Some(FilterExpression {
                    expr: Some(filter_expression::Expr::Field(filter_expression::FieldFilter {
                        field_type: "a".into(),
                        field_id: Some("lpg_record_v1".into()),
                    })),
                }),
                search_after: cursor,
                ..Default::default()
            })
            .await?
            .into_inner();
        if response.shard_ids != [graph.clone()] {
            return Err(Status::unavailable("incomplete shard search"));
        }
        let Some(paragraphs) = response.paragraph else {
            return Ok(FindRecordsResponse::default());
        };
        let mut next_page_token = String::new();
        if paragraphs.next_page {
            let last = paragraphs
                .results
                .last()
                .ok_or_else(|| Status::data_loss("missing pagination result"))?;
            let Some(paragraph_result::SortValue::Score(score)) = &last.sort_value else {
                return Err(Status::data_loss("missing pagination score"));
            };
            next_page_token = STANDARD.encode(
                SearchAfter {
                    score: score.bm25,
                    shard_id: last.shard_id.clone(),
                    docaddr: score.docaddr,
                }
                .encode_to_vec(),
            );
        }
        let fields = paragraphs
            .results
            .iter()
            .map(|hit| extracted_texts_request::FieldId {
                rid: hit.uuid.clone(),
                field_type: "a".into(),
                field_name: "lpg_record_v1".into(),
                split: None,
            })
            .collect();
        let texts = client
            .extracted_texts(ExtractedTextsRequest {
                shard_id: graph.clone(),
                field_ids: fields,
                paragraph_ids: vec![],
            })
            .await?
            .into_inner();
        let mut records = Vec::with_capacity(paragraphs.results.len());
        for hit in paragraphs.results {
            let value = texts
                .fields
                .get(&format!("{}/{FIELD}", hit.uuid))
                .filter(|value| !value.is_empty())
                .ok_or_else(|| Status::unavailable("record disappeared during search"))?;
            let bytes = STANDARD
                .decode(value)
                .map_err(|_| Status::data_loss("invalid LPG envelope"))?;
            let record = LpgRecord::decode(bytes.as_slice()).map_err(|_| Status::data_loss("invalid LPG record"))?;
            let key = record
                .key
                .as_ref()
                .ok_or_else(|| Status::data_loss("record key missing"))?;
            if key.kind != req.kind
                || resource_id(&graph, key).map_err(|_| Status::data_loss("invalid record key"))? != hit.uuid
            {
                return Err(Status::data_loss("LPG record identity mismatch"));
            }
            records.push(record);
        }
        Ok(FindRecordsResponse {
            records,
            next_page_token,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expansion_rejects_unknown_directions_and_oversized_frontiers() {
        for request in [
            ExpandRequest {
                node_ids: vec!["a".into()],
                direction: 99,
                ..Default::default()
            },
            ExpandRequest {
                node_ids: vec!["a".into(); 1001],
                direction: LpgDirection::LpgBoth as i32,
                ..Default::default()
            },
        ] {
            assert_eq!(
                expand_filter(&request).unwrap_err().code(),
                tonic::Code::InvalidArgument
            );
        }
    }

    #[test]
    fn expansion_neighbor_filters_preserve_scalar_types_and_null_semantics() {
        use lpg_property::Value;
        use lpg_property_filter::Predicate;
        let record = LpgRecord {
            properties: HashMap::from([
                (
                    "integer".into(),
                    LpgProperty {
                        value: Some(Value::Integer(1)),
                    },
                ),
                (
                    "float".into(),
                    LpgProperty {
                        value: Some(Value::Float(1.5)),
                    },
                ),
                (
                    "boolean".into(),
                    LpgProperty {
                        value: Some(Value::Boolean(true)),
                    },
                ),
                (
                    "null".into(),
                    LpgProperty {
                        value: Some(Value::NullValue(true)),
                    },
                ),
                (
                    "tags".into(),
                    LpgProperty {
                        value: Some(Value::Strings(LpgStringList {
                            values: vec!["a".into()],
                        })),
                    },
                ),
            ]),
            ..Default::default()
        };
        for (property, predicate, expected) in [
            ("integer", Predicate::Integer(1), true),
            ("integer", Predicate::Boolean(true), false),
            ("boolean", Predicate::Integer(1), false),
            ("boolean", Predicate::Boolean(true), true),
            ("float", Predicate::Float(1.5), true),
            ("float", Predicate::Integer(1), false),
            ("null", Predicate::Text("null".into()), false),
            ("missing", Predicate::Boolean(false), false),
            ("tags", Predicate::Text("a".into()), true),
            ("tags", Predicate::Text("b".into()), false),
        ] {
            assert_eq!(
                matches_property(
                    &record,
                    &LpgPropertyFilter {
                        property: property.into(),
                        predicate: Some(predicate),
                    }
                ),
                expected,
                "{property}"
            );
        }
    }

    #[tokio::test]
    async fn batch_submits_every_item_before_checking_visibility() {
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        submit_then_wait(
            &[1, 2, 3],
            Duration::from_secs(30),
            |item| {
                let events = events.clone();
                async move {
                    events.lock().unwrap().push(("submit", item));
                    Ok(())
                }
            },
            |item| {
                let events = events.clone();
                async move {
                    events.lock().unwrap().push(("visible", item));
                    Ok(())
                }
            },
        )
        .await
        .unwrap();
        assert_eq!(
            *events.lock().unwrap(),
            [
                ("submit", 1),
                ("submit", 2),
                ("submit", 3),
                ("visible", 1),
                ("visible", 2),
                ("visible", 3)
            ]
        );
    }

    #[tokio::test]
    async fn batch_propagates_submission_and_visibility_failures() {
        for fail_submit in [true, false] {
            let events = Arc::new(std::sync::Mutex::new(Vec::new()));
            let error = submit_then_wait(
                &[1, 2, 3],
                Duration::from_secs(30),
                |item| {
                    let events = events.clone();
                    async move {
                        events.lock().unwrap().push(("submit", item));
                        if fail_submit && item == 2 {
                            Err(Status::unavailable("index"))
                        } else {
                            Ok(())
                        }
                    }
                },
                |item| {
                    let events = events.clone();
                    async move {
                        events.lock().unwrap().push(("visible", item));
                        if !fail_submit && item == 2 {
                            Err(Status::data_loss("search"))
                        } else {
                            Ok(())
                        }
                    }
                },
            )
            .await
            .unwrap_err();
            assert_eq!(
                error.code(),
                if fail_submit {
                    tonic::Code::Unavailable
                } else {
                    tonic::Code::DataLoss
                }
            );
            let events = events.lock().unwrap();
            if fail_submit {
                assert_eq!(*events, [("submit", 1), ("submit", 2)]);
            } else {
                assert_eq!(
                    *events,
                    [
                        ("submit", 1),
                        ("submit", 2),
                        ("submit", 3),
                        ("visible", 1),
                        ("visible", 2)
                    ]
                );
            }
        }
    }

    #[tokio::test]
    async fn batch_visibility_checks_share_one_budget() {
        let error = submit_then_wait(
            &[1, 2],
            Duration::from_millis(100),
            |_| async { Ok(()) },
            |_| async {
                tokio::time::sleep(Duration::from_millis(80)).await;
                Ok(())
            },
        )
        .await
        .unwrap_err();
        assert_eq!(error.code(), tonic::Code::DeadlineExceeded);
    }

    #[test]
    fn bulk_preflight_orders_and_checks_request() {
        let node = |id: &str| LpgRecord {
            key: Some(LpgRecordKey {
                kind: LpgRecordKind::LpgNode as i32,
                id: id.into(),
            }),
            labels: vec!["Node".into()],
            ..Default::default()
        };
        let edge = LpgRecord {
            key: Some(LpgRecordKey {
                kind: LpgRecordKind::LpgEdge as i32,
                id: "e".into(),
            }),
            source: Some("a".into()),
            target: Some("b".into()),
            edge_type: Some("links".into()),
            ..Default::default()
        };
        let a = node("a");
        let b = node("b");
        assert!(bulk_order(&[edge.clone(), a.clone()]).is_err());
        assert_eq!(bulk_order(&[edge.clone(), b.clone(), a.clone()]).unwrap(), [1, 2, 0]);
        assert!(bulk_order(&[a.clone(), a.clone()]).is_err());
        assert!(bulk_order(&[]).is_err());
        assert!(bulk_order(&vec![a.clone(); 1001]).is_err());
        let mut changed = b;
        changed.revision = "changed".into();
        assert!(bulk_order(&[changed]).is_ok());
        let mut marked = a;
        marked.deleting = true;
        assert!(bulk_order(&[marked]).is_err());
        assert!(bulk_order(&[edge]).is_err());
    }

    #[tokio::test]
    async fn graph_mutation_locks_and_reserved_marker() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://unused@localhost/unused")
            .unwrap();
        let server = LpgServer::new(NidxMetadata { pool }, None);
        let same_server = server.clone();
        let lock = server.graph_lock("one").await;
        let guard = lock.lock().await;
        let same = same_server.graph_lock("one").await;
        assert!(same.try_lock().is_err());
        let other = same_server.graph_lock("two").await;
        assert!(other.try_lock().is_ok());
        drop(guard);
        assert!(same.try_lock().is_ok());
        let marked = LpgRecord {
            key: Some(LpgRecordKey {
                kind: LpgRecordKind::LpgNode as i32,
                id: "n".into(),
            }),
            deleting: true,
            ..Default::default()
        };
        assert_eq!(
            server.put_locked("one", &marked).await.unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }

    #[test]
    fn record_identity_and_validation() {
        let node_key = LpgRecordKey {
            kind: LpgRecordKind::LpgNode as i32,
            id: "x/y".into(),
        };
        let edge_key = LpgRecordKey {
            kind: LpgRecordKind::LpgEdge as i32,
            id: "x/y".into(),
        };
        assert_ne!(
            resource_id("graph1", &node_key).unwrap(),
            resource_id("graph1", &edge_key).unwrap()
        );
        assert_ne!(
            resource_id("graph1", &node_key).unwrap(),
            resource_id("graph2", &node_key).unwrap()
        );
        let node = LpgRecord {
            key: Some(node_key),
            labels: vec!["Entity".into(), "Product".into()],
            ..Default::default()
        };
        validate_record(&node).unwrap();
        let rid = resource_id("graph1", node.key.as_ref().unwrap()).unwrap();
        let resource = project("graph1", &node, &rid, None, "test-fence", None).unwrap();
        assert_eq!(
            LpgRecord::decode(STANDARD.decode(&resource.texts[FIELD].text).unwrap().as_slice()).unwrap(),
            node
        );
        assert_eq!(resource.texts_to_delete, vec![rid]);
        let invalid = LpgRecord {
            key: Some(edge_key),
            ..Default::default()
        };
        assert_eq!(
            validate_record(&invalid).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
        let untyped_node = LpgRecord {
            key: Some(LpgRecordKey {
                kind: LpgRecordKind::LpgNode as i32,
                id: "untyped".into(),
            }),
            ..Default::default()
        };
        assert_eq!(
            validate_record(&untyped_node).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
        let labeled_edge = LpgRecord {
            key: Some(LpgRecordKey {
                kind: LpgRecordKind::LpgEdge as i32,
                id: "labeled".into(),
            }),
            labels: vec!["Relationship".into()],
            source: Some("a".into()),
            target: Some("b".into()),
            edge_type: Some("LINKS".into()),
            ..Default::default()
        };
        assert_eq!(
            validate_record(&labeled_edge).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }

    #[test]
    fn record_types_are_immutable_but_node_label_order_is_not_semantic() {
        let node = LpgRecord {
            key: Some(LpgRecordKey {
                kind: LpgRecordKind::LpgNode as i32,
                id: "n".into(),
            }),
            labels: vec!["Product".into(), "Sellable".into()],
            ..Default::default()
        };
        let mut reordered = node.clone();
        reordered.labels.reverse();
        assert!(validate_immutable_types(&node, &reordered).is_ok());
        let mut changed_node = node.clone();
        changed_node.labels = vec!["Product".into()];
        assert_eq!(
            validate_immutable_types(&node, &changed_node).unwrap_err().code(),
            tonic::Code::FailedPrecondition
        );
        let edge = LpgRecord {
            key: Some(LpgRecordKey {
                kind: LpgRecordKind::LpgEdge as i32,
                id: "e".into(),
            }),
            source: Some("a".into()),
            target: Some("b".into()),
            edge_type: Some("PURCHASES".into()),
            ..Default::default()
        };
        let mut changed_edge = edge.clone();
        changed_edge.target = Some("c".into());
        assert!(validate_immutable_types(&edge, &changed_edge).is_ok());
        changed_edge.edge_type = Some("SUPPLIES".into());
        assert_eq!(
            validate_immutable_types(&edge, &changed_edge).unwrap_err().code(),
            tonic::Code::FailedPrecondition
        );
    }

    #[test]
    fn relation_projection_indexes_node_and_edge_types() {
        let edge = LpgRecord {
            key: Some(LpgRecordKey {
                kind: LpgRecordKind::LpgEdge as i32,
                id: "edge".into(),
            }),
            source: Some("source".into()),
            target: Some("target".into()),
            edge_type: Some("PURCHASES".into()),
            ..Default::default()
        };
        let source_labels = vec!["Product".to_string(), "Sellable".to_string()];
        let target_labels = vec!["Client".to_string()];
        let rid = resource_id("graph", edge.key.as_ref().unwrap()).unwrap();

        let resource = project(
            "graph",
            &edge,
            &rid,
            None,
            "test-fence",
            Some((&source_labels, &target_labels)),
        )
        .unwrap();
        let relations = &resource.field_relations[RELATION_FIELD].relations;
        assert_eq!(relations.len(), 1);
        let relation = relations[0].relation.as_ref().unwrap();
        assert_eq!(relation.source.as_ref().unwrap().value, "source");
        assert_eq!(relation.to.as_ref().unwrap().value, "target");
        assert_eq!(relation.relation_label, "PURCHASES");
        assert_eq!(
            relations[0].facets.iter().collect::<HashSet<_>>(),
            [
                format!("/lpg/edge/{rid}"),
                lpg_type_facet("edge", "PURCHASES"),
                lpg_type_facet("source", "Product"),
                lpg_type_facet("source", "Sellable"),
                lpg_type_facet("target", "Client"),
            ]
            .iter()
            .collect()
        );
        assert_eq!(resource.relation_fields_to_delete, [RELATION_FIELD]);
    }

    #[test]
    fn invalid_property_values_are_rejected() {
        let mut node = LpgRecord {
            key: Some(LpgRecordKey {
                kind: LpgRecordKind::LpgNode as i32,
                id: "n".into(),
            }),
            labels: vec!["Node".into()],
            ..Default::default()
        };
        for value in [
            lpg_property::Value::Float(f64::NAN),
            lpg_property::Value::NullValue(false),
            lpg_property::Value::IntegerRange(LpgIntegerRange {
                lower: Some(2),
                upper: Some(1),
            }),
        ] {
            node.properties.insert("p".into(), LpgProperty { value: Some(value) });
            assert_eq!(validate_record(&node).unwrap_err().code(), tonic::Code::InvalidArgument);
        }
        node.properties.insert("p".into(), LpgProperty { value: None });
        assert_eq!(validate_record(&node).unwrap_err().code(), tonic::Code::InvalidArgument);
        assert!(
            check_index_status(OpStatus {
                status: op_status::Status::Error as i32,
                detail: "failure".into(),
            })
            .is_err()
        );
    }

    #[test]
    fn indexed_properties_and_filter_validation() {
        let record = LpgRecord {
            key: Some(LpgRecordKey {
                kind: LpgRecordKind::LpgNode as i32,
                id: "n".into(),
            }),
            properties: HashMap::from([
                (
                    "a.b/猫".into(),
                    LpgProperty {
                        value: Some(lpg_property::Value::Integer(42)),
                    },
                ),
                (
                    "unset".into(),
                    LpgProperty {
                        value: Some(lpg_property::Value::NullValue(true)),
                    },
                ),
            ]),
            ..Default::default()
        };
        let json = indexed_properties(&record);
        assert_eq!(json["properties"][property_path("a.b/猫")], 42);
        assert!(json["properties"].get(property_path("unset")).is_none());
        let filter = LpgPropertyFilter {
            property: "a.b/猫".into(),
            predicate: Some(lpg_property_filter::Predicate::IntegerRange(LpgIntegerRange {
                lower: Some(1),
                upper: Some(42),
            })),
        };
        assert!(filter_expression(LpgRecordKind::LpgNode as i32, &[filter]).is_ok());
        let invalid = LpgPropertyFilter {
            property: "x".into(),
            predicate: Some(lpg_property_filter::Predicate::IntegerRange(LpgIntegerRange::default())),
        };
        assert_eq!(
            filter_expression(LpgRecordKind::LpgNode as i32, &[invalid])
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
    }

    #[test]
    fn text_projection_and_independent_patches() {
        let mut record = LpgRecord {
            key: Some(LpgRecordKey {
                kind: LpgRecordKind::LpgNode as i32,
                id: "n".into(),
            }),
            labels: vec!["Node".into()],
            text_properties: HashMap::from([
                (
                    "desc".into(),
                    LpgTextProperty {
                        chunks: vec!["猫".into(), "tree".into()],
                        embeddings: vec![
                            LpgVector { values: vec![1.0, 0.0] },
                            LpgVector { values: vec![0.0, 1.0] },
                        ],
                    },
                ),
                (
                    "other".into(),
                    LpgTextProperty {
                        chunks: vec!["kept".into()],
                        embeddings: vec![LpgVector { values: vec![1.0, 0.0] }],
                    },
                ),
            ]),
            ..Default::default()
        };
        validate_record(&record).unwrap();
        let rid = resource_id("graph", record.key.as_ref().unwrap()).unwrap();
        let resource = project("graph", &record, &rid, Some("v"), "test-fence", None).unwrap();
        let field = format!("t/{}", property_path("desc"));
        let paragraphs = &resource.paragraphs[&field].paragraphs;
        assert!(paragraphs.contains_key(&format!("{rid}/{field}/0-1")));
        assert!(paragraphs.contains_key(&format!("{rid}/{field}/2-6")));
        assert_eq!(
            paragraphs[&format!("{rid}/{field}/0-1")].vectorsets_sentences["v"]
                .sentences
                .len(),
            1
        );
        apply_text_patch(
            &mut record,
            HashMap::from([(
                "desc".into(),
                LpgTextPropertyPatch {
                    chunks: Some(LpgTextChunks {
                        chunks: vec!["changed".into()],
                    }),
                    embeddings: None,
                },
            )]),
        )
        .unwrap();
        assert!(record.text_properties["desc"].embeddings.is_empty());
        assert_eq!(record.text_properties["other"].embeddings.len(), 1);
        assert_eq!(
            apply_text_patch(
                &mut record,
                HashMap::from([(
                    "missing".into(),
                    LpgTextPropertyPatch {
                        chunks: None,
                        embeddings: Some(LpgEmbeddings {
                            vectors: vec![LpgVector { values: vec![1.0, 0.0] }]
                        }),
                    }
                )])
            )
            .unwrap_err()
            .code(),
            tonic::Code::NotFound
        );
    }

    #[test]
    fn chunk_ids_and_edge_filter_validation() {
        let candidate = chunk_candidate("123/t/p_64657363/2-6").unwrap();
        assert_eq!(
            (
                candidate.rid.as_str(),
                candidate.field.as_str(),
                candidate.start,
                candidate.end
            ),
            ("123", "t/p_64657363", 2, 6)
        );
        assert!(chunk_candidate("123/a/lpg/2-6").is_err());
        assert!(chunk_candidate("123/t/p_x/6-2").is_err());
        let request = FindRecordsRequest {
            kind: LpgRecordKind::LpgEdge as i32,
            incident_node: Some("a".into()),
            ..Default::default()
        };
        assert!(find_filter(&request).is_ok());
        assert_eq!(
            find_filter(&FindRecordsRequest {
                kind: LpgRecordKind::LpgNode as i32,
                ..request
            })
            .unwrap_err()
            .code(),
            tonic::Code::InvalidArgument
        );
    }
}
