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
use time::format_description::well_known::Rfc3339;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status, transport::Endpoint};
use uuid::Uuid;

use crate::indexer::batch::{MAX_BATCH_BYTES, MAX_RESOURCES};
use crate::lpg::{
    FIELD, expand_filter, find_filter, parse_datetime, property_path, resource_id, validate_search,
    validate_shortest_path,
};
use crate::{
    NidxMetadata,
    errors::NidxError,
    metadata::{Index, IndexKind, Shard},
    settings::LpgSettings,
};
use nidx_vector::config::VectorConfig;

const RELATION_FIELD: &str = "a/metadata";
const MAX_LPG_OPERATIONS: usize = 1000;

#[derive(Clone)]
struct PreparedUpsert {
    request: IndexResourceRequest,
    rid: String,
    record: LpgRecord,
    fence: String,
}

fn prepared_upsert(request: IndexResourceRequest) -> Result<PreparedUpsert, Status> {
    let resource = request
        .resource
        .as_ref()
        .ok_or_else(|| Status::data_loss("missing prepared resource"))?;
    let rid = resource
        .resource
        .as_ref()
        .ok_or_else(|| Status::data_loss("missing prepared ID"))?
        .uuid
        .clone();
    let text = &resource
        .texts
        .get(FIELD)
        .ok_or_else(|| Status::data_loss("missing prepared record"))?
        .text;
    let bytes = STANDARD
        .decode(text)
        .map_err(|_| Status::data_loss("invalid prepared record"))?;
    let record = LpgRecord::decode(bytes.as_slice()).map_err(|_| Status::data_loss("invalid prepared record"))?;
    let json: serde_json::Value = serde_json::from_str(
        &resource
            .json_fields
            .get(FIELD)
            .ok_or_else(|| Status::data_loss("missing prepared fence"))?
            .value,
    )
    .map_err(|_| Status::data_loss("invalid prepared fence"))?;
    let fence = json["fence"]
        .as_str()
        .ok_or_else(|| Status::data_loss("missing prepared fence"))?
        .to_owned();
    Ok(PreparedUpsert {
        request,
        rid,
        record,
        fence,
    })
}

async fn submit_all<T: Clone, S, SF>(items: &[T], mut submit: S) -> Result<(), Status>
where
    S: FnMut(T) -> SF,
    SF: std::future::Future<Output = Result<(), Status>>,
{
    for item in items {
        submit(item.clone()).await?;
    }
    Ok(())
}

fn index_batches(graph: &str, resources: Vec<Resource>) -> Result<Vec<IndexResourcesRequest>, Status> {
    let mut request = IndexResourcesRequest {
        shard_id: graph.into(),
        resources: vec![],
    };
    let header_size = request.encoded_len();
    let mut size = header_size;
    let mut batches = Vec::new();
    for resource in resources {
        let length = resource.encoded_len();
        let entry_size = 1 + length_delimiter_len(length) + length;
        if header_size + entry_size > MAX_BATCH_BYTES {
            return Err(Status::resource_exhausted(
                "indexed resource exceeds gRPC message limit",
            ));
        }
        if size + entry_size > MAX_BATCH_BYTES || request.resources.len() == MAX_RESOURCES {
            batches.push(request);
            request = IndexResourcesRequest {
                shard_id: graph.into(),
                resources: vec![],
            };
            size = header_size;
        }
        request.resources.push(resource);
        size += entry_size;
    }
    if !request.resources.is_empty() {
        batches.push(request);
    }
    Ok(batches)
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
        .map(|node| node.labels)
        .ok_or_else(|| Status::failed_precondition("edge endpoint is missing"))
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

    async fn write_record(&self, graph: &str, record: &LpgRecord) -> Result<(), Status> {
        let prepared = prepared_upsert(self.prepare_record(graph, record, None).await?)?;
        let mut client = NidxIndexerClient::new(endpoint(&self.endpoints()?.indexer_address)?.connect_lazy());
        check_index_status(client.index_resource(prepared.request).await?.into_inner())?;
        self.wait_visible(graph, &prepared.rid, Some((&prepared.record, &prepared.fence)))
            .await
    }

    async fn delete_one(&self, graph: &str, key: &LpgRecordKey) -> Result<(), Status> {
        let rid = resource_id(graph, key)?;
        let request = IndexMessage {
            shard: graph.into(),
            resource: rid.clone(),
            typemessage: TypeMessage::Deletion as i32,
            ..Default::default()
        };
        let mut client = NidxIndexerClient::new(endpoint(&self.endpoints()?.indexer_address)?.connect_lazy());
        check_index_status(client.index(request).await?.into_inner())?;
        self.wait_visible(graph, &rid, None).await
    }

    async fn put_locked(&self, graph: &str, record: &LpgRecord) -> Result<(), Status> {
        let key = validate_record(record)?;
        if let Some(existing) = self.get_one(graph, key.clone()).await? {
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
                    self.delete_node_locked(graph, &key.id).await?;
                } else {
                    self.delete_one(graph, &key).await?;
                }
                Ok(None)
            }
            Some(lpg_operation::Operation::DeleteNode(deletion)) => {
                self.delete_node_locked(graph, &deletion.id).await?;
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

    async fn delete_node_locked(&self, graph: &str, id: &str) -> Result<(), Status> {
        let key = LpgRecordKey {
            kind: LpgRecordKind::LpgNode as i32,
            id: id.into(),
        };
        resource_id(graph, &key)?;
        if self.get_one(graph, key.clone()).await?.is_none() {
            return Ok(());
        }
        if !self.incident_edges(graph, id, 1).await?.records.is_empty() {
            return Err(Status::failed_precondition(
                "node has incident edges; delete them first",
            ));
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
            Some(Value::Datetime(v)) => {
                parse_datetime(v)?;
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

fn bulk_order(records: &[LpgRecord], endpoint_labels: &HashMap<String, LpgStringList>) -> Result<Vec<usize>, Status> {
    if records.is_empty() || records.len() > MAX_LPG_OPERATIONS {
        return Err(Status::invalid_argument(format!(
            "records must contain between 1 and {MAX_LPG_OPERATIONS} items"
        )));
    }
    if endpoint_labels.len() > 2 * MAX_LPG_OPERATIONS {
        return Err(Status::invalid_argument("too many bulk endpoint references"));
    }
    let mut keys = HashSet::new();
    let mut nodes = HashSet::new();
    for (id, labels) in endpoint_labels {
        if id.is_empty()
            || labels.values.is_empty()
            || labels.values.iter().any(String::is_empty)
            || labels.values.iter().collect::<HashSet<_>>().len() != labels.values.len()
        {
            return Err(Status::invalid_argument(
                "bulk endpoint references require an ID and unique nonempty labels",
            ));
        }
        nodes.insert(id.as_str());
    }
    let mut order = Vec::with_capacity(records.len());
    for (index, record) in records.iter().enumerate() {
        let key = validate_record(record)?;
        resource_id("", key)?;
        if !keys.insert((key.kind, key.id.clone())) {
            return Err(Status::invalid_argument("duplicate record key in bulk request"));
        }
        if key.kind == LpgRecordKind::LpgNode as i32 {
            if let Some(labels) = endpoint_labels.get(&key.id)
                && labels.values.iter().collect::<HashSet<_>>() != record.labels.iter().collect::<HashSet<_>>()
            {
                return Err(Status::invalid_argument(
                    "bulk endpoint labels disagree with node labels",
                ));
            }
            nodes.insert(key.id.as_str());
            order.push(index);
        }
    }
    for (index, record) in records.iter().enumerate() {
        if record.key.as_ref().unwrap().kind == LpgRecordKind::LpgEdge as i32 {
            for id in [&record.source, &record.target] {
                let id = id.as_ref().unwrap();
                if !nodes.contains(id.as_str()) {
                    return Err(Status::failed_precondition(
                        "bulk edge endpoints require a node or endpoint labels in this request",
                    ));
                }
            }
            order.push(index);
        }
    }
    Ok(order)
}

fn indexed_properties(record: &LpgRecord) -> Result<serde_json::Value, Status> {
    use lpg_property::Value;
    let mut properties = serde_json::Map::new();
    for (name, property) in &record.properties {
        let value = match property.value.as_ref() {
            Some(Value::Text(v)) => serde_json::json!(v),
            Some(Value::Integer(v)) => serde_json::json!(v),
            Some(Value::Float(v)) => serde_json::json!(v),
            Some(Value::Boolean(v)) => serde_json::json!(v),
            Some(Value::Strings(v)) => serde_json::json!(v.values),
            Some(Value::Datetime(v)) => serde_json::json!(
                parse_datetime(v)?
                    .format(&Rfc3339)
                    .map_err(|_| Status::invalid_argument("datetime cannot be indexed"))?
            ),
            _ => continue,
        };
        properties.insert(property_path(name), value);
    }
    Ok(serde_json::json!({
        "kind": record.key.as_ref().map(|key| key.kind).unwrap_or_default(),
        "labels": record.labels,
        "source": record.source,
        "target": record.target,
        "edge_type": record.edge_type,
        "properties": properties,
    }))
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
                skip_tokenization: true,
            },
        )]),
        json_fields: HashMap::from([(
            FIELD.to_owned(),
            JsonFieldValue {
                value: {
                    let mut json = indexed_properties(record)?;
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
                ..Default::default()
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

#[tonic::async_trait]
impl NidxLpg for LpgServer {
    async fn bulk_load(&self, request: Request<BulkLoadRequest>) -> Result<Response<LpgOperationResponse>, Status> {
        let req = request.into_inner();
        let graph = self.graph(&req.graph_id).await?;
        let lock = self.graph_lock(&graph).await;
        let _guard = lock.lock().await;
        let order = bulk_order(&req.records, &req.endpoint_labels)?;
        let mut node_labels = req
            .endpoint_labels
            .into_iter()
            .map(|(id, labels)| (id, labels.values))
            .collect::<HashMap<_, _>>();
        node_labels.extend(req.records.iter().filter_map(|record| {
            let key = record.key.as_ref()?;
            (key.kind == LpgRecordKind::LpgNode as i32).then(|| (key.id.clone(), record.labels.clone()))
        }));
        let mut prepared = Vec::with_capacity(req.records.len());
        for record in &req.records {
            prepared.push(self.prepare_record(&graph, record, Some(&node_labels)).await?);
        }
        let resources = order
            .into_iter()
            .map(|index| {
                prepared[index]
                    .resource
                    .take()
                    .ok_or_else(|| Status::internal("missing prepared resource"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let batches = index_batches(&graph, resources)?;
        let client = NidxIndexerClient::new(endpoint(&self.endpoints()?.indexer_address)?.connect_lazy());
        submit_all(&batches, |batch| {
            let mut client = client.clone();
            async move { check_index_status(client.index_resources(batch).await?.into_inner()) }
        })
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
        let mut req = request.into_inner();
        validate_search(&req)?;
        let graph = self.graph(&req.graph_id).await?;
        if !req.vector.is_empty() {
            let (name, dimension) = self.vector_configuration(&graph, &req.vectorset).await?;
            if req.vector.len() != dimension {
                return Err(Status::invalid_argument(format!(
                    "vector must have {dimension} dimensions"
                )));
            }
            req.vectorset = name;
        }
        req.graph_id = graph;
        let mut client = NidxSearcherClient::new(endpoint(&self.endpoints()?.searcher_address)?.connect_lazy());
        client.lpg_search_chunks(req).await
    }

    async fn find_records(
        &self,
        request: Request<FindRecordsRequest>,
    ) -> Result<Response<FindRecordsResponse>, Status> {
        let req = request.into_inner();
        find_filter(&req)?;
        if !(1..=1000).contains(&req.page_size) {
            return Err(Status::invalid_argument("page_size must be between 1 and 1000"));
        }
        let graph = self.graph(&req.graph_id).await?;
        let mut client = NidxSearcherClient::new(endpoint(&self.endpoints()?.searcher_address)?.connect_lazy());
        client
            .lpg_find_records(FindRecordsRequest { graph_id: graph, ..req })
            .await
    }

    async fn expand(&self, request: Request<ExpandRequest>) -> Result<Response<ExpandResponse>, Status> {
        let req = request.into_inner();
        expand_filter(&req)?;
        let graph = self.graph(&req.graph_id).await?;
        let mut client = NidxSearcherClient::new(endpoint(&self.endpoints()?.searcher_address)?.connect_lazy());
        client.lpg_expand(ExpandRequest { graph_id: graph, ..req }).await
    }

    async fn shortest_path(
        &self,
        request: Request<ShortestPathRequest>,
    ) -> Result<Response<ShortestPathResponse>, Status> {
        let req = request.into_inner();
        validate_shortest_path(&req)?;
        let graph = self.graph(&req.graph_id).await?;
        let mut client = NidxSearcherClient::new(endpoint(&self.endpoints()?.searcher_address)?.connect_lazy());
        client
            .lpg_shortest_path(ShortestPathRequest { graph_id: graph, ..req })
            .await
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
        for key in &req.keys {
            resource_id(&graph, key)?;
        }
        let mut client = NidxSearcherClient::new(endpoint(&self.endpoints()?.searcher_address)?.connect_lazy());
        client
            .lpg_get_records(GetRecordsRequest {
                graph_id: graph,
                keys: req.keys,
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lpg::{date_timestamp, filter_expression, matches_property};

    #[tokio::test]
    async fn search_rejects_nonfinite_minimum_scores_before_reading() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://unused@localhost/unused")
            .unwrap();
        let server = LpgServer::new(NidxMetadata { pool }, None);
        for value in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
            for semantic in [false, true] {
                let error = server
                    .search_chunks(Request::new(SearchChunksRequest {
                        text: Some("query".into()),
                        min_score_semantic: if semantic { value } else { 0.0 },
                        min_score_keyword: if semantic { 0.0 } else { value },
                        ..Default::default()
                    }))
                    .await
                    .unwrap_err();
                assert_eq!(error.code(), tonic::Code::InvalidArgument);
                assert!(error.message().contains("minimum scores"));
            }
        }
    }

    #[tokio::test]
    async fn shortest_path_rejects_unknown_enums_before_reading() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://unused@localhost/unused")
            .unwrap();
        let server = LpgServer::new(NidxMetadata { pool }, None);
        for (direction, node_filter_scope) in [(99, 0), (LpgDirection::LpgBoth as i32, 99)] {
            let error = server
                .shortest_path(Request::new(ShortestPathRequest {
                    source: "same".into(),
                    target: "same".into(),
                    direction,
                    node_filter_scope,
                    ..Default::default()
                }))
                .await
                .unwrap_err();
            assert_eq!(error.code(), tonic::Code::InvalidArgument);
        }
    }

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
                )
                .unwrap(),
                expected,
                "{property}"
            );
        }
    }

    #[tokio::test]
    async fn batch_submits_every_item() {
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        submit_all(&[1, 2, 3], |item| {
            let events = events.clone();
            async move {
                events.lock().unwrap().push(item);
                Ok(())
            }
        })
        .await
        .unwrap();
        assert_eq!(*events.lock().unwrap(), [1, 2, 3]);
    }

    #[test]
    fn projected_batches_respect_the_encoded_grpc_limit() {
        let resource = Resource {
            texts: HashMap::from([(
                FIELD.into(),
                TextInformation {
                    text: "x".repeat(2 * 1024 * 1024),
                    ..Default::default()
                },
            )]),
            ..Default::default()
        };
        let resources = vec![resource; 3];
        let batches = index_batches("graph", resources.clone()).unwrap();
        assert_eq!(batches.len(), 3);
        assert!(batches.iter().all(|request| request.encoded_len() <= MAX_BATCH_BYTES));
        assert_eq!(
            batches
                .into_iter()
                .flat_map(|request| request.resources)
                .collect::<Vec<_>>(),
            resources
        );
        let mut oversized = resources[0].clone();
        oversized.texts.get_mut(FIELD).unwrap().text = "x".repeat(MAX_BATCH_BYTES);
        assert_eq!(
            index_batches("graph", vec![resources[0].clone(), oversized])
                .unwrap_err()
                .code(),
            tonic::Code::ResourceExhausted
        );
    }

    #[test]
    fn projected_batches_respect_the_resource_count_limit() {
        let resources = vec![Resource::default(); 2 * MAX_RESOURCES + 1];
        let batches = index_batches("graph", resources).unwrap();
        assert_eq!(
            batches
                .iter()
                .map(|request| request.resources.len())
                .collect::<Vec<_>>(),
            [MAX_RESOURCES, MAX_RESOURCES, 1]
        );
    }

    #[tokio::test]
    async fn batch_propagates_submission_failure_and_stops() {
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        let error = submit_all(&[1, 2, 3], |item| {
            let events = events.clone();
            async move {
                events.lock().unwrap().push(item);
                if item == 2 {
                    Err(Status::unavailable("index"))
                } else {
                    Ok(())
                }
            }
        })
        .await
        .unwrap_err();
        assert_eq!(error.code(), tonic::Code::Unavailable);
        assert_eq!(*events.lock().unwrap(), [1, 2]);
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
        let empty = HashMap::new();
        assert!(bulk_order(&[edge.clone(), a.clone()], &empty).is_err());
        assert_eq!(
            bulk_order(&[edge.clone(), b.clone(), a.clone()], &empty).unwrap(),
            [1, 2, 0]
        );
        assert!(bulk_order(&[a.clone(), a.clone()], &empty).is_err());
        assert!(bulk_order(&[], &empty).is_err());
        assert!(bulk_order(&vec![a.clone(); 1001], &empty).is_err());
        let mut changed = b;
        changed.revision = "changed".into();
        assert!(bulk_order(&[changed], &empty).is_ok());
        assert!(bulk_order(std::slice::from_ref(&edge), &empty).is_err());
        let labels = LpgStringList {
            values: vec!["Node".into()],
        };
        let mut endpoints = HashMap::from([("a".into(), labels.clone()), ("b".into(), labels)]);
        assert_eq!(bulk_order(std::slice::from_ref(&edge), &endpoints).unwrap(), [0]);
        endpoints.get_mut("b").unwrap().values = vec!["Other".into()];
        assert!(bulk_order(&[edge.clone(), node("b")], &endpoints).is_err());
        for invalid in [vec![], vec!["".into()], vec!["Node".into(), "Node".into()]] {
            endpoints.get_mut("b").unwrap().values = invalid;
            assert!(bulk_order(std::slice::from_ref(&edge), &endpoints).is_err());
        }
    }

    #[tokio::test]
    async fn graph_mutation_locks() {
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
            lpg_property::Value::Datetime("invalid".into()),
            lpg_property::Value::Datetime("9999-01-01T00:00:00Z".into()),
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
    fn datetime_filters_compare_instants_and_preserve_precision() {
        let mut record = LpgRecord {
            properties: HashMap::from([(
                "date".into(),
                LpgProperty {
                    value: Some(lpg_property::Value::Datetime("2026-11-24T14:00:00.123456+02:00".into())),
                },
            )]),
            ..Default::default()
        };
        let filter = |value| LpgPropertyFilter {
            property: "date".into(),
            predicate: Some(lpg_property_filter::Predicate::Datetime(value)),
        };
        assert!(matches_property(&record, &filter("2026-11-24T12:00:00.123456".into())).unwrap());
        assert!(!matches_property(&record, &filter("2026-11-24T12:00:00.123457Z".into())).unwrap());
        let json = indexed_properties(&record).unwrap();
        assert_eq!(json["properties"][property_path("date")], "2026-11-24T12:00:00.123456Z");
        let timestamp = date_timestamp("1969-12-31T23:59:59.123456Z").unwrap();
        assert_eq!((timestamp.seconds, timestamp.nanos), (-1, 123456000));
        record.properties.get_mut("date").unwrap().value = Some(lpg_property::Value::Datetime("bad".into()));
        assert!(matches_property(&record, &filter("2026-11-24T12:00:00Z".into())).is_err());
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
        let json = indexed_properties(&record).unwrap();
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
        assert!(resource.texts[FIELD].skip_tokenization);
        let field = format!("t/{}", property_path("desc"));
        assert!(!resource.texts[&field].skip_tokenization);
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
    fn edge_filter_validation() {
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
