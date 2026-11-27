// Copyright 2021 Bosutech XXI S.L.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use base64::{Engine, engine::general_purpose::STANDARD};
use nidx_json::{JsonSearcher, search::JsonSearchRequest};
use nidx_protos::prost::*;
use nidx_protos::{
    ExpandMatch, ExpandRequest, ExpandResponse, ExtractedTextsRequest, FindRecordsRequest, FindRecordsResponse,
    GetRecordsRequest, GetRecordsResponse, LpgDirection, LpgRecord, LpgRecordKey, LpgRecordKind,
    extracted_texts_request,
};
use nidx_relation::{LpgRelation, RelationSearcher};
use serde::{Deserialize, Serialize};
use tonic::Status;
use tracing::Span;
use uuid::Uuid;

use crate::errors::{NidxError, NidxResult};
use crate::lpg::{
    FIELD, MAX_EXPAND_RESPONSE_BYTES, decode_record, expand_filter, find_filter, matches_properties,
    records_from_texts, resource_id,
};

use super::query_planner::prefilter::proto_to_json_filter;
use super::{index_cache::IndexCache, shard_text};

mod shortest_path;
pub(super) use shortest_path::shortest_path;
mod search;
pub(super) use search::search_chunks;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FindCursor {
    version: u8,
    query: Uuid,
    after: Uuid,
}

fn query_id(request: &FindRecordsRequest) -> Uuid {
    let mut query = request.clone();
    query.page_token.clear();
    query.page_size = 0;
    Uuid::new_v5(&Uuid::NAMESPACE_OID, &query.encode_to_vec())
}

pub(super) fn validate_find(request: &FindRecordsRequest) -> Result<(JsonSearchRequest, Option<Uuid>), Status> {
    if !(1..=1000).contains(&request.page_size) {
        return Err(Status::invalid_argument("page_size must be between 1 and 1000"));
    }
    let filter = proto_to_json_filter(&find_filter(request)?).map_err(Status::from)?;
    if request.page_token.is_empty() {
        return Ok((JsonSearchRequest { filter }, None));
    }
    if request.page_token.len() > 1024 {
        return Err(Status::invalid_argument("invalid page token"));
    }
    let bytes = STANDARD
        .decode(&request.page_token)
        .map_err(|_| Status::invalid_argument("invalid page token"))?;
    let cursor: FindCursor =
        serde_json::from_slice(&bytes).map_err(|_| Status::invalid_argument("invalid page token"))?;
    if cursor.version != 1 || cursor.query != query_id(request) {
        return Err(Status::invalid_argument("page token does not match this query"));
    }
    Ok((JsonSearchRequest { filter }, Some(cursor.after)))
}

fn page_token(request: &FindRecordsRequest, after: Uuid) -> Result<String, Status> {
    let cursor = FindCursor {
        version: 1,
        query: query_id(request),
        after,
    };
    let bytes = serde_json::to_vec(&cursor).map_err(|error| Status::internal(error.to_string()))?;
    Ok(STANDARD.encode(bytes))
}

pub(super) async fn find_records(
    index_cache: Arc<IndexCache>,
    mut request: FindRecordsRequest,
) -> NidxResult<FindRecordsResponse> {
    let shard = Uuid::parse_str(&request.graph_id)?;
    request.graph_id = shard.to_string();
    let (filter, after) = validate_find(&request).map_err(NidxError::GrpcError)?;
    let indexes = index_cache.get_shard_indexes(&shard).await.ok_or(NidxError::NotFound)?;
    let json_index = indexes.json_index().ok_or(NidxError::NotFound)?;
    let index = index_cache.get(&json_index).await?;
    let limit = request.page_size as usize;
    let span = Span::current();
    let mut ids = tokio::task::spawn_blocking(move || {
        span.in_scope(|| {
            let searcher: &JsonSearcher = index.as_ref().into();
            searcher.search_page(&filter, after, limit + 1)
        })
    })
    .await??;
    let has_more = ids.len() > limit;
    ids.truncate(limit);
    if ids.is_empty() {
        return Ok(FindRecordsResponse::default());
    }
    let texts = shard_text::extracted_texts(
        index_cache,
        ExtractedTextsRequest {
            shard_id: request.graph_id.clone(),
            field_ids: ids
                .iter()
                .map(|id| extracted_texts_request::FieldId {
                    rid: id.simple().to_string(),
                    field_type: "a".into(),
                    field_name: "lpg_record_v1".into(),
                    split: None,
                })
                .collect(),
            paragraph_ids: vec![],
        },
    )
    .await?;
    page_from_texts(&request, &ids, &texts, has_more)
}

fn page_from_texts(
    request: &FindRecordsRequest,
    ids: &[Uuid],
    texts: &nidx_protos::ExtractedTextsResponse,
    mut has_more: bool,
) -> NidxResult<FindRecordsResponse> {
    let mut records = Vec::with_capacity(ids.len());
    let mut response_bytes = 0;
    // Leave room for the cursor under the normal 4 MiB transport limit.
    const MAX_PAGE_RECORD_BYTES: usize = 3 * 1024 * 1024;
    for id in ids {
        let record = record_for_resource(&request.graph_id, request.kind, *id, texts).map_err(NidxError::GrpcError)?;
        let len = record.encoded_len();
        let entry_bytes = 1 + length_delimiter_len(len) + len;
        if response_bytes + entry_bytes > MAX_PAGE_RECORD_BYTES {
            if records.is_empty() {
                return Err(NidxError::GrpcError(Status::resource_exhausted(
                    "LPG record exceeds page byte limit",
                )));
            }
            has_more = true;
            break;
        }
        response_bytes += entry_bytes;
        records.push(record);
    }
    let next_page_token = if has_more {
        page_token(request, ids[records.len() - 1]).map_err(NidxError::GrpcError)?
    } else {
        String::new()
    };
    Ok(FindRecordsResponse {
        records,
        next_page_token,
    })
}

fn record_for_resource(
    graph: &str,
    kind: i32,
    id: Uuid,
    texts: &nidx_protos::ExtractedTextsResponse,
) -> Result<LpgRecord, Status> {
    let rid = id.simple().to_string();
    let value = texts
        .fields
        .get(&format!("{rid}/{FIELD}"))
        .filter(|value| !value.is_empty())
        .ok_or_else(|| Status::unavailable("record disappeared during search"))?;
    let record = decode_record(value)?;
    let key = record
        .key
        .as_ref()
        .ok_or_else(|| Status::data_loss("record key missing"))?;
    if key.kind != kind || resource_id(graph, key).map_err(|_| Status::data_loss("invalid record key"))? != rid {
        return Err(Status::data_loss("LPG record identity mismatch"));
    }
    Ok(record)
}

pub(super) async fn expand(index_cache: Arc<IndexCache>, mut request: ExpandRequest) -> NidxResult<ExpandResponse> {
    expand_filter(&request).map_err(NidxError::GrpcError)?;
    let shard = Uuid::parse_str(&request.graph_id)?;
    request.graph_id = shard.to_string();
    let starts: HashSet<_> = request.node_ids.iter().cloned().collect();
    let mut nodes = get_records(
        Arc::clone(&index_cache),
        GetRecordsRequest {
            graph_id: request.graph_id.clone(),
            keys: starts
                .iter()
                .map(|id| LpgRecordKey {
                    kind: LpgRecordKind::LpgNode as i32,
                    id: id.clone(),
                })
                .collect(),
        },
    )
    .await?
    .records
    .into_iter()
    .map(|node| (node.key.as_ref().unwrap().id.clone(), node))
    .collect::<HashMap<_, _>>();
    if nodes.len() != starts.len() {
        return Err(NidxError::GrpcError(Status::not_found(
            "expansion starting node not found",
        )));
    }
    let indexes = index_cache.get_shard_indexes(&shard).await.ok_or(NidxError::NotFound)?;
    let candidate_limit = if request.candidate_limit == 0 {
        1000
    } else {
        request.candidate_limit
    } as usize;
    let json_index =
        if !request.edge_filters.is_empty() || !request.neighbor_filters.is_empty() || request.neighbor_label.is_some()
        {
            Some(
                index_cache
                    .get(&indexes.json_index().ok_or(NidxError::NotFound)?)
                    .await?,
            )
        } else {
            None
        };
    let relation_index = index_cache
        .get(&indexes.relation_index().ok_or(NidxError::NotFound)?)
        .await?;
    let query = request.clone();
    let query_json = json_index.clone();
    let span = Span::current();
    let relations = tokio::task::spawn_blocking(move || {
        span.in_scope(|| {
            let searcher: &RelationSearcher = relation_index.as_ref().into();
            let json = query_json.as_ref().map(|index| index.as_ref().into());
            relation_candidates(searcher, json, &query, candidate_limit)
        })
    })
    .await??;
    let mut edges = Vec::with_capacity(relations.len());
    for chunk in relations.chunks(128) {
        let texts = shard_text::extracted_texts(
            Arc::clone(&index_cache),
            ExtractedTextsRequest {
                shard_id: request.graph_id.clone(),
                field_ids: chunk
                    .iter()
                    .map(|edge| extracted_texts_request::FieldId {
                        rid: edge.resource_id.simple().to_string(),
                        field_type: "a".into(),
                        field_name: "lpg_record_v1".into(),
                        split: None,
                    })
                    .collect(),
                paragraph_ids: vec![],
            },
        )
        .await?;
        for relation in chunk {
            let edge = record_for_resource(
                &request.graph_id,
                LpgRecordKind::LpgEdge as i32,
                relation.resource_id,
                &texts,
            )
            .map_err(NidxError::GrpcError)?;
            if edge.source.as_ref() != Some(&relation.source)
                || edge.target.as_ref() != Some(&relation.target)
                || edge.edge_type.as_ref() != Some(&relation.edge_type)
            {
                return Err(NidxError::GrpcError(Status::unavailable("expansion relation changed")));
            }
            edges.push(edge);
        }
    }
    let neighbor_ids: HashSet<_> = relations
        .iter()
        .flat_map(|edge| [&edge.source, &edge.target])
        .filter(|id| !starts.contains(*id))
        .cloned()
        .collect();
    let keys: Vec<_> = neighbor_ids
        .iter()
        .map(|id| LpgRecordKey {
            kind: LpgRecordKind::LpgNode as i32,
            id: id.clone(),
        })
        .collect();
    for chunk in keys.chunks(128) {
        let records = get_records(
            Arc::clone(&index_cache),
            GetRecordsRequest {
                graph_id: request.graph_id.clone(),
                keys: chunk.to_vec(),
            },
        )
        .await?
        .records;
        if records.len() != chunk.len() {
            return Err(NidxError::GrpcError(Status::unavailable(
                "expansion neighbor disappeared",
            )));
        }
        for node in records {
            nodes.insert(node.key.as_ref().unwrap().id.clone(), node);
        }
    }
    let allowed_neighbors = if !request.neighbor_filters.is_empty() || request.neighbor_label.is_some() {
        let ids = nodes
            .values()
            .map(|node| {
                resource_id(&request.graph_id, node.key.as_ref().unwrap()).map(|id| Uuid::parse_str(&id).unwrap())
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(NidxError::GrpcError)?;
        let filter = JsonSearchRequest {
            filter: proto_to_json_filter(
                &find_filter(&FindRecordsRequest {
                    kind: LpgRecordKind::LpgNode as i32,
                    filters: request.neighbor_filters.clone(),
                    label: request.neighbor_label.clone(),
                    ..Default::default()
                })
                .map_err(NidxError::GrpcError)?,
            )?,
        };
        let index = Arc::clone(json_index.as_ref().unwrap());
        let span = Span::current();
        Some(
            tokio::task::spawn_blocking(move || {
                span.in_scope(|| {
                    let searcher: &JsonSearcher = index.as_ref().into();
                    searcher.search_resource_ids(&filter, &ids)
                })
            })
            .await??,
        )
    } else {
        None
    };
    expansion_response(&request, &edges, &nodes, allowed_neighbors.as_ref()).map_err(NidxError::GrpcError)
}

fn relation_candidates(
    relation: &RelationSearcher,
    json: Option<&JsonSearcher>,
    request: &ExpandRequest,
    limit: usize,
) -> NidxResult<Vec<LpgRelation>> {
    let filter = expand_filter(request).map_err(NidxError::GrpcError)?;
    let edge_ids = if request.edge_filters.is_empty() {
        None
    } else {
        let json = json.ok_or_else(|| NidxError::GrpcError(Status::internal("JSON searcher required")))?;
        let ids = json.search_page(
            &JsonSearchRequest {
                filter: proto_to_json_filter(&filter)?,
            },
            None,
            limit + 1,
        )?;
        if ids.len() > limit {
            return Err(NidxError::GrpcError(Status::resource_exhausted(
                "expansion candidate limit exceeded",
            )));
        }
        Some(ids.into_iter().collect::<HashSet<_>>())
    };
    let edges = relation.lpg_expand(
        &request.node_ids,
        LpgDirection::try_from(request.direction)
            .map_err(|_| NidxError::GrpcError(Status::invalid_argument("invalid expansion direction")))?,
        &request.edge_types,
        edge_ids.as_ref(),
        limit + 1,
    )?;
    if edges.len() > limit {
        return Err(NidxError::GrpcError(Status::resource_exhausted(
            "expansion candidate limit exceeded",
        )));
    }
    if edge_ids.as_ref().is_some_and(|ids| ids.len() != edges.len()) {
        return Err(NidxError::GrpcError(Status::unavailable(
            "expansion relation disappeared",
        )));
    }
    Ok(edges)
}

fn expansion_response(
    request: &ExpandRequest,
    edges: &[LpgRecord],
    nodes: &HashMap<String, LpgRecord>,
    allowed_neighbors: Option<&HashSet<Uuid>>,
) -> Result<ExpandResponse, Status> {
    let starts: HashSet<_> = request.node_ids.iter().collect();
    let direction = LpgDirection::try_from(request.direction)
        .map_err(|_| Status::invalid_argument("invalid expansion direction"))?;
    let max_results = if request.max_results == 0 {
        1000
    } else {
        request.max_results
    } as usize;
    let mut response = ExpandResponse {
        examined_edges: edges.len() as u32,
        ..Default::default()
    };
    let mut edge_indexes = HashMap::new();
    let mut neighbor_indexes = HashMap::new();
    let entry_len = |len| 1 + length_delimiter_len(len) + len;
    let mut response_bytes = response.encoded_len();
    for (index, edge) in edges.iter().enumerate() {
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
        if !outgoing && !incoming {
            return Err(Status::data_loss("expansion returned an unrelated edge"));
        }
        for (node_id, neighbor_id) in [
            outgoing.then_some((source, target)),
            (incoming && !(outgoing && source == target)).then_some((target, source)),
        ]
        .into_iter()
        .flatten()
        {
            let neighbor = nodes
                .get(neighbor_id)
                .ok_or_else(|| Status::unavailable("expansion neighbor disappeared"))?;
            if let Some(allowed) = allowed_neighbors {
                let rid = resource_id(&request.graph_id, neighbor.key.as_ref().unwrap())?;
                if !allowed.contains(&Uuid::parse_str(&rid).unwrap()) {
                    continue;
                }
            }
            if request
                .neighbor_label
                .as_ref()
                .is_some_and(|label| !neighbor.labels.contains(label))
                || !matches_properties(neighbor, &request.neighbor_filters)?
            {
                continue;
            }
            if response.matches.len() == max_results {
                return Err(Status::resource_exhausted("expansion result limit exceeded"));
            }
            let edge_index = *edge_indexes.entry(index).or_insert_with(|| {
                let i = response.edges.len() as u32;
                response_bytes += entry_len(edge.encoded_len());
                response.edges.push(edge.clone());
                i
            });
            let neighbor_index = *neighbor_indexes.entry(neighbor_id).or_insert_with(|| {
                let i = response.neighbors.len() as u32;
                response_bytes += entry_len(neighbor.encoded_len());
                response.neighbors.push(neighbor.clone());
                i
            });
            let binding = ExpandMatch {
                node_id: node_id.clone(),
                edge_index,
                neighbor_index,
            };
            response_bytes += entry_len(binding.encoded_len());
            if response_bytes > MAX_EXPAND_RESPONSE_BYTES {
                return Err(Status::resource_exhausted("expansion response byte limit exceeded"));
            }
            response.matches.push(binding);
        }
    }
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nidx_protos::{ExtractedTextsResponse, LpgRecord, LpgRecordKey, LpgRecordKind, LpgTextProperty};

    fn request() -> FindRecordsRequest {
        FindRecordsRequest {
            graph_id: Uuid::new_v4().to_string(),
            kind: LpgRecordKind::LpgNode as i32,
            page_size: 2,
            ..Default::default()
        }
    }

    fn expansion_fixture() -> (ExpandRequest, Vec<LpgRecord>, HashMap<String, LpgRecord>) {
        let request = ExpandRequest {
            graph_id: Uuid::new_v4().to_string(),
            node_ids: vec!["p".into(), "p".into(), "w".into()],
            direction: LpgDirection::LpgBoth as i32,
            ..Default::default()
        };
        let nodes = ["p", "w"]
            .into_iter()
            .map(|id| {
                (
                    id.into(),
                    LpgRecord {
                        key: Some(LpgRecordKey {
                            kind: LpgRecordKind::LpgNode as i32,
                            id: id.into(),
                        }),
                        labels: vec![if id == "w" { "Warehouse" } else { "Product" }.into()],
                        ..Default::default()
                    },
                )
            })
            .collect();
        let edges = [("e1", "p", "w"), ("e2", "p", "w"), ("loop", "p", "p")]
            .into_iter()
            .map(|(id, source, target)| LpgRecord {
                key: Some(LpgRecordKey {
                    kind: LpgRecordKind::LpgEdge as i32,
                    id: id.into(),
                }),
                source: Some(source.into()),
                target: Some(target.into()),
                edge_type: Some("STOCKED_AT".into()),
                ..Default::default()
            })
            .collect();
        (request, edges, nodes)
    }

    #[test]
    fn expansion_deduplicates_tables_not_endpoint_bindings() {
        let (mut req, edges, nodes) = expansion_fixture();
        let both = expansion_response(&req, &edges, &nodes, None).unwrap();
        assert_eq!(
            (
                both.examined_edges,
                both.matches.len(),
                both.edges.len(),
                both.neighbors.len()
            ),
            (3, 5, 3, 2)
        );
        let mut bindings = both
            .matches
            .iter()
            .map(|m| {
                (
                    m.node_id.as_str(),
                    both.edges[m.edge_index as usize].key.as_ref().unwrap().id.as_str(),
                    both.neighbors[m.neighbor_index as usize]
                        .key
                        .as_ref()
                        .unwrap()
                        .id
                        .as_str(),
                )
            })
            .collect::<Vec<_>>();
        bindings.sort();
        assert_eq!(
            bindings,
            [
                ("p", "e1", "w"),
                ("p", "e2", "w"),
                ("p", "loop", "p"),
                ("w", "e1", "p"),
                ("w", "e2", "p")
            ]
        );
        req.direction = LpgDirection::LpgOutgoing as i32;
        assert_eq!(expansion_response(&req, &edges, &nodes, None).unwrap().matches.len(), 3);
        req.direction = LpgDirection::LpgIncoming as i32;
        assert_eq!(expansion_response(&req, &edges, &nodes, None).unwrap().matches.len(), 3);
    }

    #[test]
    fn expansion_filters_neighbors_without_changing_candidate_count() {
        let (mut req, edges, nodes) = expansion_fixture();
        req.neighbor_label = Some("Warehouse".into());
        let rid = resource_id(&req.graph_id, nodes["w"].key.as_ref().unwrap()).unwrap();
        let allowed = HashSet::from([Uuid::parse_str(&rid).unwrap()]);
        let response = expansion_response(&req, &edges, &nodes, Some(&allowed)).unwrap();
        assert_eq!(
            (response.examined_edges, response.matches.len(), response.edges.len()),
            (3, 2, 2)
        );
        assert_eq!(response.neighbors.len(), 1);
        let empty = expansion_response(&req, &edges, &nodes, Some(&HashSet::new())).unwrap();
        assert_eq!(empty.examined_edges, 3);
        assert!(empty.matches.is_empty());
    }

    #[test]
    fn expansion_preserves_explicit_limits_and_missing_neighbor_errors() {
        let (mut req, edges, mut nodes) = expansion_fixture();
        req.max_results = 4;
        assert_eq!(
            expansion_response(&req, &edges, &nodes, None).unwrap_err().code(),
            tonic::Code::ResourceExhausted
        );
        req.max_results = 5;
        assert_eq!(expansion_response(&req, &edges, &nodes, None).unwrap().matches.len(), 5);
        nodes.remove("w");
        assert_eq!(
            expansion_response(&req, &edges, &nodes, None).unwrap_err().code(),
            tonic::Code::Unavailable
        );
        let (_, _, mut nodes) = expansion_fixture();
        nodes.get_mut("w").unwrap().text_properties.insert(
            "large".into(),
            nidx_protos::LpgTextProperty {
                chunks: vec!["x".repeat(MAX_EXPAND_RESPONSE_BYTES)],
                ..Default::default()
            },
        );
        assert_eq!(
            expansion_response(&req, &edges, &nodes, None).unwrap_err().code(),
            tonic::Code::ResourceExhausted
        );
    }

    #[test]
    fn cursor_is_bound_to_query_and_rejects_old_or_malformed_tokens() {
        let mut req = request();
        let after = Uuid::new_v4();
        req.page_token = page_token(&req, after).unwrap();
        assert_eq!(validate_find(&req).unwrap().1, Some(after));
        req.page_size = 1;
        assert_eq!(validate_find(&req).unwrap().1, Some(after));
        req.label = Some("different".into());
        assert_eq!(validate_find(&req).err().unwrap().code(), tonic::Code::InvalidArgument);
        req.label = None;
        req.graph_id = Uuid::new_v4().to_string();
        assert!(validate_find(&req).is_err());
        req.page_token = STANDARD.encode(
            serde_json::to_vec(&FindCursor {
                version: 2,
                query: query_id(&req),
                after,
            })
            .unwrap(),
        );
        assert!(validate_find(&req).is_err());
        for token in ["not base64".into(), STANDARD.encode(b"{}"), STANDARD.encode([0xff])] {
            req.page_token = token;
            assert!(validate_find(&req).is_err());
        }
    }

    #[test]
    fn byte_limited_page_resumes_after_last_returned_record() {
        let req = request();
        let mut texts = ExtractedTextsResponse::default();
        let mut ids = Vec::new();
        for id in ["a", "b"] {
            let key = LpgRecordKey {
                kind: req.kind,
                id: id.into(),
            };
            let rid = resource_id(&req.graph_id, &key).unwrap();
            ids.push(Uuid::parse_str(&rid).unwrap());
            let record = LpgRecord {
                key: Some(key),
                labels: vec!["Node".into()],
                text_properties: [(
                    "description".into(),
                    LpgTextProperty {
                        chunks: vec!["x".repeat(2 * 1024 * 1024)],
                        ..Default::default()
                    },
                )]
                .into(),
                ..Default::default()
            };
            texts
                .fields
                .insert(format!("{rid}/{FIELD}"), STANDARD.encode(record.encode_to_vec()));
        }
        let page = page_from_texts(&req, &ids, &texts, false).unwrap();
        assert_eq!(page.records.len(), 1);
        assert!(page.encoded_len() < 4 * 1024 * 1024);
        let continued = FindRecordsRequest {
            page_token: page.next_page_token,
            ..req.clone()
        };
        assert_eq!(validate_find(&continued).unwrap().1, Some(ids[0]));
        let tail = page_from_texts(&req, &ids[1..], &texts, false).unwrap();
        assert_eq!(tail.records[0].key.as_ref().unwrap().id, "b");
        assert!(tail.next_page_token.is_empty());
        texts.fields.clear();
        let status: Status = page_from_texts(&req, &ids, &texts, false).unwrap_err().into();
        assert_eq!(status.code(), tonic::Code::Unavailable);
    }

    #[test]
    fn find_rejects_oversized_corrupt_and_mismatched_records() {
        let req = request();
        let key = LpgRecordKey {
            kind: req.kind,
            id: "node".into(),
        };
        let rid = resource_id(&req.graph_id, &key).unwrap();
        let ids = [Uuid::parse_str(&rid).unwrap()];
        let record = LpgRecord {
            key: Some(key),
            text_properties: [(
                "description".into(),
                LpgTextProperty {
                    chunks: vec!["x".repeat(3 * 1024 * 1024)],
                    ..Default::default()
                },
            )]
            .into(),
            ..Default::default()
        };
        for (value, expected) in [
            (STANDARD.encode(record.encode_to_vec()), tonic::Code::ResourceExhausted),
            ("invalid base64".into(), tonic::Code::DataLoss),
            (STANDARD.encode([0xff]), tonic::Code::DataLoss),
            (
                STANDARD.encode(
                    LpgRecord {
                        key: Some(LpgRecordKey {
                            kind: req.kind,
                            id: "wrong".into(),
                        }),
                        ..Default::default()
                    }
                    .encode_to_vec(),
                ),
                tonic::Code::DataLoss,
            ),
        ] {
            let texts = ExtractedTextsResponse {
                fields: [(format!("{rid}/{FIELD}"), value)].into(),
                ..Default::default()
            };
            let status: Status = page_from_texts(&req, &ids, &texts, false).unwrap_err().into();
            assert_eq!(status.code(), expected);
        }
    }
}

pub(super) async fn get_records(
    index_cache: Arc<IndexCache>,
    request: GetRecordsRequest,
) -> NidxResult<GetRecordsResponse> {
    let shard = Uuid::parse_str(&request.graph_id)?;
    let graph = shard.to_string();
    let ids = request
        .keys
        .iter()
        .map(|key| resource_id(&graph, key))
        .collect::<Result<Vec<_>, _>>()
        .map_err(NidxError::GrpcError)?;
    // Empty reads still verify that this searcher serves the shard.
    if index_cache.get_shard_indexes(&shard).await.is_none() {
        return Err(NidxError::NotFound);
    }
    let texts = shard_text::extracted_texts(
        index_cache,
        ExtractedTextsRequest {
            shard_id: graph,
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
        },
    )
    .await?;
    records_from_texts(&ids, &request.keys, &texts).map_err(NidxError::GrpcError)
}
