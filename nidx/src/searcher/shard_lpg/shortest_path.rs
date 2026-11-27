// Copyright 2021 Bosutech XXI S.L.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use nidx_json::{JsonSearcher, search::JsonSearchRequest};
use nidx_protos::prost::*;
use nidx_protos::{
    ExpandRequest, ExtractedTextsRequest, ExtractedTextsResponse, FindRecordsRequest, LpgDirection, LpgNodeFilterScope,
    LpgRecordKey, LpgRecordKind, ShortestPathRequest, ShortestPathResponse, extracted_texts_request,
};
use nidx_relation::{LpgRelation, RelationSearcher};
use tonic::Status;
use uuid::Uuid;

use crate::errors::{NidxError, NidxResult};
use crate::lpg::{MAX_EXPAND_RESPONSE_BYTES, find_filter, matches_properties, resource_id, validate_shortest_path};

use super::{IndexCache, proto_to_json_filter, record_for_resource, relation_candidates, shard_text};

const MAX_STATE_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Default)]
struct Path {
    found: bool,
    nodes: Vec<String>,
    edges: Vec<Uuid>,
    examined_edges: u32,
}

struct Visit {
    parent: Option<(String, Uuid)>,
    depth: u32,
}

struct Side {
    frontier: Vec<String>,
    visits: HashMap<String, Visit>,
    depth: u32,
}

impl Side {
    fn new(root: &str) -> Self {
        Self {
            frontier: vec![root.into()],
            visits: HashMap::from([(root.into(), Visit { parent: None, depth: 0 })]),
            depth: 0,
        }
    }

    fn path_to_root(&self, node: &str) -> (Vec<String>, Vec<Uuid>) {
        let mut ids = vec![node.to_owned()];
        let mut edges = Vec::new();
        let mut current = node;
        while let Some((parent, edge)) = &self.visits[current].parent {
            ids.push(parent.clone());
            edges.push(*edge);
            current = parent;
        }
        (ids, edges)
    }
}

struct Expansion {
    edges: Vec<LpgRelation>,
    allowed_nodes: HashSet<String>,
}

fn bfs(
    request: &ShortestPathRequest,
    mut expand: impl FnMut(ExpandRequest) -> Result<Expansion, Status>,
) -> Result<Path, Status> {
    let direction = LpgDirection::try_from(request.direction)
        .map_err(|_| Status::invalid_argument("invalid expansion direction"))?;
    let reverse = match direction {
        LpgDirection::LpgOutgoing => LpgDirection::LpgIncoming,
        LpgDirection::LpgIncoming => LpgDirection::LpgOutgoing,
        _ => LpgDirection::LpgBoth,
    };
    let max_hops = if request.max_hops == 0 { 6 } else { request.max_hops };
    let max_visited = if request.max_visited_nodes == 0 {
        1000
    } else {
        request.max_visited_nodes
    } as usize;
    let max_examined = if request.max_examined_edges == 0 {
        10000
    } else {
        request.max_examined_edges
    };
    let mut forward = Side::new(&request.source);
    let mut backward = Side::new(&request.target);
    if request.source == request.target {
        return Ok(Path {
            found: true,
            nodes: vec![request.source.clone()],
            ..Default::default()
        });
    }
    if max_visited < 2 {
        return Err(Status::resource_exhausted("shortest path visited node limit exceeded"));
    }
    let mut state_bytes = 2 * (request.source.len() + request.target.len());
    if state_bytes > MAX_STATE_BYTES {
        return Err(Status::resource_exhausted("shortest path state byte limit exceeded"));
    }
    let mut examined_edges = 0;
    while !forward.frontier.is_empty() && !backward.frontier.is_empty() && forward.depth + backward.depth < max_hops {
        let from_source = forward.frontier.len() <= backward.frontier.len();
        let (active, other) = if from_source {
            (&mut forward, &backward)
        } else {
            (&mut backward, &forward)
        };
        let frontier = std::mem::take(&mut active.frontier);
        let mut meeting: Option<(u32, String)> = None;
        // Finish every batch in the level before choosing a shortest meeting.
        for batch in frontier.chunks(128) {
            if examined_edges == max_examined {
                return Err(Status::resource_exhausted("shortest path examined edge limit exceeded"));
            }
            let expansion_direction = if from_source { direction } else { reverse };
            let expansion = expand(ExpandRequest {
                graph_id: request.graph_id.clone(),
                node_ids: batch.to_vec(),
                direction: expansion_direction as i32,
                edge_types: request.edge_types.clone(),
                edge_filters: request.edge_filters.clone(),
                candidate_limit: (max_examined - examined_edges).min(10000),
                ..Default::default()
            })?;
            if expansion.edges.len() > (max_examined - examined_edges) as usize {
                return Err(Status::resource_exhausted("shortest path examined edge limit exceeded"));
            }
            examined_edges += expansion.edges.len() as u32;
            let starts: HashSet<_> = batch.iter().collect();
            for edge in expansion.edges {
                let outgoing = matches!(expansion_direction, LpgDirection::LpgOutgoing | LpgDirection::LpgBoth)
                    && starts.contains(&edge.source);
                let incoming = matches!(expansion_direction, LpgDirection::LpgIncoming | LpgDirection::LpgBoth)
                    && starts.contains(&edge.target);
                if !outgoing && !incoming {
                    return Err(Status::data_loss("shortest path returned an unrelated edge"));
                }
                for (parent, id) in [
                    outgoing.then_some((&edge.source, &edge.target)),
                    (incoming && !(outgoing && edge.source == edge.target)).then_some((&edge.target, &edge.source)),
                ]
                .into_iter()
                .flatten()
                {
                    if active.visits.contains_key(id) || !expansion.allowed_nodes.contains(id) {
                        continue;
                    }
                    if active.visits.len() + other.visits.len() == max_visited {
                        return Err(Status::resource_exhausted("shortest path visited node limit exceeded"));
                    }
                    state_bytes += 2 * id.len() + parent.len() + 16;
                    if state_bytes > MAX_STATE_BYTES {
                        return Err(Status::resource_exhausted("shortest path state byte limit exceeded"));
                    }
                    let depth = active.depth + 1;
                    active.visits.insert(
                        id.clone(),
                        Visit {
                            parent: Some((parent.clone(), edge.resource_id)),
                            depth,
                        },
                    );
                    active.frontier.push(id.clone());
                    if let Some(visit) = other.visits.get(id) {
                        let distance = depth + visit.depth;
                        if meeting.as_ref().is_none_or(|(best, _)| distance < *best) {
                            meeting = Some((distance, id.clone()));
                        }
                    }
                }
            }
        }
        active.depth += 1;
        if let Some((_, id)) = meeting {
            let (mut nodes, mut edges) = forward.path_to_root(&id);
            nodes.reverse();
            edges.reverse();
            let (tail_nodes, tail_edges) = backward.path_to_root(&id);
            nodes.extend(tail_nodes.into_iter().skip(1));
            edges.extend(tail_edges);
            return Ok(Path {
                found: true,
                nodes,
                edges,
                examined_edges,
            });
        }
    }
    Ok(Path {
        examined_edges,
        ..Default::default()
    })
}

fn node_resources(graph: &str, ids: &[String]) -> Result<Vec<Uuid>, Status> {
    ids.iter()
        .map(|id| {
            let rid = resource_id(
                graph,
                &LpgRecordKey {
                    kind: LpgRecordKind::LpgNode as i32,
                    id: id.clone(),
                },
            )?;
            Uuid::parse_str(&rid).map_err(|_| Status::data_loss("invalid node resource identity"))
        })
        .collect()
}

fn eligible_nodes(
    json: &JsonSearcher,
    request: &ShortestPathRequest,
    ids: &[String],
    missing: Status,
) -> NidxResult<HashSet<String>> {
    let resources = node_resources(&request.graph_id, ids).map_err(NidxError::GrpcError)?;
    let node_filter = |filtered: bool| -> NidxResult<JsonSearchRequest> {
        let filter = find_filter(&FindRecordsRequest {
            kind: LpgRecordKind::LpgNode as i32,
            filters: if filtered { request.node_filters.clone() } else { vec![] },
            label: if filtered { request.node_label.clone() } else { None },
            ..Default::default()
        })
        .map_err(NidxError::GrpcError)?;
        Ok(JsonSearchRequest {
            filter: proto_to_json_filter(&filter)?,
        })
    };
    let existing = json.search_resource_ids(&node_filter(false)?, &resources)?;
    if existing.len() != resources.len() {
        return Err(NidxError::GrpcError(missing));
    }
    let allowed = if request.node_filters.is_empty() && request.node_label.is_none() {
        existing
    } else {
        json.search_resource_ids(&node_filter(true)?, &resources)?
    };
    let exempt_endpoints = request.node_filter_scope == LpgNodeFilterScope::LpgIntermediateNodes as i32;
    Ok(ids
        .iter()
        .zip(resources)
        .filter(|(id, rid)| {
            allowed.contains(rid) || (exempt_endpoints && (*id == &request.source || *id == &request.target))
        })
        .map(|(id, _)| id.clone())
        .collect())
}

fn search_path(json: &JsonSearcher, relation: &RelationSearcher, request: &ShortestPathRequest) -> NidxResult<Path> {
    let endpoints: Vec<_> = [&request.source, &request.target]
        .into_iter()
        .cloned()
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    let allowed = eligible_nodes(
        json,
        request,
        &endpoints,
        Status::not_found("shortest path endpoint not found"),
    )?;
    if allowed.len() != endpoints.len() {
        return Ok(Path::default());
    }
    bfs(request, |expansion| {
        let edges = relation_candidates(relation, Some(json), &expansion, expansion.candidate_limit as usize)
            .map_err(Status::from)?;
        let neighbors: Vec<_> = edges
            .iter()
            .flat_map(|edge| [&edge.source, &edge.target])
            .cloned()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        let allowed_nodes = eligible_nodes(
            json,
            request,
            &neighbors,
            Status::unavailable("shortest path neighbor disappeared"),
        )
        .map_err(Status::from)?;
        Ok(Expansion { edges, allowed_nodes })
    })
    .map_err(NidxError::GrpcError)
}

fn path_response(
    request: &ShortestPathRequest,
    path: &Path,
    texts: &ExtractedTextsResponse,
) -> Result<ShortestPathResponse, Status> {
    let mut response = ShortestPathResponse {
        found: path.found,
        examined_edges: path.examined_edges,
        ..Default::default()
    };
    for rid in node_resources(&request.graph_id, &path.nodes)? {
        response.nodes.push(record_for_resource(
            &request.graph_id,
            LpgRecordKind::LpgNode as i32,
            rid,
            texts,
        )?);
    }
    for rid in &path.edges {
        response.edges.push(record_for_resource(
            &request.graph_id,
            LpgRecordKind::LpgEdge as i32,
            *rid,
            texts,
        )?);
    }
    if response.encoded_len() > MAX_EXPAND_RESPONSE_BYTES {
        return Err(Status::resource_exhausted("shortest path response byte limit exceeded"));
    }
    for (index, node) in response.nodes.iter().enumerate() {
        let exempt = request.node_filter_scope == LpgNodeFilterScope::LpgIntermediateNodes as i32
            && (index == 0 || index + 1 == response.nodes.len());
        if !exempt
            && (request
                .node_label
                .as_ref()
                .is_some_and(|label| !node.labels.contains(label))
                || !matches_properties(node, &request.node_filters)?)
        {
            return Err(Status::unavailable("shortest path node changed during search"));
        }
    }
    let direction = LpgDirection::try_from(request.direction)
        .map_err(|_| Status::invalid_argument("invalid expansion direction"))?;
    for (pair, edge) in response.nodes.windows(2).zip(&response.edges) {
        let source = &pair[0]
            .key
            .as_ref()
            .ok_or_else(|| Status::data_loss("node key missing"))?
            .id;
        let target = &pair[1]
            .key
            .as_ref()
            .ok_or_else(|| Status::data_loss("node key missing"))?
            .id;
        let outgoing = matches!(direction, LpgDirection::LpgOutgoing | LpgDirection::LpgBoth)
            && edge.source.as_ref() == Some(source)
            && edge.target.as_ref() == Some(target);
        let incoming = matches!(direction, LpgDirection::LpgIncoming | LpgDirection::LpgBoth)
            && edge.target.as_ref() == Some(source)
            && edge.source.as_ref() == Some(target);
        if (!outgoing && !incoming)
            || (!request.edge_types.is_empty()
                && !edge
                    .edge_type
                    .as_ref()
                    .is_some_and(|kind| request.edge_types.contains(kind)))
            || !matches_properties(edge, &request.edge_filters)?
        {
            return Err(Status::unavailable("shortest path edge changed during search"));
        }
    }
    Ok(response)
}

pub(in crate::searcher) async fn shortest_path(
    index_cache: Arc<IndexCache>,
    mut request: ShortestPathRequest,
) -> NidxResult<ShortestPathResponse> {
    validate_shortest_path(&request).map_err(NidxError::GrpcError)?;
    let shard = Uuid::parse_str(&request.graph_id)?;
    request.graph_id = shard.to_string();
    let indexes = index_cache.get_shard_indexes(&shard).await.ok_or(NidxError::NotFound)?;
    let json = index_cache
        .get(&indexes.json_index().ok_or(NidxError::NotFound)?)
        .await?;
    let relation = index_cache
        .get(&indexes.relation_index().ok_or(NidxError::NotFound)?)
        .await?;
    let query = request.clone();
    let span = tracing::Span::current();
    let path = tokio::task::spawn_blocking(move || {
        span.in_scope(|| search_path(json.as_ref().into(), relation.as_ref().into(), &query))
    })
    .await??;
    if !path.found {
        return Ok(ShortestPathResponse {
            examined_edges: path.examined_edges,
            ..Default::default()
        });
    }
    let mut resources = node_resources(&request.graph_id, &path.nodes).map_err(NidxError::GrpcError)?;
    resources.extend(&path.edges);
    let mut texts = ExtractedTextsResponse::default();
    for batch in resources.chunks(128) {
        let response = shard_text::extracted_texts(
            Arc::clone(&index_cache),
            ExtractedTextsRequest {
                shard_id: request.graph_id.clone(),
                field_ids: batch
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
        texts.fields.extend(response.fields);
    }
    path_response(&request, &path, &texts).map_err(NidxError::GrpcError)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lpg::{FIELD, property_path};
    use base64::{Engine, engine::general_purpose::STANDARD};
    use nidx_protos::{
        ExpandResponse, IndexRelation, IndexRelations, JsonFieldValue, LpgProperty, LpgPropertyFilter, LpgRecord,
        LpgTextProperty, Relation, RelationNode, Resource, ResourceId, lpg_property, lpg_property_filter,
        relation::RelationType, relation_node::NodeType,
    };
    use nidx_types::{OpenIndexMetadata, SegmentMetadata, Seq};
    use tempfile::TempDir;

    struct TestIndex<T>(Vec<(SegmentMetadata<T>, Seq)>);

    impl<T: Clone> OpenIndexMetadata<T> for TestIndex<T> {
        fn segments(&self) -> impl DoubleEndedIterator<Item = (SegmentMetadata<T>, Seq)> {
            self.0.iter().cloned()
        }
        fn deletions(&self) -> impl DoubleEndedIterator<Item = (&String, Seq)> {
            std::iter::empty()
        }
    }

    struct Graph {
        json: JsonSearcher,
        relation: RelationSearcher,
        texts: ExtractedTextsResponse,
        _json_dir: TempDir,
        _relation_dir: TempDir,
    }

    fn request() -> ShortestPathRequest {
        ShortestPathRequest {
            graph_id: Uuid::new_v4().to_string(),
            source: "source".into(),
            target: "target".into(),
            direction: LpgDirection::LpgBoth as i32,
            ..Default::default()
        }
    }

    fn node(id: &str) -> LpgRecord {
        LpgRecord {
            key: Some(LpgRecordKey {
                kind: LpgRecordKind::LpgNode as i32,
                id: id.into(),
            }),
            labels: vec!["Node".into()],
            ..Default::default()
        }
    }

    fn edge(id: &str, source: &str, target: &str) -> LpgRecord {
        LpgRecord {
            key: Some(LpgRecordKey {
                kind: LpgRecordKind::LpgEdge as i32,
                id: id.into(),
            }),
            source: Some(source.into()),
            target: Some(target.into()),
            edge_type: Some("LINK".into()),
            ..Default::default()
        }
    }

    fn graph(request: &ShortestPathRequest, records: &[LpgRecord]) -> Graph {
        let mut texts = ExtractedTextsResponse::default();
        let resources = records.iter().map(|record| {
            let rid = resource_id(&request.graph_id, record.key.as_ref().unwrap()).unwrap();
            texts.fields.insert(format!("{rid}/{FIELD}"), STANDARD.encode(record.encode_to_vec()));
            let properties = record.properties.iter().map(|(name, property)| {
                let Some(lpg_property::Value::Boolean(value)) = property.value else { panic!("fixture requires boolean properties") };
                (property_path(name), serde_json::Value::Bool(value))
            }).collect::<serde_json::Map<_, _>>();
            let json = serde_json::json!({
                "kind": record.key.as_ref().unwrap().kind, "labels": record.labels,
                "source": record.source, "target": record.target, "edge_type": record.edge_type, "properties": properties,
            });
            let mut resource = Resource {
                resource: Some(ResourceId { uuid: rid.clone(), shard_id: request.graph_id.clone() }),
                json_fields: HashMap::from([(FIELD.into(), JsonFieldValue { value: json.to_string() })]),
                ..Default::default()
            };
            if record.key.as_ref().unwrap().kind == LpgRecordKind::LpgEdge as i32 {
                resource.field_relations.insert("a/metadata".into(), IndexRelations {
                    relations: vec![IndexRelation {
                        relation: Some(Relation {
                            source: Some(RelationNode { value: record.source.clone().unwrap(), ntype: NodeType::Resource as i32, ..Default::default() }),
                            to: Some(RelationNode { value: record.target.clone().unwrap(), ntype: NodeType::Resource as i32, ..Default::default() }),
                            relation: RelationType::Other as i32, relation_label: record.edge_type.clone().unwrap(),
                            ..Default::default()
                        }),
                        facets: vec![format!("/lpg/edge/{rid}")], ..Default::default()
                    }],
                });
            }
            resource
        }).collect::<Vec<_>>();
        let json_dir = TempDir::new().unwrap();
        let json_meta = nidx_json::JsonIndexer
            .index_resources(json_dir.path(), &resources)
            .unwrap()
            .unwrap();
        let json = JsonSearcher::open(TestIndex(vec![(json_meta, 1i64.into())])).unwrap();
        let relation_dir = TempDir::new().unwrap();
        let config = nidx_relation::RelationConfig::default();
        let relation_meta = nidx_relation::RelationIndexer
            .index_resources(relation_dir.path(), &config, &resources)
            .unwrap()
            .unwrap();
        let relation = RelationSearcher::open(config, TestIndex(vec![(relation_meta, 1i64.into())])).unwrap();
        Graph {
            json,
            relation,
            texts,
            _json_dir: json_dir,
            _relation_dir: relation_dir,
        }
    }

    #[test]
    fn large_off_path_records_do_not_limit_traversal_or_require_hydration() {
        let request = request();
        let mut records = vec![
            node("source"),
            node("target"),
            node("hub"),
            edge("first", "source", "hub"),
            edge("last", "hub", "target"),
        ];
        let mut legacy_expansion = ExpandResponse::default();
        for number in 0..20 {
            let id = format!("leaf-{number}");
            let mut leaf = node(&id);
            leaf.text_properties.insert(
                "payload".into(),
                LpgTextProperty {
                    chunks: vec!["x".repeat(200_000)],
                    ..Default::default()
                },
            );
            legacy_expansion.neighbors.push(leaf.clone());
            records.push(leaf);
            records.push(edge(&format!("branch-{number}"), "hub", &id));
        }
        assert!(legacy_expansion.encoded_len() > MAX_EXPAND_RESPONSE_BYTES);
        let graph = graph(&request, &records);
        let path = search_path(&graph.json, &graph.relation, &request).unwrap();
        assert!(path.found);
        assert_eq!(path.nodes, ["source", "hub", "target"]);
        let selected: HashSet<_> = node_resources(&request.graph_id, &path.nodes)
            .unwrap()
            .into_iter()
            .chain(path.edges.iter().copied())
            .map(|rid| format!("{}/{FIELD}", rid.simple()))
            .collect();
        let texts = ExtractedTextsResponse {
            fields: graph
                .texts
                .fields
                .into_iter()
                .filter(|(field, _)| selected.contains(field))
                .collect(),
            ..Default::default()
        };
        assert_eq!(texts.fields.len(), 5);
        let response = path_response(&request, &path, &texts).unwrap();
        assert!(response.found);
        assert_eq!((response.nodes.len(), response.edges.len()), (3, 2));
        assert!(response.encoded_len() < MAX_EXPAND_RESPONSE_BYTES);
    }

    #[test]
    fn filters_scopes_directions_and_hop_limits_select_valid_paths() {
        let mut request = request();
        let mut records = ["source", "target", "bad", "mid1", "mid2"].map(node).to_vec();
        records[0].labels = vec!["Root".into()];
        records[1].labels = vec!["Root".into()];
        for record in &mut records {
            record.properties.insert(
                "active".into(),
                LpgProperty {
                    value: Some(lpg_property::Value::Boolean(record.key.as_ref().unwrap().id != "bad")),
                },
            );
        }
        for (id, source, target, confirmed) in [
            ("bad1", "source", "bad", false),
            ("bad2", "bad", "target", false),
            ("ok1", "source", "mid1", true),
            ("ok2", "mid1", "mid2", true),
            ("ok3", "mid2", "target", true),
            ("parallel", "mid1", "mid2", true),
            ("loop", "mid1", "mid1", true),
        ] {
            let mut edge = edge(id, source, target);
            edge.properties.insert(
                "confirmed".into(),
                LpgProperty {
                    value: Some(lpg_property::Value::Boolean(confirmed)),
                },
            );
            records.push(edge);
        }
        let graph = graph(&request, &records);
        assert_eq!(
            search_path(&graph.json, &graph.relation, &request).unwrap().nodes.len(),
            3
        );
        request.node_label = Some("Node".into());
        request.node_filters = vec![LpgPropertyFilter {
            property: "active".into(),
            predicate: Some(lpg_property_filter::Predicate::Boolean(true)),
        }];
        assert!(!search_path(&graph.json, &graph.relation, &request).unwrap().found);
        request.node_filter_scope = LpgNodeFilterScope::LpgIntermediateNodes as i32;
        request.edge_filters = vec![LpgPropertyFilter {
            property: "confirmed".into(),
            predicate: Some(lpg_property_filter::Predicate::Boolean(true)),
        }];
        for direction in [LpgDirection::LpgBoth, LpgDirection::LpgOutgoing] {
            request.direction = direction as i32;
            let path = search_path(&graph.json, &graph.relation, &request).unwrap();
            assert_eq!(path.nodes, ["source", "mid1", "mid2", "target"]);
            assert_eq!(path_response(&request, &path, &graph.texts).unwrap().edges.len(), 3);
        }
        request.max_hops = 2;
        assert!(!search_path(&graph.json, &graph.relation, &request).unwrap().found);
        request.max_hops = 3;
        std::mem::swap(&mut request.source, &mut request.target);
        assert!(!search_path(&graph.json, &graph.relation, &request).unwrap().found);
        request.direction = LpgDirection::LpgIncoming as i32;
        let path = search_path(&graph.json, &graph.relation, &request).unwrap();
        assert_eq!(path.nodes, ["target", "mid2", "mid1", "source"]);
        assert_eq!(path_response(&request, &path, &graph.texts).unwrap().edges.len(), 3);
        request.target = request.source.clone();
        let path = search_path(&graph.json, &graph.relation, &request).unwrap();
        assert!(path.found);
        assert!(path.edges.is_empty());
        request.target = "missing".into();
        let error: Status = search_path(&graph.json, &graph.relation, &request).unwrap_err().into();
        assert_eq!(error.code(), tonic::Code::NotFound);
    }

    #[test]
    fn disconnected_nodes_and_single_hop_paths_are_not_errors() {
        let mut request = request();
        let graph = graph(
            &request,
            &[
                node("source"),
                node("target"),
                node("isolated"),
                edge("direct", "source", "target"),
            ],
        );
        request.max_hops = 1;
        let path = search_path(&graph.json, &graph.relation, &request).unwrap();
        assert_eq!(path.nodes, ["source", "target"]);
        assert_eq!(path.edges.len(), 1);
        request.target = "isolated".into();
        let path = search_path(&graph.json, &graph.relation, &request).unwrap();
        assert!(!path.found);
        assert!(path.nodes.is_empty());
        assert!(path.edges.is_empty());
    }

    #[test]
    fn bfs_finishes_all_batches_before_returning_a_meeting() {
        let mut edges = Vec::new();
        for number in 0..129 {
            let left = format!("left-{number}");
            let right = format!("right-{number}");
            for (source, target) in [
                ("source".into(), left.clone()),
                (left, right.clone()),
                (right, "target".into()),
            ] {
                edges.push(LpgRelation {
                    resource_id: Uuid::new_v4(),
                    source,
                    target,
                    edge_type: "LINK".into(),
                });
            }
        }
        let request = ShortestPathRequest {
            direction: LpgDirection::LpgOutgoing as i32,
            max_hops: 3,
            ..request()
        };
        for fail_last_batch in [false, true] {
            let mut calls = Vec::new();
            let result = bfs(&request, |request| {
                calls.push((request.direction, request.node_ids.len()));
                if fail_last_batch && request.node_ids.len() == 1 && request.node_ids[0].starts_with("left-") {
                    return Err(Status::unavailable("last batch failed"));
                }
                let matching: Vec<_> = edges
                    .iter()
                    .filter(|edge| {
                        if request.direction == LpgDirection::LpgOutgoing as i32 {
                            request.node_ids.contains(&edge.source)
                        } else {
                            request.node_ids.contains(&edge.target)
                        }
                    })
                    .cloned()
                    .collect();
                let allowed_nodes = matching
                    .iter()
                    .flat_map(|edge| [edge.source.clone(), edge.target.clone()])
                    .collect();
                Ok(Expansion {
                    edges: matching,
                    allowed_nodes,
                })
            });
            assert_eq!(
                calls,
                [
                    (LpgDirection::LpgOutgoing as i32, 1),
                    (LpgDirection::LpgIncoming as i32, 1),
                    (LpgDirection::LpgOutgoing as i32, 128),
                    (LpgDirection::LpgOutgoing as i32, 1)
                ]
            );
            if fail_last_batch {
                assert_eq!(result.unwrap_err().code(), tonic::Code::Unavailable);
            } else {
                let path = result.unwrap();
                assert!(path.found);
                assert_eq!(
                    (path.nodes.len(), path.edges.len(), path.examined_edges),
                    (4, 3, 129 * 3)
                );
            }
        }
    }

    #[test]
    fn work_limits_and_final_response_validation_remain_explicit() {
        let request = request();
        let graph = graph(
            &request,
            &[
                node("source"),
                node("target"),
                node("hub"),
                edge("first", "source", "hub"),
                edge("last", "hub", "target"),
            ],
        );
        for limited in [
            ShortestPathRequest {
                max_visited_nodes: 2,
                ..request.clone()
            },
            ShortestPathRequest {
                max_examined_edges: 1,
                ..request.clone()
            },
        ] {
            let error: Status = search_path(&graph.json, &graph.relation, &limited).unwrap_err().into();
            assert_eq!(error.code(), tonic::Code::ResourceExhausted);
        }
        let path = search_path(&graph.json, &graph.relation, &request).unwrap();
        let field = format!("{}/{FIELD}", path.edges[0].simple());
        let mut texts = graph.texts.clone();
        texts.fields.insert(field.clone(), "invalid base64".into());
        assert_eq!(
            path_response(&request, &path, &texts).unwrap_err().code(),
            tonic::Code::DataLoss
        );
        texts.fields.remove(&field);
        assert_eq!(
            path_response(&request, &path, &texts).unwrap_err().code(),
            tonic::Code::Unavailable
        );
        let mut changed = edge("first", "target", "hub");
        texts.fields.insert(field, STANDARD.encode(changed.encode_to_vec()));
        assert_eq!(
            path_response(&request, &path, &texts).unwrap_err().code(),
            tonic::Code::Unavailable
        );
        changed = node("hub");
        changed.text_properties.insert(
            "payload".into(),
            LpgTextProperty {
                chunks: vec!["x".repeat(MAX_EXPAND_RESPONSE_BYTES)],
                ..Default::default()
            },
        );
        let mut texts = graph.texts;
        let rid = resource_id(&request.graph_id, changed.key.as_ref().unwrap()).unwrap();
        texts
            .fields
            .insert(format!("{rid}/{FIELD}"), STANDARD.encode(changed.encode_to_vec()));
        assert_eq!(
            path_response(&request, &path, &texts).unwrap_err().code(),
            tonic::Code::ResourceExhausted
        );
    }

    #[test]
    fn bfs_bounds_identifier_state_instead_of_record_payloads() {
        let mut request = request();
        request.source = "s".repeat(MAX_STATE_BYTES / 4);
        request.target = "t".repeat(MAX_STATE_BYTES / 4);
        let error = bfs(&request, |request| {
            Ok(Expansion {
                edges: vec![LpgRelation {
                    resource_id: Uuid::new_v4(),
                    source: request.node_ids[0].clone(),
                    target: "neighbor".into(),
                    edge_type: "LINK".into(),
                }],
                allowed_nodes: HashSet::from(["neighbor".into()]),
            })
        })
        .unwrap_err();
        assert_eq!(error.code(), tonic::Code::ResourceExhausted);
        assert!(error.message().contains("state byte limit"));
    }
}
