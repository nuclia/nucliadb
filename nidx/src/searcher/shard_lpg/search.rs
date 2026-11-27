// Copyright 2021 Bosutech XXI S.L.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use nidx_protos::prost::*;
use nidx_protos::{
    ExtractedTextsRequest, ExtractedTextsResponse, FilterExpression, LpgChunkHit, SearchChunksRequest,
    SearchChunksResponse, SearchRequest, SearchResponse, extracted_texts_request, filter_expression,
};
use tonic::Status;
use uuid::Uuid;

use crate::errors::{NidxError, NidxResult};
use crate::lpg::{
    FIELD, MAX_SEARCH_RESULTS, decode_record, filter_expression as record_filter, matches_properties, property_path,
    resource_id, validate_search,
};
use crate::searcher::{shard_search, shard_text};

use super::IndexCache;

fn index_request(req: &SearchChunksRequest) -> Result<SearchRequest, Status> {
    validate_search(req)?;
    // RRF needs a deeper window than the final result set; filling it is normal.
    let window = if req.text.is_some() && !req.vector.is_empty() {
        req.top_k.saturating_mul(10).clamp(100, MAX_SEARCH_RESULTS)
    } else {
        req.top_k
    };
    Ok(SearchRequest {
        shard_ids: vec![req.graph_id.clone()],
        body: req.text.clone().unwrap_or_default(),
        paragraph: req.text.is_some(),
        vector: req.vector.clone(),
        vectorset: req.vectorset.clone(),
        min_score_semantic: req.min_score_semantic,
        min_score_bm25: req.min_score_keyword,
        result_per_page: window as i32,
        with_duplicates: true,
        json_filter: if req.kind.is_some() || !req.filters.is_empty() {
            Some(record_filter(req.kind.unwrap_or_default(), &req.filters)?)
        } else {
            None
        },
        field_filter: req.property.as_ref().map(|property| FilterExpression {
            expr: Some(filter_expression::Expr::Field(filter_expression::FieldFilter {
                field_type: "t".into(),
                field_id: Some(property_path(property)),
            })),
        }),
        ..Default::default()
    })
}

#[derive(Debug, Default)]
struct ChunkCandidate {
    id: String,
    rid: String,
    field: String,
    start: usize,
    end: usize,
    text_rank: Option<u32>,
    vector_rank: Option<u32>,
}

impl ChunkCandidate {
    fn score(&self, rrf_k: u32) -> f64 {
        let k = if rrf_k == 0 { 60 } else { rrf_k } as f64;
        self.text_rank.map_or(0.0, |rank| 1.0 / (k + rank as f64))
            + self.vector_rank.map_or(0.0, |rank| 1.0 / (k + rank as f64))
    }
}

fn chunk_candidate(id: &str) -> Result<ChunkCandidate, Status> {
    let parts: Vec<_> = id.split('/').collect();
    if parts.len() != 4 || parts[1] != "t" || !parts[2].starts_with("p_") {
        return Err(Status::data_loss("invalid chunk ID"));
    }
    let rid = Uuid::parse_str(parts[0]).map_err(|_| Status::data_loss("invalid chunk resource ID"))?;
    let (start, end) = parts[3]
        .split_once('-')
        .ok_or_else(|| Status::data_loss("invalid chunk ID"))?;
    let start = start.parse().map_err(|_| Status::data_loss("invalid chunk offset"))?;
    let end = end.parse().map_err(|_| Status::data_loss("invalid chunk offset"))?;
    if start >= end {
        return Err(Status::data_loss("invalid chunk offset"));
    }
    Ok(ChunkCandidate {
        id: id.into(),
        rid: rid.simple().to_string(),
        field: format!("{}/{}", parts[1], parts[2]),
        start,
        end,
        ..Default::default()
    })
}

fn ranked_candidates(req: &SearchChunksRequest, response: SearchResponse) -> Result<Vec<ChunkCandidate>, Status> {
    let mut candidates: HashMap<String, ChunkCandidate> = HashMap::new();
    if let Some(paragraphs) = response.paragraph {
        let mut rank = 0;
        for hit in paragraphs.results {
            let field = hit.field.trim_matches('/');
            if !field.starts_with("t/p_") {
                continue;
            }
            rank += 1;
            let id = format!("{}/{field}/{}-{}", hit.uuid, hit.start, hit.end);
            let mut candidate = chunk_candidate(&id)?;
            candidate.text_rank = Some(rank);
            candidates.insert(id, candidate);
        }
    }
    if let Some(vectors) = response.vector {
        for (index, hit) in vectors.documents.into_iter().enumerate() {
            let id = hit.doc_id.ok_or_else(|| Status::data_loss("missing vector ID"))?.id;
            candidates
                .entry(id.clone())
                .or_insert(chunk_candidate(&id)?)
                .vector_rank = Some(index as u32 + 1);
        }
    }
    let mut candidates: Vec<_> = candidates.into_values().collect();
    candidates.sort_by(|a, b| {
        b.score(req.rrf_k)
            .total_cmp(&a.score(req.rrf_k))
            .then_with(|| a.id.cmp(&b.id))
    });
    candidates.truncate(req.top_k as usize);
    Ok(candidates)
}

fn hydration_request(graph: &str, candidates: &[ChunkCandidate]) -> ExtractedTextsRequest {
    let ids: HashSet<_> = candidates.iter().map(|c| c.rid.clone()).collect();
    ExtractedTextsRequest {
        shard_id: graph.into(),
        field_ids: ids
            .into_iter()
            .map(|rid| extracted_texts_request::FieldId {
                rid,
                field_type: "a".into(),
                field_name: "lpg_record_v1".into(),
                split: None,
            })
            .collect(),
        paragraph_ids: vec![],
    }
}

fn hydrated_response(
    req: &SearchChunksRequest,
    candidates: Vec<ChunkCandidate>,
    texts: ExtractedTextsResponse,
) -> Result<SearchChunksResponse, Status> {
    let mut records = HashMap::new();
    let mut hits = Vec::with_capacity(candidates.len());
    let mut response_bytes = 0;
    for candidate in candidates {
        if !records.contains_key(&candidate.rid) {
            let value = texts
                .fields
                .get(&format!("{}/{FIELD}", candidate.rid))
                .filter(|value| !value.is_empty())
                .ok_or_else(|| Status::unavailable("search candidate disappeared"))?;
            let record = decode_record(value)?;
            let key = record
                .key
                .as_ref()
                .ok_or_else(|| Status::data_loss("record key missing"))?;
            if resource_id(&req.graph_id, key).map_err(|_| Status::data_loss("invalid record key"))? != candidate.rid
                || req.kind.is_some_and(|kind| key.kind != kind)
            {
                return Err(Status::data_loss("search candidate identity mismatch"));
            }
            if !matches_properties(&record, &req.filters)? {
                return Err(Status::unavailable("search candidate changed during search"));
            }
            records.insert(candidate.rid.clone(), record);
        }
        let record = &records[&candidate.rid];
        let mut matched = None;
        for (name, property) in &record.text_properties {
            if format!("t/{}", property_path(name)) != candidate.field {
                continue;
            }
            let mut start = 0;
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
        let hit = LpgChunkHit {
            record: Some(record.clone()),
            property,
            chunk_index,
            text,
            score: candidate.score(req.rrf_k),
            text_rank: candidate.text_rank,
            vector_rank: candidate.vector_rank,
        };
        let size = hit.encoded_len();
        response_bytes += 1 + length_delimiter_len(size) + size;
        if response_bytes > 3 * 1024 * 1024 {
            return Err(Status::resource_exhausted("search response byte limit exceeded"));
        }
        hits.push(hit);
    }
    Ok(SearchChunksResponse { hits })
}

pub(in crate::searcher) async fn search_chunks(
    index_cache: Arc<IndexCache>,
    mut req: SearchChunksRequest,
) -> NidxResult<SearchChunksResponse> {
    let shard = Uuid::parse_str(&req.graph_id)?;
    req.graph_id = shard.to_string();
    let request = index_request(&req).map_err(NidxError::GrpcError)?;
    let mut responses = shard_search::search(Arc::clone(&index_cache), request, vec![shard]).await?;
    let response = responses
        .pop()
        .ok_or_else(|| NidxError::GrpcError(Status::unavailable("missing shard search")))?;
    let candidates = ranked_candidates(&req, response).map_err(NidxError::GrpcError)?;
    if candidates.is_empty() {
        return Ok(SearchChunksResponse::default());
    }
    let texts = shard_text::extracted_texts(index_cache, hydration_request(&req.graph_id, &candidates)).await?;
    hydrated_response(&req, candidates, texts).map_err(NidxError::GrpcError)
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{Engine, engine::general_purpose::STANDARD};
    use nidx_protos::{
        DocumentScored, DocumentVectorIdentifier, LpgRecord, LpgRecordKey, LpgRecordKind, LpgTextProperty,
        ParagraphResult, ParagraphSearchResponse, VectorSearchResponse,
    };

    fn fixture() -> (SearchChunksRequest, Vec<LpgRecord>, Vec<String>) {
        let req = SearchChunksRequest {
            graph_id: Uuid::new_v4().to_string(),
            text: Some("apple".into()),
            vector: vec![1.0, 0.0, 0.0],
            property: Some("description".into()),
            top_k: 1,
            ..Default::default()
        };
        let records: Vec<_> = ["a", "b", "c"]
            .into_iter()
            .map(|id| LpgRecord {
                key: Some(LpgRecordKey {
                    kind: LpgRecordKind::LpgNode as i32,
                    id: id.into(),
                }),
                labels: vec!["Node".into()],
                text_properties: HashMap::from([(
                    "description".into(),
                    LpgTextProperty {
                        chunks: vec!["apple".into()],
                        ..Default::default()
                    },
                )]),
                ..Default::default()
            })
            .collect();
        let ids = records
            .iter()
            .map(|record| {
                format!(
                    "{}/t/{}/0-5",
                    resource_id(&req.graph_id, record.key.as_ref().unwrap()).unwrap(),
                    property_path("description")
                )
            })
            .collect();
        (req, records, ids)
    }

    fn response(text: &[String], vector: &[String]) -> SearchResponse {
        SearchResponse {
            paragraph: Some(ParagraphSearchResponse {
                next_page: true,
                results: text
                    .iter()
                    .map(|id| {
                        let candidate = chunk_candidate(id).unwrap();
                        ParagraphResult {
                            uuid: candidate.rid,
                            field: candidate.field,
                            start: 0,
                            end: 5,
                            ..Default::default()
                        }
                    })
                    .collect(),
                ..Default::default()
            }),
            vector: Some(VectorSearchResponse {
                documents: vector
                    .iter()
                    .map(|id| DocumentScored {
                        doc_id: Some(DocumentVectorIdentifier { id: id.clone() }),
                        ..Default::default()
                    })
                    .collect(),
            }),
            ..Default::default()
        }
    }

    #[test]
    fn saturated_windows_are_ranked_not_rejected_and_only_winners_are_hydrated() {
        let (mut req, records, ids) = fixture();
        // "c" wins fusion despite not leading either modality.
        let ranked = ranked_candidates(
            &req,
            response(&[ids[0].clone(), ids[2].clone()], &[ids[1].clone(), ids[2].clone()]),
        )
        .unwrap();
        assert_eq!(ranked.len(), 1);
        assert_eq!(ranked[0].text_rank, Some(2));
        assert_eq!(ranked[0].vector_rank, Some(2));
        let hydration = hydration_request(&req.graph_id, &ranked);
        assert_eq!(hydration.field_ids.len(), 1);
        let rid = resource_id(&req.graph_id, records[2].key.as_ref().unwrap()).unwrap();
        assert_eq!(hydration.field_ids[0].rid, rid);
        let texts = ExtractedTextsResponse {
            fields: HashMap::from([(format!("{rid}/{FIELD}"), STANDARD.encode(records[2].encode_to_vec()))]),
            ..Default::default()
        };
        let result = hydrated_response(&req, ranked, texts).unwrap();
        assert_eq!(result.hits[0].record.as_ref().unwrap().key.as_ref().unwrap().id, "c");
        assert_eq!(result.hits[0].score, 2.0 / 62.0);
        req.vector.clear();
        let ranked = ranked_candidates(&req, response(&ids, &[])).unwrap();
        assert_eq!(ranked[0].id, ids[0]);
        req.text = None;
        req.vector = vec![1.0, 0.0, 0.0];
        let vector_ids: Vec<_> = (1..=1001)
            .map(|index| format!("{}/t/p_64657363/0-5", Uuid::from_u128(index).simple()))
            .collect();
        let ranked = ranked_candidates(&req, response(&[], &vector_ids)).unwrap();
        assert_eq!(ranked[0].id, vector_ids[0]);
    }

    #[test]
    fn native_requests_preserve_filters_scores_and_use_top_k_or_hybrid_windows() {
        let (mut req, _, _) = fixture();
        req.min_score_semantic = 0.7;
        req.min_score_keyword = 2.0;
        req.kind = Some(LpgRecordKind::LpgNode as i32);
        let search = index_request(&req).unwrap();
        assert_eq!(search.result_per_page, 100);
        assert_eq!(search.min_score_semantic, 0.7);
        assert_eq!(search.min_score_bm25, 2.0);
        assert!(search.json_filter.is_some());
        assert!(search.field_filter.is_some());
        req.vector.clear();
        assert_eq!(index_request(&req).unwrap().result_per_page, 1);
        req.vector = vec![1.0];
        req.text = None;
        assert_eq!(index_request(&req).unwrap().result_per_page, 1);
    }

    #[test]
    fn final_payload_limits_and_stale_records_still_fail_explicitly() {
        let (req, mut records, ids) = fixture();
        let candidates = || ranked_candidates(&req, response(&ids[..1], &[])).unwrap();
        assert_eq!(
            hydrated_response(&req, candidates(), ExtractedTextsResponse::default())
                .unwrap_err()
                .code(),
            tonic::Code::Unavailable
        );
        records[0].revision = "x".repeat(3 * 1024 * 1024);
        let rid = resource_id(&req.graph_id, records[0].key.as_ref().unwrap()).unwrap();
        let texts = ExtractedTextsResponse {
            fields: HashMap::from([(format!("{rid}/{FIELD}"), STANDARD.encode(records[0].encode_to_vec()))]),
            ..Default::default()
        };
        let error = hydrated_response(&req, candidates(), texts).unwrap_err();
        assert_eq!(error.code(), tonic::Code::ResourceExhausted);
        assert!(error.message().contains("response byte limit"));
        assert!(chunk_candidate("123/a/lpg/2-6").is_err());
        assert!(chunk_candidate(&ids[0].replace("0-5", "6-2")).is_err());
    }
}
