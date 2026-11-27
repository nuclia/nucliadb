// Copyright 2021 Bosutech XXI S.L.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

use std::collections::HashSet;

use base64::{Engine, engine::general_purpose::STANDARD};
use nidx_protos::prost::*;
use nidx_protos::*;
use time::{OffsetDateTime, UtcOffset, format_description::well_known::Rfc3339};
use tonic::Status;
use uuid::Uuid;

pub(crate) const FIELD: &str = "a/lpg_record_v1";
pub(crate) const MAX_EXPAND_RESPONSE_BYTES: usize = 3 * 1024 * 1024;
pub(crate) const MAX_SEARCH_RESULTS: u32 = 10000;
const NAMESPACE: Uuid = Uuid::from_u128(0x958ee941_b31c_4d0b_9226_f28b07f98180);

pub(crate) fn parse_datetime(value: &str) -> Result<OffsetDateTime, Status> {
    let date = OffsetDateTime::parse(value, &Rfc3339)
        .or_else(|_| OffsetDateTime::parse(&format!("{value}Z"), &Rfc3339))
        .map_err(|_| Status::invalid_argument(format!("invalid datetime: {value:?}")))?;
    i64::try_from(date.unix_timestamp_nanos())
        .map_err(|_| Status::invalid_argument("datetime is outside the date index's supported range"))?;
    Ok(date.to_offset(UtcOffset::UTC))
}

pub(crate) fn date_timestamp(value: &str) -> Result<prost_types::Timestamp, Status> {
    let date = parse_datetime(value)?;
    Ok(prost_types::Timestamp {
        seconds: date.unix_timestamp(),
        nanos: date.nanosecond() as i32,
    })
}

pub(crate) fn property_path(name: &str) -> String {
    let hex: String = name.as_bytes().iter().map(|b| format!("{b:02x}")).collect();
    format!("p_{hex}")
}

pub(crate) fn validate_search(req: &SearchChunksRequest) -> Result<(), Status> {
    if !req.min_score_semantic.is_finite() || !req.min_score_keyword.is_finite() {
        return Err(Status::invalid_argument("minimum scores must be finite"));
    }
    if req.text.as_ref().is_some_and(|text| text.trim().is_empty()) || (req.text.is_none() && req.vector.is_empty()) {
        return Err(Status::invalid_argument("nonempty text or vector required"));
    }
    if !req.vector.is_empty() && (req.vector.iter().any(|v| !v.is_finite()) || req.vector.iter().all(|v| *v == 0.0)) {
        return Err(Status::invalid_argument("vector must be finite and nonzero"));
    }
    if req.kind == Some(LpgRecordKind::Unspecified as i32) {
        return Err(Status::invalid_argument("invalid record kind"));
    }
    filter_expression(req.kind.unwrap_or_default(), &req.filters)?;
    if !(1..=MAX_SEARCH_RESULTS).contains(&req.top_k) {
        return Err(Status::invalid_argument("top_k must be between 1 and 10000"));
    }
    Ok(())
}

pub(crate) fn filter_expression(kind: i32, filters: &[LpgPropertyFilter]) -> Result<JsonFilterExpression, Status> {
    use json_field_path_filter::{DateRangePredicate, FloatRangePredicate, IntegerRangePredicate, Predicate};
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
            Some(Filter::Datetime(v)) => Predicate::Date(date_timestamp(v)?),
            Some(Filter::DateRange(v)) => {
                let lower = v.lower.as_deref().map(date_timestamp).transpose()?;
                let upper = v.upper.as_deref().map(date_timestamp).transpose()?;
                if (lower.is_none() && upper.is_none())
                    || matches!((&lower, &upper), (Some(a), Some(b))
                        if (a.seconds, a.nanos) > (b.seconds, b.nanos))
                {
                    return Err(Status::invalid_argument("invalid date range filter"));
                }
                Predicate::DateRange(DateRangePredicate { lower, upper })
            }
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

pub(crate) fn find_filter(req: &FindRecordsRequest) -> Result<JsonFilterExpression, Status> {
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

pub(crate) fn resource_id(graph: &str, key: &LpgRecordKey) -> Result<String, Status> {
    let kind = LpgRecordKind::try_from(key.kind).map_err(|_| Status::invalid_argument("invalid record kind"))?;
    if kind == LpgRecordKind::Unspecified || key.id.is_empty() {
        return Err(Status::invalid_argument("record kind and nonempty ID required"));
    }
    // Length prefix makes the kind and ID mapping unambiguous.
    let name = format!("{graph}/{}:{}:{}", key.kind, key.id.len(), key.id);
    Ok(Uuid::new_v5(&NAMESPACE, name.as_bytes()).simple().to_string())
}

pub(crate) fn expand_filter(req: &ExpandRequest) -> Result<JsonFilterExpression, Status> {
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
    if req.candidate_limit > 10000 || req.max_results > 10000 {
        return Err(Status::invalid_argument("expansion limits must not exceed 10000"));
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

pub(crate) fn matches_property(record: &LpgRecord, filter: &LpgPropertyFilter) -> Result<bool, Status> {
    use lpg_property::Value;
    use lpg_property_filter::Predicate;
    let value = record
        .properties
        .get(&filter.property)
        .and_then(|property| property.value.as_ref());
    Ok(match (value, filter.predicate.as_ref()) {
        (Some(Value::Datetime(a)), Some(Predicate::Datetime(b))) => parse_datetime(a)? == parse_datetime(b)?,
        (Some(Value::Datetime(a)), Some(Predicate::DateRange(b))) => {
            let date = parse_datetime(a)?;
            let lower = b.lower.as_deref().map(parse_datetime).transpose()?;
            let upper = b.upper.as_deref().map(parse_datetime).transpose()?;
            lower.is_none_or(|lower| date >= lower) && upper.is_none_or(|upper| date <= upper)
        }
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
    })
}

pub(crate) fn matches_properties(record: &LpgRecord, filters: &[LpgPropertyFilter]) -> Result<bool, Status> {
    for filter in filters {
        if !matches_property(record, filter)? {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(crate) fn validate_shortest_path(req: &ShortestPathRequest) -> Result<(), Status> {
    if req.source.is_empty() || req.target.is_empty() {
        return Err(Status::invalid_argument("source and target must be nonempty"));
    }
    LpgNodeFilterScope::try_from(req.node_filter_scope)
        .map_err(|_| Status::invalid_argument("invalid node filter scope"))?;
    expand_filter(&ExpandRequest {
        node_ids: vec![req.source.clone()],
        direction: req.direction,
        edge_types: req.edge_types.clone(),
        edge_filters: req.edge_filters.clone(),
        neighbor_label: req.node_label.clone(),
        neighbor_filters: req.node_filters.clone(),
        ..Default::default()
    })?;
    if req.max_hops > 64 || req.max_visited_nodes > 10000 || req.max_examined_edges > 100000 {
        return Err(Status::invalid_argument(
            "shortest path limits exceed supported maximums",
        ));
    }
    Ok(())
}

pub(crate) fn decode_record(value: &str) -> Result<LpgRecord, Status> {
    let bytes = STANDARD
        .decode(value)
        .map_err(|_| Status::data_loss("invalid LPG envelope"))?;
    LpgRecord::decode(bytes.as_slice()).map_err(|_| Status::data_loss("invalid LPG record"))
}

pub(crate) fn records_from_texts(
    ids: &[String],
    keys: &[LpgRecordKey],
    texts: &ExtractedTextsResponse,
) -> Result<GetRecordsResponse, Status> {
    let mut records = Vec::new();
    for (rid, key) in ids.iter().zip(keys) {
        if let Some(value) = texts.fields.get(&format!("{rid}/{FIELD}")).filter(|v| !v.is_empty()) {
            let record = decode_record(value)?;
            if record.key.as_ref() != Some(key) {
                return Err(Status::data_loss("LPG record key mismatch"));
            }
            records.push(record);
        }
    }
    Ok(GetRecordsResponse { records })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_preserve_order_duplicates_and_record_kind() {
        let graph = Uuid::new_v4().to_string();
        let node_key = LpgRecordKey {
            kind: LpgRecordKind::LpgNode as i32,
            id: "same/id 🦊".into(),
        };
        let edge_key = LpgRecordKey {
            kind: LpgRecordKind::LpgEdge as i32,
            id: node_key.id.clone(),
        };
        let node = LpgRecord {
            key: Some(node_key.clone()),
            labels: vec!["Node".into()],
            ..Default::default()
        };
        let edge = LpgRecord {
            key: Some(edge_key.clone()),
            source: Some(node_key.id.clone()),
            target: Some(node_key.id.clone()),
            edge_type: Some("loop".into()),
            ..Default::default()
        };
        let keys = vec![
            edge_key,
            LpgRecordKey {
                id: "missing".into(),
                ..node_key.clone()
            },
            node_key.clone(),
            node_key,
        ];
        let ids: Vec<_> = keys.iter().map(|key| resource_id(&graph, key).unwrap()).collect();
        let texts = ExtractedTextsResponse {
            fields: [
                (format!("{}/{FIELD}", ids[0]), STANDARD.encode(edge.encode_to_vec())),
                (format!("{}/{FIELD}", ids[2]), STANDARD.encode(node.encode_to_vec())),
            ]
            .into(),
            ..Default::default()
        };
        assert_eq!(
            records_from_texts(&ids, &keys, &texts).unwrap().records,
            vec![edge, node.clone(), node]
        );
        assert!(records_from_texts(&[], &[], &texts).unwrap().records.is_empty());
    }

    #[test]
    fn corrupt_or_mismatched_records_are_data_loss() {
        let key = LpgRecordKey {
            kind: LpgRecordKind::LpgNode as i32,
            id: "node".into(),
        };
        let ids = vec!["rid".into()];
        let keys = vec![key.clone()];
        let field = format!("rid/{FIELD}");
        for value in [
            "!invalid base64".into(),
            STANDARD.encode([0xff]),
            STANDARD.encode(
                LpgRecord {
                    key: Some(LpgRecordKey {
                        id: "other".into(),
                        ..key
                    }),
                    ..Default::default()
                }
                .encode_to_vec(),
            ),
        ] {
            let texts = ExtractedTextsResponse {
                fields: [(field.clone(), value)].into(),
                ..Default::default()
            };
            assert_eq!(
                records_from_texts(&ids, &keys, &texts).unwrap_err().code(),
                tonic::Code::DataLoss
            );
        }
    }
}
