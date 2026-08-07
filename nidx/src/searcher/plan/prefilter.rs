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

use tracing::Span;

use nidx_json::JsonSearcher;
use nidx_json::search::{JsonFilterExpression, JsonPathFilter, JsonPredicate, JsonSearchRequest};
use nidx_protos::SearchRequest;
use nidx_protos::json_field_path_filter::Predicate;
use nidx_text::TextSearcher;
use nidx_text::prefilter::*;
use nidx_types::prefilter::{FilterOperator, PrefilterResult};

use crate::errors::NidxResult;
use crate::searcher::query_planner::proto_filter_operator;

/// A filter step using the text and/or json indexes. Prefiltering generates a
/// set of resource/fields to narrow other indexes search.
#[derive(Default, Clone)]
pub struct Prefilter {
    pub text: Option<PreFilterRequest>,
    pub json: Option<JsonSearchRequest>,
    pub filter_operator: FilterOperator,
}

impl Prefilter {
    pub fn run(
        mut self,
        text_searcher: Option<&TextSearcher>,
        json_searcher: Option<&JsonSearcher>,
    ) -> NidxResult<PrefilterResult> {
        if self.text.is_none() && self.json.is_none() {
            return Ok(PrefilterResult::All);
        }

        let text: Option<(&TextSearcher, PreFilterRequest)> = if let Some(query) = self.text.take() {
            Some((
                text_searcher.expect("text searcher is required when text prefilter is needed"),
                query,
            ))
        } else {
            None
        };
        let json: Option<(&JsonSearcher, JsonSearchRequest)> = if let Some(query) = self.json.take() {
            Some((
                json_searcher.expect("text searcher is required when text prefilter is needed"),
                query,
            ))
        } else {
            None
        };

        let mut text_result = None;
        let mut json_result = None;
        std::thread::scope(|scope| {
            if let Some((text_searcher, query)) = text {
                let span = Span::current();
                let result = &mut text_result;
                scope.spawn(move || *result = Some(span.in_scope(|| text_searcher.prefilter(&query))));
            }

            if let Some((json_searcher, query)) = json {
                let span = Span::current();
                let result = &mut json_result;
                scope.spawn(move || *result = Some(span.in_scope(|| json_searcher.search(&query))));
            }
        });

        let text_prefilter = text_result.transpose()?.unwrap_or(PrefilterResult::All);
        let combined = if let Some(uuids) = json_result.transpose()? {
            text_prefilter.combine(uuids, self.filter_operator)
        } else {
            text_prefilter
        };

        Ok(combined)
    }

    pub fn parse_search(request: &SearchRequest) -> anyhow::Result<Self> {
        let text_prefilter = compute_prefilters(request);
        let json_prefilter = compute_json_request(request)?;
        let filter_operator = proto_filter_operator(request.filter_operator)?;

        Ok(Self {
            text: text_prefilter,
            json: json_prefilter,
            filter_operator,
        })
    }
}

fn compute_prefilters(request: &SearchRequest) -> Option<PreFilterRequest> {
    let prefilter_request = PreFilterRequest {
        security: request.security.clone(),
        filter_expression: request.field_filter.clone(),
    };

    if prefilter_request.security.is_some() || prefilter_request.filter_expression.is_some() {
        Some(prefilter_request)
    } else {
        None
    }
}

fn compute_json_request(request: &SearchRequest) -> anyhow::Result<Option<JsonSearchRequest>> {
    let Some(json_filter) = &request.json_filter else {
        return Ok(None);
    };
    Ok(Some(JsonSearchRequest {
        filter: proto_to_json_filter(json_filter)?,
    }))
}

fn proto_to_json_filter(expr: &nidx_protos::JsonFilterExpression) -> anyhow::Result<JsonFilterExpression> {
    use nidx_protos::json_filter_expression::Expr as JsonExpr;

    match expr
        .expr
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Empty JsonFilterExpression"))?
    {
        JsonExpr::BoolAnd(list) => {
            let operands = list
                .operands
                .iter()
                .map(proto_to_json_filter)
                .collect::<anyhow::Result<Vec<_>>>()?;
            Ok(JsonFilterExpression::And(operands))
        }
        JsonExpr::BoolOr(list) => {
            let operands = list
                .operands
                .iter()
                .map(proto_to_json_filter)
                .collect::<anyhow::Result<Vec<_>>>()?;
            Ok(JsonFilterExpression::Or(operands))
        }
        JsonExpr::BoolNot(inner) => Ok(JsonFilterExpression::Not(Box::new(proto_to_json_filter(inner)?))),
        JsonExpr::Path(path_filter) => {
            let predicate = match path_filter
                .predicate
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("Missing predicate"))?
            {
                Predicate::Text(s) => JsonPredicate::Text(s.clone()),
                Predicate::Int(i) => JsonPredicate::Int(*i),
                Predicate::IntRange(r) => JsonPredicate::IntRange {
                    lower: r.lower,
                    upper: r.upper,
                },
                Predicate::Float(f) => JsonPredicate::Float(*f),
                Predicate::FloatRange(r) => JsonPredicate::FloatRange {
                    lower: r.lower,
                    upper: r.upper,
                },
                Predicate::Boolean(b) => JsonPredicate::Boolean(*b),
                Predicate::Date(ts) => JsonPredicate::Date(nidx_json::DateTime::from_timestamp_secs(ts.seconds)),
                Predicate::DateRange(r) => {
                    let ts_to_dt =
                        |ts: &nidx_protos::prost_types::Timestamp| nidx_json::DateTime::from_timestamp_secs(ts.seconds);
                    JsonPredicate::DateRange {
                        lower: r.lower.as_ref().map(ts_to_dt),
                        upper: r.upper.as_ref().map(ts_to_dt),
                    }
                }
            };
            Ok(JsonFilterExpression::Path(JsonPathFilter {
                field_id: path_filter.field_id.clone(),
                json_path: path_filter.json_path.clone(),
                predicate,
            }))
        }
    }
}
