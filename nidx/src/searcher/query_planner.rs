// Copyright (C) 2021 Bosutech XXI S.L.
//
// nucliadb is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at info@nuclia.com.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.
//

use nidx_paragraph::ParagraphSearchRequest;
use nidx_protos::filter_expression::Expr;
use nidx_protos::graph_search_request::QueryKind;
use nidx_protos::{FilterExpression, FilterOperator, GraphSearchRequest, SearchRequest};
use nidx_text::DocumentSearchRequest;
use nidx_text::prefilter::*;
use nidx_types::prefilter::PrefilterResult;
use nidx_types::query_language::*;
use nidx_vector::SEGMENT_TAGS;
use nidx_vector::VectorSearchRequest;

use super::query_language::extract_label_filters;

/// The queries a [`QueryPlan`] has decided to send to each index.
#[derive(Default, Clone)]
pub struct IndexQueries {
    pub prefilter_results: PrefilterResult,
    pub vectors_request: Option<VectorSearchRequest>,
    pub paragraphs_request: Option<ParagraphSearchRequest>,
    pub texts_request: Option<DocumentSearchRequest>,
    pub relations_request: Option<GraphSearchRequest>,
}

impl IndexQueries {
    /// When a pre-filter is run, the result can be used to modify the queries
    /// that the indexes must resolve.
    pub fn apply_prefilter(&mut self, prefiltered: PrefilterResult) {
        if matches!(prefiltered, PrefilterResult::None) {
            // There are no matches so there is no need to run the rest of the search
            self.vectors_request = None;
            self.paragraphs_request = None;
            self.texts_request = None;
            self.relations_request = None;
            return;
        }

        self.prefilter_results = prefiltered;
    }
}

/// A shard reader will use this plan to produce search results as efficiently as
/// possible.
pub struct QueryPlan {
    pub prefilter: Option<PreFilterRequest>,
    pub index_queries: IndexQueries,
}

pub fn build_query_plan(search_request: SearchRequest) -> anyhow::Result<QueryPlan> {
    let graph_request = compute_graph_request(&search_request)?;
    let texts_request = compute_texts_request(&search_request);
    let vectors_request = compute_vectors_request(&search_request)?;
    let paragraphs_request = compute_paragraphs_request(&search_request)?;

    let prefilter = compute_prefilters(&search_request);

    Ok(QueryPlan {
        prefilter,
        index_queries: IndexQueries {
            prefilter_results: PrefilterResult::All,
            vectors_request,
            paragraphs_request,
            texts_request,
            relations_request: graph_request,
        },
    })
}

fn compute_prefilters(search_request: &SearchRequest) -> Option<PreFilterRequest> {
    let prefilter_request = PreFilterRequest {
        security: search_request.security.clone(),
        filter_expression: search_request.field_filter.clone(),
    };

    if prefilter_request.security.is_some() || prefilter_request.filter_expression.is_some() {
        Some(prefilter_request)
    } else {
        None
    }
}

fn compute_paragraphs_request(search_request: &SearchRequest) -> anyhow::Result<Option<ParagraphSearchRequest>> {
    if !search_request.paragraph {
        return Ok(None);
    }

    Ok(Some(ParagraphSearchRequest {
        uuid: "".to_string(),
        body: search_request.body.clone(),
        order: search_request.order.clone(),
        faceted: search_request.faceted.clone(),
        result_per_page: search_request.result_per_page,
        with_duplicates: search_request.with_duplicates,
        only_faceted: search_request.only_faceted,
        advanced_query: search_request.advanced_query.clone(),
        id: String::default(),
        min_score: search_request.min_score_bm25,
        security: search_request.security.clone(),
        filtering_formula: search_request
            .paragraph_filter
            .clone()
            .map(filter_to_boolean_expression)
            .transpose()?,
        filter_or: search_request.filter_operator == FilterOperator::Or as i32,
    }))
}

fn compute_texts_request(search_request: &SearchRequest) -> Option<DocumentSearchRequest> {
    if !search_request.document {
        return None;
    }

    Some(DocumentSearchRequest {
        id: search_request.shard.clone(),
        body: search_request.body.clone(),
        order: search_request.order.clone(),
        faceted: search_request.faceted.clone(),
        result_per_page: search_request.result_per_page,
        only_faceted: search_request.only_faceted,
        advanced_query: search_request.advanced_query.clone(),
        min_score: search_request.min_score_bm25,
        filter_expression: search_request.field_filter.clone(),
    })
}

fn compute_vectors_request(search_request: &SearchRequest) -> anyhow::Result<Option<VectorSearchRequest>> {
    if search_request.result_per_page == 0 || search_request.vector.is_empty() {
        return Ok(None);
    }

    let segment_filtering_formula = search_request
        .field_filter
        .as_ref()
        .and_then(|f| extract_label_filters(f, SEGMENT_TAGS));

    Ok(Some(VectorSearchRequest {
        id: search_request.shard.clone(),
        vector_set: search_request.vectorset.clone(),
        vector: search_request.vector.clone(),
        result_per_page: search_request.result_per_page,
        with_duplicates: search_request.with_duplicates,
        min_score: search_request.min_score_semantic,
        filtering_formula: search_request
            .paragraph_filter
            .clone()
            .map(filter_to_boolean_expression)
            .transpose()?,
        segment_filtering_formula,
        filter_or: search_request.filter_operator == FilterOperator::Or as i32,
    }))
}

#[allow(deprecated)]
fn compute_graph_request(search_request: &SearchRequest) -> anyhow::Result<Option<GraphSearchRequest>> {
    let Some(graph_search) = search_request.graph_search.clone() else {
        return Ok(None);
    };

    Ok(Some(GraphSearchRequest {
        query: graph_search.query,
        top_k: std::cmp::max(search_request.result_per_page as u32, 20),
        kind: QueryKind::Path.into(),
        // we don't need to populate filters nor shard as they won't be used in search. Prefilter
        // will be done with request filters and shard have been already obtained
        ..Default::default()
    }))
}

pub fn filter_to_boolean_expression(filter: FilterExpression) -> anyhow::Result<BooleanExpression<String>> {
    match filter.expr.unwrap() {
        Expr::BoolAnd(and) => {
            let operands = and
                .operands
                .into_iter()
                .map(filter_to_boolean_expression)
                .collect::<anyhow::Result<Vec<BooleanExpression<String>>>>()?;
            Ok(BooleanExpression::Operation(BooleanOperation {
                operator: Operator::And,
                operands,
            }))
        }
        Expr::BoolOr(or) => {
            let operands = or
                .operands
                .into_iter()
                .map(filter_to_boolean_expression)
                .collect::<anyhow::Result<Vec<BooleanExpression<String>>>>()?;
            Ok(BooleanExpression::Operation(BooleanOperation {
                operator: Operator::Or,
                operands,
            }))
        }
        Expr::BoolNot(not) => Ok(BooleanExpression::Not(Box::new(filter_to_boolean_expression(*not)?))),
        Expr::Facet(facet_filter) => Ok(BooleanExpression::Literal(facet_filter.facet)),
        _ => Err(anyhow::anyhow!(
            "FilterExpression type not supported for conversion to BooleanExpression"
        )),
    }
}

#[cfg(test)]
mod tests {
    use nidx_protos::filter_expression::{FacetFilter, FilterExpressionList};

    use super::*;
    #[test]
    fn proper_propagation() {
        #[allow(deprecated)]
        let request = SearchRequest {
            result_per_page: 10,
            vector: vec![1.0],
            paragraph: true,
            field_filter: Some(FilterExpression {
                expr: Some(Expr::Facet(FacetFilter { facet: "this".into() })),
            }),
            paragraph_filter: Some(FilterExpression {
                expr: Some(Expr::BoolAnd(FilterExpressionList {
                    operands: vec![
                        FilterExpression {
                            expr: Some(Expr::Facet(FacetFilter { facet: "and".into() })),
                        },
                        FilterExpression {
                            expr: Some(Expr::Facet(FacetFilter { facet: "that".into() })),
                        },
                    ],
                })),
            }),
            ..Default::default()
        };
        let query_plan = build_query_plan(request).unwrap();
        let Some(prefilter) = query_plan.prefilter else {
            panic!("There should be a prefilter");
        };
        let Some(formula) = prefilter.filter_expression else {
            panic!("The prefilter should have a formula");
        };
        let FilterExpression {
            expr: Some(Expr::Facet(FacetFilter { facet: literal })),
        } = &formula
        else {
            panic!("The formula should be a literal")
        };
        assert_eq!(literal, "this");

        let index_queries = query_plan.index_queries;
        let vectors_request = index_queries.vectors_request.unwrap();
        let paragraphs_request = index_queries.paragraphs_request.unwrap();
        assert_eq!(vectors_request.filtering_formula, paragraphs_request.filtering_formula);

        let Some(formula) = paragraphs_request.filtering_formula else {
            panic!("there should be a paragraphs formula")
        };
        let BooleanExpression::Operation(inner_formula) = formula else {
            panic!("the inner formula should be an operation");
        };
        assert!(matches!(inner_formula.operator, Operator::And));
        let BooleanExpression::Literal(literal) = &inner_formula.operands[0] else {
            panic!("first operand should be a literal");
        };
        assert_eq!(literal, "and");
        let BooleanExpression::Literal(literal) = &inner_formula.operands[1] else {
            panic!("second operand should be a literal");
        };
        assert_eq!(literal, "that");
    }
}
