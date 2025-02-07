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
use nidx_protos::{RelationSearchRequest, SearchRequest};
use nidx_text::prefilter::*;
use nidx_text::DocumentSearchRequest;
use nidx_types::prefilter::PrefilterResult;
use nidx_types::query_language::*;
use nidx_vector::VectorSearchRequest;
use nidx_vector::SEGMENT_TAGS;

use super::query_language;
use super::query_language::QueryAnalysis;

/// The queries a [`QueryPlan`] has decided to send to each index.
#[derive(Default, Clone)]
pub struct IndexQueries {
    pub prefilter_results: PrefilterResult,
    pub vectors_request: Option<VectorSearchRequest>,
    pub paragraphs_request: Option<ParagraphSearchRequest>,
    pub texts_request: Option<DocumentSearchRequest>,
    pub relations_request: Option<RelationSearchRequest>,
}

impl IndexQueries {
    fn apply_to_vectors(request: &mut VectorSearchRequest, response: &PrefilterResult) {
        let PrefilterResult::Some(_) = &response else {
            return;
        };

        // Clear labels to avoid duplicate filtering
        request.field_labels.clear();
    }

    fn apply_to_paragraphs(request: &mut ParagraphSearchRequest, response: &PrefilterResult) {
        if matches!(response, PrefilterResult::All) {
            // Since all the fields are matching there is no need to use this filter.
            request.timestamps = None;
        }
    }

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

        if let Some(vectors_request) = self.vectors_request.as_mut() {
            IndexQueries::apply_to_vectors(vectors_request, &self.prefilter_results);
        };

        if let Some(paragraph_request) = self.paragraphs_request.as_mut() {
            IndexQueries::apply_to_paragraphs(paragraph_request, &self.prefilter_results);
        }
    }
}

/// A shard reader will use this plan to produce search results as efficiently as
/// possible.
pub struct QueryPlan {
    pub prefilter: Option<PreFilterRequest>,
    pub index_queries: IndexQueries,
}

fn analyze_filter(search_request: &SearchRequest) -> anyhow::Result<query_language::QueryAnalysis> {
    let Some(filter) = &search_request.filter else {
        return Ok(Default::default());
    };
    let context = QueryContext {
        field_labels: filter.field_labels.iter().cloned().collect(),
        paragraph_labels: filter.paragraph_labels.iter().cloned().collect(),
    };
    query_language::translate(Some(&filter.labels_expression), Some(&filter.keywords_expression), &context)
}

pub fn build_query_plan(search_request: SearchRequest) -> anyhow::Result<QueryPlan> {
    let relations_request = compute_relations_request(&search_request);
    let query_analysis = analyze_filter(&search_request)?;
    let texts_request = compute_texts_request(&search_request, &query_analysis);
    let vectors_request = compute_vectors_request(&search_request, &query_analysis);
    let paragraphs_request = compute_paragraphs_request(&search_request, &query_analysis.search_query);

    let prefilter = compute_prefilters(
        &search_request,
        query_analysis.labels_prefilter_query.clone(),
        query_analysis.keywords_prefilter_query,
    );

    Ok(QueryPlan {
        prefilter,
        index_queries: IndexQueries {
            prefilter_results: PrefilterResult::All,
            vectors_request,
            paragraphs_request,
            texts_request,
            relations_request,
        },
    })
}

fn compute_prefilters(
    search_request: &SearchRequest,
    labels: Option<BooleanExpression>,
    keywords: Option<BooleanExpression>,
) -> Option<PreFilterRequest> {
    let mut prefilter_request = PreFilterRequest {
        timestamp_filters: vec![],
        labels_formula: labels,
        security: None,
        keywords_formula: keywords,
        key_filter: search_request.key_filters.clone(),
    };

    // Security filters
    let request_has_security_filters = search_request.security.is_some();
    if request_has_security_filters {
        prefilter_request.security = search_request.security.clone();
    }

    // Timestamp filters
    let request_has_timestamp_filters = search_request.timestamps.as_ref().is_some();
    if request_has_timestamp_filters {
        let timestamp_filters = compute_timestamp_prefilters(search_request);
        prefilter_request.timestamp_filters.extend(timestamp_filters);
    }

    let request_has_labels_filters = prefilter_request.labels_formula.is_some();
    let request_has_keywords_filters = prefilter_request.keywords_formula.is_some();
    if !request_has_timestamp_filters
        && !request_has_labels_filters
        && !request_has_keywords_filters
        && !request_has_security_filters
        && prefilter_request.key_filter.is_empty()
    {
        None
    } else {
        Some(prefilter_request)
    }
}

fn compute_timestamp_prefilters(search_request: &SearchRequest) -> Vec<TimestampFilter> {
    let mut timestamp_prefilters = vec![];
    let Some(request_timestamp_filters) = search_request.timestamps.as_ref() else {
        return timestamp_prefilters;
    };

    let modified_filter = TimestampFilter {
        applies_to: FieldDateType::Modified,
        from: request_timestamp_filters.from_modified,
        to: request_timestamp_filters.to_modified,
    };
    timestamp_prefilters.push(modified_filter);

    let created_filter = TimestampFilter {
        applies_to: FieldDateType::Created,
        from: request_timestamp_filters.from_created,
        to: request_timestamp_filters.to_created,
    };
    timestamp_prefilters.push(created_filter);
    timestamp_prefilters
}

fn compute_paragraphs_request(
    search_request: &SearchRequest,
    search_query: &Option<BooleanExpression>,
) -> Option<ParagraphSearchRequest> {
    if !search_request.paragraph {
        return None;
    }

    Some(ParagraphSearchRequest {
        uuid: "".to_string(),
        with_duplicates: search_request.with_duplicates,
        body: search_request.body.clone(),
        fields: search_request.fields.clone(),
        order: search_request.order.clone(),
        faceted: search_request.faceted.clone(),
        page_number: search_request.page_number,
        result_per_page: search_request.result_per_page,
        timestamps: search_request.timestamps,
        only_faceted: search_request.only_faceted,
        advanced_query: search_request.advanced_query.clone(),
        id: String::default(),
        filter: None,
        min_score: search_request.min_score_bm25,
        security: search_request.security.clone(),
        filtering_formula: search_query.clone(),
    })
}

fn compute_texts_request(
    search_request: &SearchRequest,
    query_analysis: &QueryAnalysis,
) -> Option<DocumentSearchRequest> {
    if !search_request.document {
        return None;
    }

    Some(DocumentSearchRequest {
        id: search_request.shard.clone(),
        body: search_request.body.clone(),
        fields: search_request.fields.clone(),
        order: search_request.order.clone(),
        faceted: search_request.faceted.clone(),
        page_number: search_request.page_number,
        result_per_page: search_request.result_per_page,
        timestamps: search_request.timestamps,
        only_faceted: search_request.only_faceted,
        advanced_query: search_request.advanced_query.clone(),
        with_status: search_request.with_status,
        filter: search_request.filter.clone(),
        min_score: search_request.min_score_bm25,
        label_filtering_formula: query_analysis.labels_prefilter_query.clone(),
    })
}

fn compute_vectors_request(
    search_request: &SearchRequest,
    query_analysis: &QueryAnalysis,
) -> Option<VectorSearchRequest> {
    if search_request.result_per_page == 0 || search_request.vector.is_empty() {
        return None;
    }

    Some(VectorSearchRequest {
        id: search_request.shard.clone(),
        vector_set: search_request.vectorset.clone(),
        vector: search_request.vector.clone(),
        page_number: search_request.page_number,
        result_per_page: search_request.result_per_page,
        with_duplicates: search_request.with_duplicates,
        min_score: search_request.min_score_semantic,
        field_labels: Vec::with_capacity(0),
        paragraph_labels: Vec::with_capacity(0),
        field_filters: search_request.fields.clone(),
        filtering_formula: query_analysis.search_query.clone(),
        segment_filtering_formula: query_analysis
            .labels_prefilter_query
            .as_ref()
            .and_then(|e| query_language::extract_label_filters(e, SEGMENT_TAGS)),
    })
}

fn compute_relations_request(search_request: &SearchRequest) -> Option<RelationSearchRequest> {
    if search_request.relation_prefix.is_none() && search_request.relation_subgraph.is_none() {
        return None;
    }

    #[allow(deprecated)]
    Some(RelationSearchRequest {
        shard_id: search_request.shard.clone(),
        prefix: search_request.relation_prefix.clone(),
        subgraph: search_request.relation_subgraph.clone(),
        reload: search_request.reload,
    })
}

#[cfg(test)]
mod tests {
    use nidx_protos::Filter;

    use super::*;
    #[test]
    fn proper_propagation() {
        let expression = serde_json::json!({
            "and": [
                { "literal": "this" },
                { "literal": "and" },
                { "literal": "that"},
            ],
        });
        let request = SearchRequest {
            filter: Some(Filter {
                field_labels: vec!["this".to_string()],
                paragraph_labels: vec!["and".to_string(), "that".to_string()],
                labels_expression: expression.to_string(),
                keywords_expression: "".to_string(),
            }),
            result_per_page: 10,
            vector: vec![1.0],
            paragraph: true,
            ..Default::default()
        };
        let query_plan = build_query_plan(request).unwrap();
        let Some(prefilter) = query_plan.prefilter else {
            panic!("There should be a prefilter");
        };
        let Some(formula) = prefilter.labels_formula else {
            panic!("The prefilter should have a formula");
        };
        let BooleanExpression::Literal(literal) = &formula else {
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
