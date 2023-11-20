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

use nucliadb_protos::prelude::Filter;

pub use crate::protos::prost_types::Timestamp as ProtoTimestamp;
use crate::protos::{
    DocumentSearchRequest, ParagraphSearchRequest, RelationSearchRequest, SearchRequest,
    VectorSearchRequest,
};

/// A field has two dates
#[derive(Debug, Clone, Copy)]
pub enum FieldDateType {
    /// When the field was created
    Created,
    /// When the field was modified
    Modified,
}

/// Used to define the time range of interest
#[derive(Debug, Clone)]
pub struct TimestampFilter {
    pub applies_to: FieldDateType,
    pub from: Option<ProtoTimestamp>,
    pub to: Option<ProtoTimestamp>,
}

/// A query plan pre-filtering stage.
/// It is useful for reducing the space of results
/// for the rest of the plan.
#[derive(Debug, Clone)]
pub struct PreFilterRequest {
    pub timestamp_filters: Vec<TimestampFilter>,
    pub labels_filters: Vec<String>,
}

/// Represents a field that has met all of the
/// pre-filtering requirements.
#[derive(Debug, Clone)]
pub struct ValidField {
    pub resource_id: String,
    pub field_id: String,
}

/// Utility type to identify and allow optimizations in filtering edge cases
#[derive(Debug, Default, Clone)]
pub enum ValidFieldCollector {
    #[default]
    None,
    All,
    Some(Vec<ValidField>),
}

/// Once a [`PreFilterRequest`] was successfully executed
/// this type can be used to modify the rest of the query plan.
#[derive(Debug, Default, Clone)]
pub struct PreFilterResponse {
    pub valid_fields: ValidFieldCollector,
}

/// The queries a [`QueryPlan`] has decided to send to each index.
#[derive(Debug, Default, Clone)]
pub struct IndexQueries {
    pub vectors_request: Option<VectorSearchRequest>,
    pub paragraphs_request: Option<ParagraphSearchRequest>,
    pub texts_request: Option<DocumentSearchRequest>,
    pub relations_request: Option<RelationSearchRequest>,
}

impl IndexQueries {
    fn apply_to_vectors(request: &mut VectorSearchRequest, response: &PreFilterResponse) {
        let ValidFieldCollector::Some(valid_fields) = &response.valid_fields else {
            return;
        };
        // Add vectors key filters
        for valid_field in valid_fields {
            let resource_id = &valid_field.resource_id;
            let field_id = &valid_field.field_id;
            let as_vectors_key = format!("{resource_id}{field_id}");
            request.key_filters.push(as_vectors_key);
        }
        // Clear labels to avoid duplicate filtering
        request.field_labels.clear();
    }

    fn apply_to_paragraphs(request: &mut ParagraphSearchRequest, response: &PreFilterResponse) {
        if matches!(response.valid_fields, ValidFieldCollector::All) {
            // Since all the fields are matching there is no need to use this filter.
            request.timestamps = None;
        }
        let ValidFieldCollector::Some(valid_fields) = &response.valid_fields else {
            return;
        };

        // Clear filter labels to avoid duplicate filtering
        let mut paragraph_labels = vec![];
        if let Some(filter) = request.filter.as_ref() {
            paragraph_labels = filter.paragraph_labels.clone();
        }
        let filter = Filter {
            field_labels: vec![],
            paragraph_labels,
        };
        request.filter.replace(filter);

        // Add key filters
        for valid_field in valid_fields {
            let resource_id = &valid_field.resource_id;
            let field_id = &valid_field.field_id;
            let unique_field_key = format!("{resource_id}{field_id}");
            request.key_filters.push(unique_field_key);
        }
    }

    /// When a pre-filter is run, the result can be used to modify the queries
    /// that the indexes must resolve.
    pub fn apply_pre_filter(&mut self, pre_filtered: PreFilterResponse) {
        if matches!(pre_filtered.valid_fields, ValidFieldCollector::None) {
            // There are no matches so there is no need to run the rest of the search
            self.vectors_request = None;
            self.paragraphs_request = None;
            self.texts_request = None;
            self.relations_request = None;
            return;
        }

        if let Some(vectors_request) = self.vectors_request.as_mut() {
            IndexQueries::apply_to_vectors(vectors_request, &pre_filtered);
        };
        if let Some(paragraph_request) = self.paragraphs_request.as_mut() {
            IndexQueries::apply_to_paragraphs(paragraph_request, &pre_filtered);
        };
    }
}

/// A shard reader will use this plan to produce search results as efficiently as
/// possible.
pub struct QueryPlan {
    pub pre_filter: Option<PreFilterRequest>,
    pub index_queries: IndexQueries,
}

/// A [`QueryPlan`] can be traced from a [`SearchRequest`]
impl From<SearchRequest> for QueryPlan {
    fn from(search_request: SearchRequest) -> Self {
        QueryPlan {
            pre_filter: compute_pre_filters(&search_request),
            index_queries: IndexQueries {
                vectors_request: compute_vectors_request(&search_request),
                paragraphs_request: compute_paragraphs_request(&search_request),
                texts_request: compute_texts_request(&search_request),
                relations_request: compute_relations_request(&search_request),
            },
        }
    }
}

fn compute_pre_filters(search_request: &SearchRequest) -> Option<PreFilterRequest> {
    let mut pre_filter_request = PreFilterRequest {
        timestamp_filters: vec![],
        labels_filters: vec![],
    };

    // Timestamp filters
    let request_has_timestamp_filters = search_request.timestamps.as_ref().is_some();
    if request_has_timestamp_filters {
        let timestamp_filters = compute_timestamp_pre_filters(search_request);
        pre_filter_request
            .timestamp_filters
            .extend(timestamp_filters);
    }

    // Labels filters
    let request_has_labels_filters = search_request
        .filter
        .as_ref()
        .map(|i| !i.field_labels.is_empty())
        .unwrap_or_default();

    if request_has_labels_filters {
        let labels = compute_labels_pre_filters(search_request);
        pre_filter_request.labels_filters.extend(labels);
    }

    if !request_has_timestamp_filters && !request_has_labels_filters {
        return None;
    }
    Some(pre_filter_request)
}

fn compute_timestamp_pre_filters(search_request: &SearchRequest) -> Vec<TimestampFilter> {
    let mut timestamp_pre_filters = vec![];
    let Some(request_timestamp_filters) = search_request.timestamps.as_ref() else {
        return timestamp_pre_filters;
    };

    let modified_filter = TimestampFilter {
        applies_to: FieldDateType::Modified,
        from: request_timestamp_filters.from_modified.clone(),
        to: request_timestamp_filters.to_modified.clone(),
    };
    timestamp_pre_filters.push(modified_filter);

    let created_filter = TimestampFilter {
        applies_to: FieldDateType::Created,
        from: request_timestamp_filters.from_created.clone(),
        to: request_timestamp_filters.to_created.clone(),
    };
    timestamp_pre_filters.push(created_filter);
    timestamp_pre_filters
}

fn compute_labels_pre_filters(search_request: &SearchRequest) -> Vec<String> {
    let mut labels_pre_filters = vec![];
    search_request
        .filter
        .iter()
        .flat_map(|f| f.field_labels.iter())
        .for_each(|tag| {
            labels_pre_filters.push(tag.clone());
        });
    labels_pre_filters
}

fn compute_paragraphs_request(search_request: &SearchRequest) -> Option<ParagraphSearchRequest> {
    if !search_request.paragraph {
        return None;
    }
    Some(ParagraphSearchRequest {
        uuid: "".to_string(),
        with_duplicates: search_request.with_duplicates,
        body: search_request.body.clone(),
        fields: search_request.fields.clone(),
        filter: search_request.filter.clone(),
        order: search_request.order.clone(),
        faceted: search_request.faceted.clone(),
        page_number: search_request.page_number,
        result_per_page: search_request.result_per_page,
        timestamps: search_request.timestamps.clone(),
        only_faceted: search_request.only_faceted,
        advanced_query: search_request.advanced_query.clone(),
        key_filters: search_request.key_filters.clone(),
        ..Default::default()
    })
}

fn compute_texts_request(search_request: &SearchRequest) -> Option<DocumentSearchRequest> {
    if !search_request.document {
        return None;
    }
    Some(DocumentSearchRequest {
        body: search_request.body.clone(),
        fields: search_request.fields.clone(),
        filter: search_request.filter.clone(),
        order: search_request.order.clone(),
        faceted: search_request.faceted.clone(),
        page_number: search_request.page_number,
        result_per_page: search_request.result_per_page,
        timestamps: search_request.timestamps.clone(),
        only_faceted: search_request.only_faceted,
        advanced_query: search_request.advanced_query.clone(),
        with_status: search_request.with_status,
        ..Default::default()
    })
}

fn compute_vectors_request(search_request: &SearchRequest) -> Option<VectorSearchRequest> {
    if search_request.result_per_page == 0 || search_request.vector.is_empty() {
        return None;
    }
    let field_label_filters = search_request
        .filter
        .iter()
        .flat_map(|f| f.field_labels.iter().cloned())
        .chain(search_request.fields.iter().cloned())
        .collect();
    let paragraph_label_filters = search_request
        .filter
        .iter()
        .flat_map(|f| f.paragraph_labels.iter().cloned())
        .collect();
    Some(VectorSearchRequest {
        vector_set: search_request.vectorset.clone(),
        vector: search_request.vector.clone(),
        page_number: search_request.page_number,
        result_per_page: search_request.result_per_page,
        with_duplicates: search_request.with_duplicates,
        key_filters: search_request.key_filters.clone(),
        field_labels: field_label_filters,
        paragraph_labels: paragraph_label_filters,
        min_score: search_request.min_score,
        ..Default::default()
    })
}

fn compute_relations_request(search_request: &SearchRequest) -> Option<RelationSearchRequest> {
    if search_request.relation_prefix.is_none() && search_request.relation_subgraph.is_none() {
        return None;
    }
    Some(RelationSearchRequest {
        shard_id: search_request.shard.clone(),
        prefix: search_request.relation_prefix.clone(),
        subgraph: search_request.relation_subgraph.clone(),
        ..Default::default()
    })
}
