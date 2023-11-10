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
}

/// Represents a field that has met all of the
/// pre-filtering requirements.
#[derive(Debug, Clone)]
pub struct ValidField {
    pub resource_id: String,
    pub field_id: String,
}

/// Once a [`PreFilterRequest`] was successfully executed
/// this type can be used to modify the rest of the query plan.
#[derive(Debug, Default, Clone)]
pub struct PreFilterResponse {
    pub valid_fields: Vec<ValidField>,
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
    fn apply_to_vectors(request: &mut VectorSearchRequest, pre_filtered: &PreFilterResponse) {
        for valid_field in pre_filtered.valid_fields.iter() {
            let resource_id = &valid_field.resource_id;
            let field_id = &valid_field.field_id;
            let as_vectors_key = format!("{resource_id}{field_id}");
            request.key_filters.push(as_vectors_key);
        }
    }

    /// When a pre-filter is run, the result can be used to modify the queries
    /// that the indexes must resolve.
    pub fn apply_pre_filter(&mut self, pre_filtered: PreFilterResponse) {
        // TODO:
        //  - Apply to relations?
        if let Some(vectors_request) = self.vectors_request.as_mut() {
            IndexQueries::apply_to_vectors(vectors_request, &pre_filtered);
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
    let Some(timestamp_filters) = search_request.timestamps.as_ref() else {
        return None;
    };
    let mut pre_filter_request = PreFilterRequest {
        timestamp_filters: vec![],
    };
    let modified_filter = TimestampFilter {
        applies_to: FieldDateType::Modified,
        from: timestamp_filters.from_modified.clone(),
        to: timestamp_filters.to_modified.clone(),
    };
    let created_filter = TimestampFilter {
        applies_to: FieldDateType::Created,
        from: timestamp_filters.from_created.clone(),
        to: timestamp_filters.to_created.clone(),
    };
    pre_filter_request.timestamp_filters.push(modified_filter);
    pre_filter_request.timestamp_filters.push(created_filter);
    Some(pre_filter_request)
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
    let tag_filters = search_request
        .filter
        .iter()
        .flat_map(|f| f.tags.iter().cloned())
        .chain(search_request.fields.iter().cloned())
        .collect();
    Some(VectorSearchRequest {
        vector_set: search_request.vectorset.clone(),
        vector: search_request.vector.clone(),
        page_number: search_request.page_number,
        result_per_page: search_request.result_per_page,
        with_duplicates: search_request.with_duplicates,
        key_filters: search_request.key_filters.clone(),
        tags: tag_filters,
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
