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

use nucliadb_core::protos::{
    DocumentSearchRequest, ParagraphSearchRequest, RelationSearchRequest, SearchRequest,
    VectorSearchRequest,
};
use nucliadb_core::search::{DateField, PreFilter, ProtoTimestamp, TimestampFilter};

#[derive(Debug, Default, Clone)]
pub struct QueryPlan {
    pub pre_filter: Option<PreFilter>,
    pub vectors_request: Option<VectorSearchRequest>,
    pub paragraphs_request: Option<ParagraphSearchRequest>,
    pub texts_request: Option<DocumentSearchRequest>,
    pub relations_request: Option<RelationSearchRequest>,
}

fn compute_pre_filters(_: &SearchRequest) -> Option<PreFilter> {
    None
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

impl From<SearchRequest> for QueryPlan {
    fn from(search_request: SearchRequest) -> Self {
        QueryPlan {
            pre_filter: compute_pre_filters(&search_request),
            vectors_request: compute_vectors_request(&search_request),
            paragraphs_request: compute_paragraphs_request(&search_request),
            texts_request: compute_texts_request(&search_request),
            relations_request: compute_relations_request(&search_request),
        }
    }
}
