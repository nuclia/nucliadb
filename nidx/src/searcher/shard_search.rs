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

use std::sync::Arc;

use nidx_paragraph::ParagraphSearcher;
use nidx_protos::{GraphSearchRequest, GraphSearchResponse, SearchRequest, SearchResponse};
use nidx_relation::RelationSearcher;
use nidx_text::{TextSearcher, prefilter::PreFilterRequest};
use nidx_types::prefilter::PrefilterResult;
use nidx_vector::VectorSearcher;
use tracing::{Span, instrument};

use crate::errors::{NidxError, NidxResult};

use super::{
    index_cache::IndexCache,
    query_planner::{self, QueryPlan},
};

#[instrument(skip_all, fields(shard_id = search_request.shard))]
pub async fn search(index_cache: Arc<IndexCache>, search_request: SearchRequest) -> NidxResult<SearchResponse> {
    let shard_id = uuid::Uuid::parse_str(&search_request.shard)?;

    let query_plan = query_planner::build_query_plan(search_request.clone())?;

    let Some(indexes) = index_cache.get_shard_indexes(&shard_id).await else {
        return Err(NidxError::NotFound);
    };

    let paragraph_search = if query_plan.index_queries.paragraphs_request.is_some() {
        let Some(paragraph_index) = indexes.paragraph_index() else {
            return Err(NidxError::NotFound);
        };
        Some(index_cache.get(&paragraph_index).await?)
    } else {
        None
    };

    let relation_search = if query_plan.index_queries.relations_request.is_some() {
        let Some(relation_index) = indexes.relation_index() else {
            return Err(NidxError::NotFound);
        };
        Some(index_cache.get(&relation_index).await?)
    } else {
        None
    };

    let text_search = if query_plan.prefilter.is_some() || query_plan.index_queries.texts_request.is_some() {
        let Some(text_index) = indexes.text_index() else {
            return Err(NidxError::NotFound);
        };
        Some(index_cache.get(&text_index).await?)
    } else {
        None
    };

    // Do not require the vectorset parameter if it's not going to be used
    let vector_seach = if query_plan.index_queries.vectors_request.is_some() {
        if search_request.vectorset.is_empty() {
            return Err(NidxError::invalid("Vectorset is required"));
        }
        let Some(vector_index) = indexes.vector_index(&search_request.vectorset) else {
            return Err(NidxError::NotFound);
        };
        Some(index_cache.get(&vector_index).await?)
    } else {
        None
    };

    let current = Span::current();
    let search_results = tokio::task::spawn_blocking(move || {
        current.in_scope(|| {
            blocking_search(
                query_plan,
                paragraph_search.as_ref().map(|v| v.as_ref().into()),
                relation_search.as_ref().map(|v| v.as_ref().into()),
                text_search.as_ref().map(|v| v.as_ref().into()),
                vector_seach.as_ref().map(|v| v.as_ref().into()),
            )
        })
    })
    .await??;
    Ok(search_results)
}

fn blocking_search(
    query_plan: QueryPlan,
    paragraph_searcher: Option<&ParagraphSearcher>,
    relation_searcher: Option<&RelationSearcher>,
    text_searcher: Option<&TextSearcher>,
    vector_searcher: Option<&VectorSearcher>,
) -> anyhow::Result<SearchResponse> {
    let search_id = uuid::Uuid::new_v4().to_string();
    let mut index_queries = query_plan.index_queries;

    // Apply pre-filtering to the query plan
    if let Some(prefilter) = query_plan.prefilter {
        let prefiltered = text_searcher.unwrap().prefilter(&prefilter)?;
        index_queries.apply_prefilter(prefiltered);
    }

    // Run the rest of the plan
    let text_task = index_queries.texts_request.map(|mut request| {
        request.id = search_id.clone();
        move || text_searcher.unwrap().search(&request)
    });

    let prefilter = &index_queries.prefilter_results;
    let paragraph_task = index_queries.paragraphs_request.map(|mut request| {
        request.id = search_id.clone();
        move || paragraph_searcher.unwrap().search(&request, prefilter)
    });

    let relation_task = index_queries
        .relations_request
        .map(|request| move || relation_searcher.unwrap().graph_search(&request, prefilter));

    let vector_task = index_queries.vectors_request.map(|mut request| {
        request.id = search_id.clone();
        move || vector_searcher.unwrap().search(&request, prefilter)
    });

    let mut rtext = None;
    let mut rparagraph = None;
    let mut rvector = None;
    let mut rrelation = None;

    std::thread::scope(|scope| {
        if let Some(task) = paragraph_task {
            let current = Span::current();
            let rparagraph = &mut rparagraph;
            scope.spawn(move || *rparagraph = Some(current.in_scope(task)));
        }

        if let Some(task) = relation_task {
            let current = Span::current();
            let rrelation = &mut rrelation;
            scope.spawn(move || *rrelation = Some(current.in_scope(task)));
        }

        if let Some(task) = text_task {
            let current = Span::current();
            let rtext = &mut rtext;
            scope.spawn(move || *rtext = Some(current.in_scope(task)));
        }

        if let Some(task) = vector_task {
            let current = Span::current();
            let rvector = &mut rvector;
            scope.spawn(move || *rvector = Some(current.in_scope(task)));
        }
    });

    Ok(SearchResponse {
        document: rtext.transpose()?,
        paragraph: rparagraph.transpose()?,
        vector: rvector.transpose()?,
        graph: rrelation.transpose()?,
    })
}

#[instrument(skip_all, fields(shard_id = graph_request.shard))]
pub async fn graph_search(
    index_cache: Arc<IndexCache>,
    graph_request: GraphSearchRequest,
) -> NidxResult<GraphSearchResponse> {
    let shard_id = uuid::Uuid::parse_str(&graph_request.shard)?;

    let Some(indexes) = index_cache.get_shard_indexes(&shard_id).await else {
        return Err(NidxError::NotFound);
    };

    let Some(relation_index_id) = indexes.relation_index() else {
        return Err(NidxError::NotFound);
    };

    // If we got prefilter params, apply prefilter
    let prefilter = if graph_request.security.is_some() || graph_request.field_filter.is_some() {
        let prefilter_request = PreFilterRequest {
            security: graph_request.security.clone(),
            filter_expression: graph_request.field_filter.clone(),
        };
        let Some(text_index_id) = indexes.text_index() else {
            return Err(NidxError::NotFound);
        };
        let text_searcher = index_cache.get(&text_index_id).await?;
        let current = Span::current();
        tokio::task::spawn_blocking(move || {
            current.in_scope(|| {
                let searcher: &TextSearcher = text_searcher.as_ref().into();
                searcher.prefilter(&prefilter_request)
            })
        })
        .await??
    } else {
        PrefilterResult::All
    };

    if matches!(prefilter, PrefilterResult::None) {
        return Ok(GraphSearchResponse::default());
    }

    let relation_searcher = index_cache.get(&relation_index_id).await?;
    let current = Span::current();
    let results = tokio::task::spawn_blocking(move || {
        current.in_scope(|| {
            let searcher: &RelationSearcher = relation_searcher.as_ref().into();
            searcher.graph_search(&graph_request, &prefilter)
        })
    })
    .await??;
    Ok(results)
}
