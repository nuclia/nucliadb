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
use nidx_protos::{SearchRequest, SearchResponse};
use nidx_relation::RelationSearcher;
use nidx_text::TextSearcher;
use nidx_vector::VectorSearcher;

use crate::{
    errors::{NidxError, NidxResult},
    metadata::{Index, IndexKind},
    NidxMetadata,
};

use super::{index_cache::IndexCache, query_planner};

pub async fn search(
    meta: &NidxMetadata,
    index_cache: Arc<IndexCache>,
    search_request: SearchRequest,
) -> NidxResult<SearchResponse> {
    let shard_id = uuid::Uuid::parse_str(&search_request.shard)?;
    if search_request.vectorset.is_empty() {
        return Err(NidxError::invalid("Vectorset is required"));
    }

    // TODO: Avoid querying here, the information can be take from synced metadata
    let paragraph_index = Index::find(&meta.pool, shard_id, IndexKind::Paragraph, "paragraph").await?;
    let paragraph_searcher_arc = index_cache.get(&paragraph_index.id).await?;

    let relation_index = Index::find(&meta.pool, shard_id, IndexKind::Relation, "relation").await?;
    let relation_searcher_arc = index_cache.get(&relation_index.id).await?;

    let text_index = Index::find(&meta.pool, shard_id, IndexKind::Text, "text").await?;
    let text_searcher_arc = index_cache.get(&text_index.id).await?;

    let vector_index = Index::find(&meta.pool, shard_id, IndexKind::Vector, &search_request.vectorset).await?;
    let vector_seacher_arc = index_cache.get(&vector_index.id).await?;

    let search_results = tokio::task::spawn_blocking(move || {
        blocking_search(
            search_request,
            paragraph_searcher_arc.as_ref().into(),
            relation_searcher_arc.as_ref().into(),
            text_searcher_arc.as_ref().into(),
            vector_seacher_arc.as_ref().into(),
        )
    })
    .await??;
    Ok(search_results)
}

fn blocking_search(
    search_request: SearchRequest,
    paragraph_searcher: &ParagraphSearcher,
    relation_searcher: &RelationSearcher,
    text_searcher: &TextSearcher,
    vector_searcher: &VectorSearcher,
) -> anyhow::Result<SearchResponse> {
    let query_plan = query_planner::build_query_plan(search_request)?;
    let search_id = uuid::Uuid::new_v4().to_string();
    let mut index_queries = query_plan.index_queries;

    // Apply pre-filtering to the query plan
    if let Some(prefilter) = query_plan.prefilter {
        let prefiltered = text_searcher.prefilter(&prefilter)?;
        index_queries.apply_prefilter(prefiltered);
    }

    // Run the rest of the plan
    let text_task = index_queries.texts_request.map(|mut request| {
        request.id = search_id.clone();
        move || text_searcher.search(&request, &index_queries.texts_context)
    });

    let paragraph_task = index_queries.paragraphs_request.map(|mut request| {
        request.id = search_id.clone();
        move || paragraph_searcher.search(&request, &index_queries.paragraphs_context)
    });

    let relation_task = index_queries.relations_request.map(|request| move || relation_searcher.search(&request));

    let vector_task = index_queries.vectors_request.map(|mut request| {
        request.id = search_id.clone();
        move || vector_searcher.search(&request, &index_queries.vectors_context)
    });

    let mut rtext = None;
    let mut rparagraph = None;
    let mut rvector = None;
    let mut rrelation = None;

    std::thread::scope(|scope| {
        if let Some(task) = paragraph_task {
            let rparagraph = &mut rparagraph;
            scope.spawn(move || *rparagraph = Some(task()));
        }

        if let Some(task) = relation_task {
            let rrelation = &mut rrelation;
            scope.spawn(move || *rrelation = Some(task()));
        }

        if let Some(task) = text_task {
            let rtext = &mut rtext;
            scope.spawn(move || *rtext = Some(task()));
        }

        if let Some(task) = vector_task {
            let rvector = &mut rvector;
            scope.spawn(move || *rvector = Some(task()));
        }
    });

    Ok(SearchResponse {
        document: rtext.transpose()?,
        paragraph: rparagraph.transpose()?,
        vector: rvector.transpose()?,
        relation: rrelation.transpose()?,
    })
}
