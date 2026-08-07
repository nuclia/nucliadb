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

use std::sync::Arc;

use nidx_json::JsonSearcher;
use nidx_paragraph::ParagraphSearcher;
use nidx_protos::{RelationPrefixSearchResponse, SuggestRequest, SuggestResponse};
use nidx_relation::RelationSearcher;
use nidx_text::TextSearcher;
use nidx_types::prefilter::PrefilterResult;
use tracing::{Span, instrument};
use uuid::Uuid;

use crate::errors::{NidxError, NidxResult};
use crate::searcher::index_cache::IndexCache;
use crate::searcher::plan::suggest::SuggestPlan;
use crate::searcher::shards_query::shards_query;

/// Suggest gives possible strings to autocomplete a partial query that's been
/// written. To do so, it searches keyword and relation indexes to find good
/// suggestions.
///
/// TODO: review implementation. Timestamps are not used and we are probably
/// filtering twice in the prefilter and paragraphs filter
pub async fn suggest(
    index_cache: Arc<IndexCache>,
    request: SuggestRequest,
    shards: Vec<Uuid>,
) -> NidxResult<Vec<SuggestResponse>> {
    shards_query(index_cache, shards, request, shard_suggest).await
}

#[instrument(skip_all, fields(shard_id = shard_id.to_string()))]
pub async fn shard_suggest(
    shard_id: Uuid,
    index_cache: Arc<IndexCache>,
    request: SuggestRequest,
) -> NidxResult<SuggestResponse> {
    let Some(indexes) = index_cache.get_shard_indexes(&shard_id).await else {
        return Err(NidxError::NotFound);
    };

    let Some(text_index) = indexes.text_index() else {
        return Err(NidxError::NotFound);
    };
    let text_searcher_arc = index_cache.get(&text_index).await?;

    let Some(json_index) = indexes.json_index() else {
        return Err(NidxError::NotFound);
    };
    let json_searcher_arc = index_cache.get(&json_index).await?;

    let Some(relation_index) = indexes.relation_index() else {
        return Err(NidxError::NotFound);
    };
    let relation_searcher_arc = index_cache.get(&relation_index).await?;

    let Some(paragraph_index) = indexes.paragraph_index() else {
        return Err(NidxError::NotFound);
    };
    let paragraph_searcher_arc = index_cache.get(&paragraph_index).await?;

    let current = Span::current();
    let mut suggest_results = tokio::task::spawn_blocking(move || {
        current.in_scope(|| {
            blocking_suggest(
                request,
                text_searcher_arc.as_ref().into(),
                json_searcher_arc.as_ref().into(),
                paragraph_searcher_arc.as_ref().into(),
                relation_searcher_arc.as_ref().into(),
            )
        })
    })
    .await??;
    suggest_results.shard_ids.push(shard_id.to_string());

    Ok(suggest_results)
}

fn blocking_suggest(
    request: SuggestRequest,
    text_searcher: &TextSearcher,
    json_searcher: &JsonSearcher,
    paragraph_searcher: &ParagraphSearcher,
    relation_searcher: &RelationSearcher,
) -> anyhow::Result<SuggestResponse> {
    let top_k = request.top_k;
    let Some(query_plan) = SuggestPlan::build(request)? else {
        // nothing to search, we can return
        return Ok(SuggestResponse::default());
    };

    let prefilter = if let Some(prefilter) = query_plan.prefilter {
        prefilter.run(Some(text_searcher), Some(json_searcher))?
    } else {
        PrefilterResult::All
    };
    if matches!(prefilter, PrefilterResult::None) {
        // Nothing matches the prefilter, searching won't yield any result
        return Ok(SuggestResponse::default());
    }

    let paragraph_request = query_plan.paragraphs;
    let relation_request = query_plan.relations;

    let paragraph_task = {
        let prefilter = prefilter.clone();
        paragraph_request.map(|request| move || paragraph_searcher.suggest(&request, &prefilter))
    };
    let relation_task = relation_request.map(|prefixes| move || relation_searcher.suggest(prefixes, &prefilter, top_k));

    let mut rparagraph = None;
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
    });

    // Build suggest response from paragraph and relation results

    let mut response = SuggestResponse::default();

    if let Some(paragraph_response) = rparagraph {
        let paragraph_response = paragraph_response?;
        response.query = paragraph_response.query;
        response.total = paragraph_response.total;
        response.results = paragraph_response.results;
        response.ematches = paragraph_response.ematches;
    }

    if let Some(entities) = rrelation {
        let entities = entities?;
        response.entity_results = Some(RelationPrefixSearchResponse { nodes: entities });
    }

    Ok(response)
}
