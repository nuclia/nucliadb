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

use nidx_paragraph::{ParagraphSearcher, ParagraphSuggestRequest};
use nidx_protos::{FilterOperator, RelationPrefixSearchResponse, SuggestFeatures, SuggestRequest, SuggestResponse};
use nidx_relation::RelationSearcher;
use nidx_text::{TextSearcher, prefilter::PreFilterRequest};
use nidx_types::prefilter::PrefilterResult;
use tracing::{Span, instrument};

use crate::errors::{NidxError, NidxResult};

use super::{index_cache::IndexCache, query_planner::filter_to_boolean_expression};

/// Max number of words accepted as a suggest query. This is useful for
/// compounds with semantic meaning (like a name and a surname) but can add
/// irrelevant words to queries
const MAX_SUGGEST_COMPOUND_WORDS: usize = 3;

/// Suggest gives possible strings to autocomplete a partial query that's been
/// written. To do so, it searches keyword and relation indexes to find good
/// suggestions.
///
/// TODO: review implementation. Timestamps are not used and we are probably
/// filtering twice in the prefilter and paragraphs filter
#[instrument(skip_all, fields(shard_id = request.shard))]
pub async fn suggest(index_cache: Arc<IndexCache>, request: SuggestRequest) -> NidxResult<SuggestResponse> {
    let shard_id = uuid::Uuid::parse_str(&request.shard)?;

    let Some(indexes) = index_cache.get_shard_indexes(&shard_id).await else {
        return Err(NidxError::NotFound);
    };

    let Some(text_index) = indexes.text_index() else {
        return Err(NidxError::NotFound);
    };
    let text_searcher_arc = index_cache.get(&text_index).await?;

    let Some(relation_index) = indexes.relation_index() else {
        return Err(NidxError::NotFound);
    };
    let relation_searcher_arc = index_cache.get(&relation_index).await?;

    let Some(paragraph_index) = indexes.paragraph_index() else {
        return Err(NidxError::NotFound);
    };
    let paragraph_searcher_arc = index_cache.get(&paragraph_index).await?;

    let current = Span::current();
    let suggest_results = tokio::task::spawn_blocking(move || {
        current.in_scope(|| {
            blocking_suggest(
                request,
                text_searcher_arc.as_ref().into(),
                paragraph_searcher_arc.as_ref().into(),
                relation_searcher_arc.as_ref().into(),
            )
        })
    })
    .await??;

    Ok(suggest_results)
}

fn blocking_suggest(
    request: SuggestRequest,
    text_searcher: &TextSearcher,
    paragraph_searcher: &ParagraphSearcher,
    relation_searcher: &RelationSearcher,
) -> anyhow::Result<SuggestResponse> {
    let suggest_paragraphs = request.features.contains(&(SuggestFeatures::Paragraphs as i32));
    let suggest_entities = request.features.contains(&(SuggestFeatures::Entities as i32));

    let prefixes = split_suggest_query(&request.body, MAX_SUGGEST_COMPOUND_WORDS);

    let mut prefiltered = PrefilterResult::All;

    let mut paragraph_request = None;
    if suggest_paragraphs {
        if let Some(expr) = &request.field_filter {
            let prefilter = PreFilterRequest {
                security: None,
                filter_expression: Some(expr.clone()),
            };

            prefiltered = text_searcher.prefilter(&prefilter)?;
        }
        paragraph_request = Some(ParagraphSuggestRequest {
            body: request.body,
            filtering_formula: request
                .paragraph_filter
                .clone()
                .map(filter_to_boolean_expression)
                .transpose()?,
            filter_or: request.filter_operator == FilterOperator::Or as i32,
        })
    }

    if matches!(prefiltered, PrefilterResult::None) {
        paragraph_request = None;
    }

    let paragraph_task = paragraph_request.map(|req| move || paragraph_searcher.suggest(&req, &prefiltered));

    let relation_task = suggest_entities.then_some(move || relation_searcher.suggest(prefixes));

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

/// Given a query, return a list of derived queries using word(s) from the end
/// of the original query.
///
/// The longer query, i.e., the one with more words, will come first. That's the
/// one with more probability to get a meaningful suggestion.
///
/// `max_group` defines the limit of words a query can have.
fn split_suggest_query(query: &str, max_group: usize) -> Vec<String> {
    // Paying the price of allocating the vector to not have to
    // prepend to the partial strings.
    let relevant_words: Vec<_> = query.split(' ').rev().take(max_group).collect();
    let mut prefixes = vec![String::new(); max_group];
    for (index, word) in relevant_words.into_iter().rev().enumerate() {
        // The inner loop is upper-bounded by max_group
        for prefix in prefixes.iter_mut().take(index + 1) {
            if !prefix.is_empty() {
                prefix.push(' ');
            }
            prefix.push_str(word);
        }
    }
    prefixes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_suggest_split() {
        let query = "what are the best use cases for Apache Cassandra".to_string();

        let expected = vec!["for Apache Cassandra", "Apache Cassandra", "Cassandra"];
        let got = split_suggest_query(&query, 3);
        assert_eq!(expected, got);

        let expected = vec!["Apache Cassandra", "Cassandra"];
        let got = split_suggest_query(&query, 2);
        assert_eq!(expected, got);
    }
}
