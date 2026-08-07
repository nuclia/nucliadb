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

use nidx_paragraph::ParagraphSuggestRequest;
use nidx_protos::{SuggestFeatures, SuggestRequest};

use crate::errors::NidxResult;
use crate::searcher::query_planner::prefilter::Prefilter;
use crate::searcher::query_planner::{filter_to_boolean_expression, proto_filter_operator};

/// Max number of words accepted as a suggest query. This is useful for
/// compounds with semantic meaning (like a name and a surname) but can add
/// irrelevant words to queries
const MAX_SUGGEST_COMPOUND_WORDS: usize = 3;

pub struct SuggestPlan {
    pub prefilter: Option<Prefilter>,
    pub paragraphs: Option<ParagraphSuggestRequest>,
    pub relations: Option<Vec<String>>,
}

impl SuggestPlan {
    pub fn build(request: SuggestRequest) -> NidxResult<Option<Self>> {
        if request.top_k == 0 {
            // nothing requested
            return Ok(None);
        }

        let suggest_paragraphs = request.features.contains(&(SuggestFeatures::Paragraphs as i32));
        let suggest_entities = request.features.contains(&(SuggestFeatures::Entities as i32));
        if !suggest_paragraphs && !suggest_entities {
            // all features disabled, we won't search
            return Ok(None);
        }

        let prefilter = Prefilter::parse_suggest(&request)?;

        let relations = if suggest_entities {
            let prefixes = split_suggest_query(&request.body, MAX_SUGGEST_COMPOUND_WORDS);
            Some(prefixes)
        } else {
            None
        };

        let paragraphs = if suggest_paragraphs {
            Some(ParagraphSuggestRequest {
                body: request.body,
                top_k: request.top_k,
                filtering_formula: request
                    .paragraph_filter
                    .clone()
                    .map(filter_to_boolean_expression)
                    .transpose()?,
                filter_operator: proto_filter_operator(request.filter_operator)?,
            })
        } else {
            None
        };

        Ok(Some(Self {
            prefilter,
            paragraphs,
            relations,
        }))
    }
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
