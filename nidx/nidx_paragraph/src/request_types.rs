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

use nidx_types::prefilter::FilterOperator;
use nidx_types::query_language::BooleanExpression;
use tantivy::schema::Facet;

#[derive(Clone)]
pub enum SearchAfterTieBreak {
    Drop,
    KeepAfter(u64),
    Keep,
}

#[derive(Clone)]
pub struct SearchAfter {
    pub score: f32,
    pub tie_break: SearchAfterTieBreak,
}

#[derive(Clone, Default)]
pub struct ParagraphSearchRequest {
    pub uuid: String,
    pub body: String,
    pub order: Option<nidx_protos::OrderBy>,
    pub faceted: Option<nidx_protos::Faceted>,
    pub result_per_page: i32,
    pub with_duplicates: bool,
    pub only_faceted: bool,
    pub advanced_query: Option<String>,
    pub min_score: f32,
    pub security: Option<nidx_protos::utils::Security>,

    pub filtering_formula: Option<BooleanExpression<String>>,
    /// Whether to do an OR/AND between prefilter results and filtering_formula
    pub filter_operator: FilterOperator,

    pub search_after: Option<SearchAfter>,
}

pub struct ParagraphSuggestRequest {
    pub body: String,
    pub top_k: u32,
    pub filtering_formula: Option<BooleanExpression<String>>,
    /// Whether to do an OR/AND between prefilter results and filtering_formula
    pub filter_operator: FilterOperator,
}

impl ParagraphSearchRequest {
    // Helper method to extract valid facets from the request
    pub fn facets(&self) -> Vec<Facet> {
        self.faceted
            .as_ref()
            .iter()
            .flat_map(|faceted| faceted.labels.iter())
            .filter_map(|facet| Facet::from_text(facet).ok())
            .collect()
    }
}
