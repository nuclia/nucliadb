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

#[derive(Clone, Default)]
pub struct VectorSearchRequest {
    /// Embedded vector search.
    pub vector: Vec<f32>,
    /// How many results are in this page.
    pub result_per_page: i32,
    pub with_duplicates: bool,
    /// ID for the vector set.
    /// Empty for searching on the original index
    pub vector_set: String,
    pub min_score: f32,

    pub filtering_formula: Option<BooleanExpression<String>>,
    pub segment_filtering_formula: Option<BooleanExpression<String>>,
    /// Whether to do an OR/AND between prefilter results and filtering_formula
    pub filter_operator: FilterOperator,
}
