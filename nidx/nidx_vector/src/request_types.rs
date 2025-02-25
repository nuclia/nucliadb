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

use nidx_types::query_language::BooleanExpression;

#[derive(Clone, Default)]
pub struct VectorSearchRequest {
    /// Shard ID
    pub id: String,
    /// Embedded vector search.
    pub vector: Vec<f32>,
    /// What page is the answer.
    pub page_number: i32,
    /// How many results are in this page.
    pub result_per_page: i32,
    pub with_duplicates: bool,
    /// ID for the vector set.
    /// Empty for searching on the original index
    pub vector_set: String,
    pub min_score: f32,

    pub filtering_formula: Option<BooleanExpression>,
    pub segment_filtering_formula: Option<BooleanExpression>,
    /// Whether to do an OR/AND between prefilter results and filtering_formula
    pub filter_or: bool,
}
