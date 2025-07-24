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
use tantivy::schema::Facet;

#[derive(Clone, Default)]
pub struct ParagraphSearchRequest {
    pub id: String,
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
    pub filter_or: bool,
}

pub struct ParagraphSuggestRequest {
    pub body: String,
    pub filtering_formula: Option<BooleanExpression<String>>,
    /// Whether to do an OR/AND between prefilter results and filtering_formula
    pub filter_or: bool,
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
