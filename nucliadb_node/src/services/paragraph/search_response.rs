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
use std::collections::HashMap;

use nucliadb_protos::order_by::OrderType;
use nucliadb_protos::{
    FacetResult, FacetResults, OrderBy, ParagraphResult, ParagraphSearchResponse,
};
use tantivy::collector::FacetCounts;
use tantivy::schema::Value;
use tantivy::DocAddress;
use tracing::*;

use super::reader::ParagraphReaderService;

pub struct SearchResponse<'a> {
    pub text_service: &'a ParagraphReaderService,
    pub facets_count: FacetCounts,
    pub facets: Vec<String>,
    pub top_docs: Vec<(f32, DocAddress)>,
    pub order_by: Option<OrderBy>,
    pub page_number: i32,
    pub results_per_page: i32,
}

// TODO
// change in protos to f32

impl<'a> From<SearchResponse<'a>> for ParagraphSearchResponse {
    fn from(response: SearchResponse) -> Self {
        let total = response.top_docs.len();
        let mut results: Vec<ParagraphResult> = Vec::with_capacity(total);
        let searcher = response.text_service.reader.searcher();

        let default_split = Value::Str("".to_string());
        for (score, doc_address) in response.top_docs {
            info!("Score: {} - DocAddress: {:?}", score, doc_address);
            match searcher.doc(doc_address) {
                Ok(doc) => {
                    let schema = &response.text_service.schema;
                    let uuid = doc
                        .get_first(schema.uuid)
                        .expect("document doesn't appear to have uuid.")
                        .as_text()
                        .unwrap()
                        .to_string();

                    let field = doc
                        .get_first(schema.field)
                        .expect("document doesn't appear to have uuid.")
                        .as_facet()
                        .unwrap()
                        .to_path_string();

                    let start_pos = doc.get_first(schema.start_pos).unwrap().as_u64().unwrap();

                    let end_pos = doc.get_first(schema.end_pos).unwrap().as_u64().unwrap();

                    let paragraph = doc
                        .get_first(schema.paragraph)
                        .expect("document doesn't appear to have end_pos.")
                        .as_text()
                        .unwrap()
                        .to_string();

                    let split = doc
                        .get_first(schema.split)
                        .unwrap_or(&default_split)
                        .as_text()
                        .unwrap()
                        .to_string();

                    let index = doc.get_first(schema.index).unwrap().as_u64().unwrap();

                    let result = ParagraphResult {
                        uuid,
                        score,
                        field,
                        start: start_pos,
                        end: end_pos,
                        paragraph,
                        split,
                        index,
                    };

                    results.push(result);
                }
                Err(e) => error!("Error retrieving document from index: {}", e),
            }
        }

        if let Some(order_by) = response.order_by {
            if order_by.r#type == OrderType::Asc as i32 {
                results.reverse();
            }
        }

        let mut facet_map = HashMap::new();

        for facet in response.facets {
            let count: Vec<_> = response
                .facets_count
                .top_k(facet.as_str(), 50)
                .iter()
                .map(|(facet, count)| FacetResult {
                    tag: facet.to_string(),
                    total: *count as i32,
                })
                .collect();

            facet_map.insert(
                facet,
                FacetResults {
                    facetresults: count,
                },
            );
        }
        ParagraphSearchResponse {
            total: total as i32,
            results,
            facets: facet_map,
            page_number: response.page_number,
            result_per_page: response.results_per_page,
        }
    }
}
