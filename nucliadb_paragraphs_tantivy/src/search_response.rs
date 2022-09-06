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

use nucliadb_protos::{FacetResult, FacetResults, ParagraphResult, ParagraphSearchResponse};
use nucliadb_service_interface::prelude::*;
use tantivy::collector::FacetCounts;
use tantivy::schema::Value;
use tantivy::DocAddress;
use tracing::*;

use crate::reader::ParagraphReaderService;
use crate::search_query::TermCollector;

fn facet_count(facet: &str, facets_count: &FacetCounts) -> Vec<FacetResult> {
    facets_count
        .top_k(facet, 50)
        .into_iter()
        .map(|(facet, count)| FacetResult {
            tag: facet.to_string(),
            total: count as i32,
        })
        .collect()
}
fn produce_facets(facets: Vec<String>, facets_count: FacetCounts) -> HashMap<String, FacetResults> {
    facets
        .into_iter()
        .map(|facet| (&facets_count, facet))
        .map(|(facets_count, facet)| (facet_count(&facet, facets_count), facet))
        .filter(|(r, _)| !r.is_empty())
        .map(|(facetresults, facet)| (facet, FacetResults { facetresults }))
        .collect()
}

pub struct SearchBm25Response<'a> {
    pub text_service: &'a ParagraphReaderService,
    pub query: &'a str,
    pub facets_count: Option<FacetCounts>,
    pub facets: Vec<String>,
    pub top_docs: Vec<(f32, DocAddress)>,
    pub page_number: i32,
    pub results_per_page: i32,
    pub termc: TermCollector,
}

pub struct SearchIntResponse<'a> {
    pub text_service: &'a ParagraphReaderService,
    pub query: &'a str,
    pub facets_count: Option<FacetCounts>,
    pub facets: Vec<String>,
    pub top_docs: Vec<(u64, DocAddress)>,
    pub page_number: i32,
    pub results_per_page: i32,
    pub termc: TermCollector,
}

pub struct SearchFacetsResponse<'a> {
    pub text_service: &'a ParagraphReaderService,
    pub facets_count: Option<FacetCounts>,
    pub facets: Vec<String>,
}

impl<'a> From<SearchFacetsResponse<'a>> for ParagraphSearchResponse {
    fn from(response: SearchFacetsResponse) -> Self {
        let facets = response
            .facets_count
            .map(|count| produce_facets(response.facets, count))
            .unwrap_or_default();
        let results: Vec<ParagraphResult> = Vec::with_capacity(0);
        ParagraphSearchResponse {
            results,
            facets,
            ..Default::default()
        }
    }
}

impl<'a> From<SearchIntResponse<'a>> for ParagraphSearchResponse {
    fn from(response: SearchIntResponse) -> Self {
        let mut total = response.top_docs.len();

        let next_page: bool;
        if total > response.results_per_page as usize {
            next_page = true;
            total = response.results_per_page as usize;
        } else {
            next_page = false;
        }

        let mut results: Vec<ParagraphResult> = Vec::with_capacity(total);
        let searcher = response.text_service.reader.searcher();
        let default_split = Value::Str("".to_string());
        for (score, doc_address) in response.top_docs.into_iter().take(total) {
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
                        // This can be used instead of get_first() considering there is only one
                        // field. Done because of a bug in the writing
                        // process [sc 1604].
                        .get_all(schema.field)
                        .last()
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
                    let mut terms: Vec<_> = response
                        .termc
                        .get_fterms(doc_address.doc_id)
                        .into_iter()
                        .collect();
                    terms.sort();
                    let result = ParagraphResult {
                        uuid,
                        score,
                        field,
                        start: start_pos,
                        end: end_pos,
                        paragraph,
                        split,
                        index,
                        matches: terms,
                        ..Default::default()
                    };

                    results.push(result);
                }
                Err(e) => error!("Error retrieving document from index: {}", e),
            }
        }

        let facets = response
            .facets_count
            .map(|count| produce_facets(response.facets, count))
            .unwrap_or_default();
        ParagraphSearchResponse {
            results,
            facets,
            total: total as i32,
            page_number: response.page_number,
            result_per_page: response.results_per_page,
            query: response.query.to_string(),
            next_page,
            bm25: false,
            ematches: response.termc.eterms.into_iter().collect(),
        }
    }
}

impl<'a> From<SearchBm25Response<'a>> for ParagraphSearchResponse {
    fn from(response: SearchBm25Response) -> Self {
        let mut total = response.top_docs.len();
        let next_page: bool;
        if total > response.results_per_page as usize {
            next_page = true;
            total = response.results_per_page as usize;
        } else {
            next_page = false;
        }

        let mut results: Vec<ParagraphResult> = Vec::with_capacity(total);
        let searcher = response.text_service.reader.searcher();

        let default_split = Value::Str("".to_string());
        for (score, doc_address) in response.top_docs.into_iter().take(total) {
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
                        // This can be used instead of get_first() considering there is only one
                        // field. Done because of a bug in the writing
                        // process [sc 1604].
                        .get_all(schema.field)
                        .last()
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
                    let mut terms: Vec<_> = response
                        .termc
                        .get_fterms(doc_address.doc_id)
                        .into_iter()
                        .collect();
                    terms.sort();
                    let result = ParagraphResult {
                        uuid,
                        score_bm25: score,
                        field,
                        start: start_pos,
                        end: end_pos,
                        paragraph,
                        split,
                        index,
                        matches: terms,
                        ..Default::default()
                    };

                    results.push(result);
                }
                Err(e) => error!("Error retrieving document from index: {}", e),
            }
        }

        let facets = response
            .facets_count
            .map(|count| produce_facets(response.facets, count))
            .unwrap_or_default();
        ParagraphSearchResponse {
            results,
            facets,
            total: total as i32,
            page_number: response.page_number,
            result_per_page: response.results_per_page,
            query: response.query.to_string(),
            next_page,
            bm25: true,
            ematches: response.termc.eterms.into_iter().collect(),
        }
    }
}
