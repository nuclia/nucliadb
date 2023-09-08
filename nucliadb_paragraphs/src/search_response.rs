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

use itertools::Itertools;
use nucliadb_core::protos::{
    FacetResult, FacetResults, ParagraphResult, ParagraphSearchResponse, ResultScore,
};
use nucliadb_core::tracing::*;
use tantivy::collector::FacetCounts;
use tantivy::schema::Value;
use tantivy::DocAddress;

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
pub fn produce_facets(
    facets: Vec<String>,
    facets_count: FacetCounts,
) -> HashMap<String, FacetResults> {
    facets
        .into_iter()
        .map(|facet| (&facets_count, facet))
        .map(|(facets_count, facet)| (facet_count(&facet, facets_count), facet))
        .filter(|(r, _)| !r.is_empty())
        .map(|(facetresults, facet)| (facet, FacetResults { facetresults }))
        .collect()
}

pub struct SearchBm25Response<'a> {
    pub total: usize,
    pub text_service: &'a ParagraphReaderService,
    pub query: &'a str,
    pub facets_count: Option<FacetCounts>,
    pub facets: Vec<String>,
    pub top_docs: Vec<(f32, DocAddress)>,
    pub page_number: i32,
    pub results_per_page: i32,
    pub termc: TermCollector,
    pub searcher: tantivy::LeasedItem<tantivy::Searcher>,
}

pub struct SearchIntResponse<'a> {
    pub total: usize,
    pub text_service: &'a ParagraphReaderService,
    pub query: &'a str,
    pub facets_count: Option<FacetCounts>,
    pub facets: Vec<String>,
    pub top_docs: Vec<(u64, DocAddress)>,
    pub page_number: i32,
    pub results_per_page: i32,
    pub termc: TermCollector,
    pub searcher: tantivy::LeasedItem<tantivy::Searcher>,
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
        let total = response.total as i32;
        let obtained = response.top_docs.len();
        let requested = response.results_per_page as usize;
        let next_page = obtained > requested;
        let no_results = std::cmp::min(obtained, requested);
        let mut results: Vec<ParagraphResult> = Vec::with_capacity(no_results);
        let searcher = response.searcher;
        let default_split = Value::Str("".to_string());
        for (_, doc_address) in response.top_docs.into_iter().take(no_results) {
            match searcher.doc(doc_address) {
                Ok(doc) => {
                    let score = ResultScore {
                        bm25: 0.0,
                        booster: 0.0,
                    };
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

                    let labels = doc
                        .get_all(schema.facets)
                        .flat_map(|x| x.as_facet())
                        .map(|x| x.to_path_string())
                        .filter(|x| x.starts_with("/l/"))
                        .collect_vec();

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
                        field,
                        labels,
                        start: start_pos,
                        end: end_pos,
                        paragraph,
                        split,
                        index,
                        matches: terms,
                        score: Some(score),
                        metadata: schema.metadata(&doc),
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
            total,
            fuzzy_distance: 0i32,
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
        let total = response.total as i32;
        let obtained = response.top_docs.len();
        let requested = response.results_per_page as usize;
        let next_page = obtained > requested;
        let no_results = std::cmp::min(obtained, requested);
        let mut results: Vec<ParagraphResult> = Vec::with_capacity(no_results);
        let searcher = response.searcher;
        let default_split = Value::Str("".to_string());
        for (score, doc_address) in response.top_docs.into_iter().take(no_results) {
            match searcher.doc(doc_address) {
                Ok(doc) => {
                    let score = ResultScore {
                        bm25: score,
                        booster: 0.0,
                    };
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

                    let labels = doc
                        .get_all(schema.facets)
                        .map(|x| x.as_facet().unwrap().to_path_string())
                        .filter(|x| x.starts_with("/l/"))
                        .collect_vec();

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
                        labels,
                        field,
                        split,
                        index,
                        paragraph,
                        score: Some(score),
                        start: start_pos,
                        end: end_pos,
                        matches: terms,
                        metadata: schema.metadata(&doc),
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
            next_page,
            total,
            bm25: true,
            fuzzy_distance: 0i32,
            page_number: response.page_number,
            result_per_page: response.results_per_page,
            query: response.query.to_string(),
            ematches: response.termc.eterms.into_iter().collect(),
        }
    }
}
