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
use std::collections::HashMap;

use lazy_static::lazy_static;
use nidx_protos::order_by::OrderField;
use nidx_protos::paragraph_result::SortValue;
use nidx_protos::{FacetResult, FacetResults, ParagraphResult, ParagraphSearchResponse, ResultScore};
use nidx_tantivy::utils::decode_facet;
use tantivy::collector::FacetCounts;
use tantivy::schema::document::CompactDocValue;
use tantivy::schema::{Facet, Value};
use tantivy::{DateTime, DocAddress, TantivyDocument};
use tracing::*;

use crate::reader::ParagraphReaderService;
use crate::schema::datetime_utc_to_timestamp;
use crate::search_query::TermCollector;

pub fn extract_labels<'a>(facets_iterator: impl Iterator<Item = CompactDocValue<'a>>) -> Vec<String> {
    facets_iterator
        .flat_map(|x| x.as_facet())
        .map(decode_facet)
        .filter(is_label)
        .map(|x| x.to_path_string())
        .collect()
}

pub fn is_label(facet: &Facet) -> bool {
    lazy_static! {
        static ref LABEL_PREFIX: Facet = Facet::from_text("/l").unwrap();
    };
    LABEL_PREFIX.is_prefix_of(facet)
}

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
pub fn produce_facets(facets: &[Facet], facets_count: FacetCounts) -> HashMap<String, FacetResults> {
    facets
        .iter()
        .map(|facet| facet.to_string())
        .filter_map(|facet| {
            let facet_counts = facet_count(&facet, &facets_count);

            if !facet_counts.is_empty() {
                Some((
                    facet,
                    FacetResults {
                        facetresults: facet_counts,
                    },
                ))
            } else {
                None
            }
        })
        .collect()
}

pub struct SearchBm25Response<'a> {
    pub total: usize,
    pub text_service: &'a ParagraphReaderService,
    pub query: &'a str,
    pub facets_count: Option<FacetCounts>,
    pub facets: &'a [Facet],
    pub top_docs: Vec<(f32, DocAddress)>,
    pub results_per_page: i32,
    pub termc: TermCollector,
    pub searcher: tantivy::Searcher,
    pub min_score: f32,
}

pub struct SearchIntResponse<'a> {
    pub total: usize,
    pub text_service: &'a ParagraphReaderService,
    pub query: &'a str,
    pub facets_count: Option<FacetCounts>,
    pub facets: &'a [Facet],
    pub top_docs: Vec<(Option<DateTime>, DocAddress)>,
    pub results_per_page: i32,
    pub termc: TermCollector,
    pub searcher: tantivy::Searcher,
    pub sort_field: OrderField,
}

pub struct SearchFacetsResponse<'a> {
    pub text_service: &'a ParagraphReaderService,
    pub facets_count: Option<FacetCounts>,
    pub facets: &'a [Facet],
}

impl From<SearchFacetsResponse<'_>> for ParagraphSearchResponse {
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

impl From<SearchIntResponse<'_>> for ParagraphSearchResponse {
    fn from(response: SearchIntResponse) -> Self {
        let total = response.total as i32;
        let obtained = response.top_docs.len();
        let requested = response.results_per_page as usize;
        let next_page = obtained > requested;
        let no_results = std::cmp::min(obtained, requested);
        let mut results: Vec<ParagraphResult> = Vec::with_capacity(no_results);
        let searcher = response.searcher;
        for (score, doc_address) in response.top_docs.into_iter().take(no_results) {
            match searcher.doc::<TantivyDocument>(doc_address) {
                Ok(doc) => {
                    let schema = &response.text_service.schema;
                    let uuid = doc
                        .get_first(schema.uuid)
                        .expect("document doesn't appear to have uuid.")
                        .as_str()
                        .unwrap()
                        .to_string();

                    let field_facet = doc
                        // This can be used instead of get_first() considering there is only one
                        // field. Done because of a bug in the writing
                        // process [sc 1604].
                        .get_all(schema.field)
                        .last()
                        .expect("document doesn't appear to have uuid.")
                        .as_facet()
                        .unwrap();
                    let field = decode_facet(field_facet).to_path_string();

                    let labels = extract_labels(doc.get_all(schema.facets));

                    let start_pos = doc.get_first(schema.start_pos).unwrap().as_u64().unwrap();

                    let end_pos = doc.get_first(schema.end_pos).unwrap().as_u64().unwrap();

                    let paragraph = doc
                        .get_first(schema.paragraph)
                        .expect("document doesn't appear to have end_pos.")
                        .as_str()
                        .unwrap()
                        .to_string();

                    let split = doc
                        .get_first(schema.split)
                        .map(|v| v.as_str())
                        .unwrap_or(Some(""))
                        .unwrap()
                        .to_string();

                    let sort_value = score.as_ref().map(|s| SortValue::Date(datetime_utc_to_timestamp(s)));

                    let index = doc.get_first(schema.index).unwrap().as_u64().unwrap();
                    let mut terms: Vec<_> = response.termc.get_fterms(doc_address.doc_id).into_iter().collect();
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
                        sort_value,
                        metadata: schema.metadata(&doc),
                        shard_id: vec![],
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
            query: response.query.to_string(),
            next_page,
            ematches: response.termc.eterms.into_iter().collect(),
        }
    }
}

impl From<SearchBm25Response<'_>> for ParagraphSearchResponse {
    fn from(response: SearchBm25Response) -> Self {
        let total = response.total as i32;
        let min_score = response.min_score;
        let obtained = response.top_docs.len();
        let requested = response.results_per_page as usize;
        let next_page = obtained > requested;
        let no_results = std::cmp::min(obtained, requested);
        let mut results: Vec<ParagraphResult> = Vec::with_capacity(no_results);
        let searcher = response.searcher;
        for (score, doc_address) in response.top_docs.into_iter().take(no_results) {
            if score < min_score {
                break;
            }
            match searcher.doc::<TantivyDocument>(doc_address) {
                Ok(doc) => {
                    let score = ResultScore {
                        bm25: score,
                        docaddr: (doc_address.segment_ord as u64) << 32 | doc_address.doc_id as u64,
                    };
                    let schema = &response.text_service.schema;
                    let uuid = doc
                        .get_first(schema.uuid)
                        .expect("document doesn't appear to have uuid.")
                        .as_str()
                        .unwrap()
                        .to_string();

                    let field_facet = doc
                        .get_first(schema.field)
                        .expect("document doesn't appear to have uuid.")
                        .as_facet()
                        .unwrap();
                    let field = decode_facet(field_facet).to_path_string();

                    let labels = extract_labels(doc.get_all(schema.facets));

                    let start_pos = doc.get_first(schema.start_pos).unwrap().as_u64().unwrap();

                    let end_pos = doc.get_first(schema.end_pos).unwrap().as_u64().unwrap();

                    let paragraph = doc
                        .get_first(schema.paragraph)
                        .expect("document doesn't appear to have end_pos.")
                        .as_str()
                        .unwrap()
                        .to_string();

                    let split = doc
                        .get_first(schema.split)
                        .map(|v| v.as_str())
                        .unwrap_or(Some(""))
                        .unwrap()
                        .to_string();

                    let index = doc.get_first(schema.index).unwrap().as_u64().unwrap();
                    let mut terms: Vec<_> = response.termc.get_fterms(doc_address.doc_id).into_iter().collect();
                    terms.sort();
                    let result = ParagraphResult {
                        uuid,
                        labels,
                        field,
                        split,
                        index,
                        paragraph,
                        sort_value: Some(SortValue::Score(score)),
                        start: start_pos,
                        end: end_pos,
                        matches: terms,
                        metadata: schema.metadata(&doc),
                        shard_id: vec![],
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
            query: response.query.to_string(),
            ematches: response.termc.eterms.into_iter().collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::is_label;
    use tantivy::schema::Facet;

    #[test]
    fn test_is_label() {
        let label = Facet::from("/l/labelset1/label1");
        assert!(is_label(&label));

        let net = Facet::from("/e/PERSON/Juan");
        assert!(!is_label(&net));
    }
}
