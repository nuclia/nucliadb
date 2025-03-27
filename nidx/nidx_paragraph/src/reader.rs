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

use std::fmt::Debug;
use std::time::Instant;

use nidx_protos::order_by::{OrderField, OrderType};
use nidx_protos::{OrderBy, ParagraphItem, ParagraphSearchResponse, StreamRequest};
use nidx_types::prefilter::PrefilterResult;
use tantivy::collector::{Collector, Count, FacetCollector, TopDocs};
use tantivy::query::{AllQuery, Query, QueryParser};
use tantivy::{DateTime, Order, schema::*};
use tantivy::{DocAddress, Index, IndexReader};
use tracing::*;

use super::schema::ParagraphSchema;
use crate::request_types::{ParagraphSearchRequest, ParagraphSuggestRequest};
use crate::search_query::{SharedTermC, search_query, streaming_query, suggest_query};
use crate::search_response::{SearchBm25Response, SearchFacetsResponse, SearchIntResponse, extract_labels};

const FUZZY_DISTANCE: u8 = 1;
const NUMBER_OF_RESULTS_SUGGEST: usize = 20;

pub struct ParagraphReaderService {
    pub index: Index,
    pub schema: ParagraphSchema,
    pub reader: IndexReader,
}

impl Debug for ParagraphReaderService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TextService")
            .field("index", &self.index)
            .field("schema", &self.schema)
            .finish()
    }
}

impl ParagraphReaderService {
    fn count(&self) -> anyhow::Result<usize> {
        let searcher = self.reader.searcher();
        let count = searcher.search(&AllQuery, &Count).unwrap_or_default();
        Ok(count)
    }

    pub fn suggest(
        &self,
        request: &ParagraphSuggestRequest,
        prefilter: &PrefilterResult,
    ) -> anyhow::Result<ParagraphSearchResponse> {
        let parser = QueryParser::for_index(&self.index, vec![self.schema.text]);
        let text = self.adapt_text(&parser, &request.body);
        let (original, termc, fuzzied) =
            suggest_query(&parser, &text, request, prefilter, &self.schema, FUZZY_DISTANCE);

        let searcher = self.reader.searcher();
        let topdocs = TopDocs::with_limit(NUMBER_OF_RESULTS_SUGGEST);
        let mut results = searcher.search(&original, &topdocs)?;

        if results.is_empty() {
            let topdocs = TopDocs::with_limit(NUMBER_OF_RESULTS_SUGGEST);
            match searcher.search(&fuzzied, &topdocs) {
                Ok(mut fuzzied) => results.append(&mut fuzzied),
                Err(err) => error!("{err:?} during suggest"),
            }
        }

        Ok(ParagraphSearchResponse::from(SearchBm25Response {
            total: results.len(),
            top_docs: results,
            facets_count: None,
            facets: vec![],
            termc: termc.get_termc(),
            text_service: self,
            query: &text,
            page_number: 1,
            results_per_page: 10,
            searcher,
            min_score: 0.0,
        }))
    }

    pub fn iterator(&self, request: &StreamRequest) -> anyhow::Result<impl Iterator<Item = ParagraphItem> + use<>> {
        let producer = BatchProducer {
            offset: 0,
            total: self.count()?,
            paragraph_field: self.schema.paragraph,
            facet_field: self.schema.facets,
            searcher: self.reader.searcher(),
            query: streaming_query(&self.schema, request),
        };
        Ok(producer.flatten())
    }

    pub fn search(
        &self,
        request: &ParagraphSearchRequest,
        prefilter: &PrefilterResult,
    ) -> anyhow::Result<ParagraphSearchResponse> {
        let time = Instant::now();
        let id = Some(&request.id);

        let v = time.elapsed().as_millis();
        debug!("{id:?} - Creating query: starts at {v} ms");

        let parser = QueryParser::for_index(&self.index, vec![self.schema.text]);
        let results = request.result_per_page as usize;
        let offset = results * request.page_number as usize;

        let facets: Vec<_> = request
            .faceted
            .as_ref()
            .iter()
            .flat_map(|v| v.labels.iter())
            .filter(|s| ParagraphReaderService::is_valid_facet(s))
            .cloned()
            .collect();
        let text = self.adapt_text(&parser, &request.body);
        let v = time.elapsed().as_millis();
        debug!("{id:?} - Creating query: ends at {v} ms");

        let v = time.elapsed().as_millis();
        debug!("{id:?} - Searching: starts at {v} ms");

        let advanced = request
            .advanced_query
            .as_ref()
            .map(|query| parser.parse_query(query))
            .transpose()?;
        #[rustfmt::skip] let (original, termc, fuzzied) = search_query(
            &parser,
            &text,
            request,
            prefilter,
            &self.schema,
            FUZZY_DISTANCE,
            advanced
        );
        let searcher = Searcher {
            request,
            results,
            offset,
            facets: &facets,
            text: &text,
            only_faceted: request.only_faceted,
        };
        let mut response = searcher.do_search(termc.clone(), original, self, request.min_score)?;
        let v = time.elapsed().as_millis();
        debug!("{id:?} - Searching: ends at {v} ms");

        if response.results.is_empty() && request.result_per_page > 0 && request.min_score == 0 as f32 {
            let v = time.elapsed().as_millis();
            debug!("{id:?} - Applying fuzzy: starts at {v} ms");

            let fuzzied = searcher.do_search(termc, fuzzied, self, request.min_score)?;
            response = fuzzied;
            response.fuzzy_distance = FUZZY_DISTANCE as i32;
            let v = time.elapsed().as_millis();
            debug!("{id:?} - Applying fuzzy: ends at {v} ms");
        }

        let v = time.elapsed().as_millis();
        debug!("{id:?} - Producing results: starts at {v} ms");

        let total = response.results.len() as f32;
        let mut some_below_min_score: bool = false;
        let response_results = std::mem::take(&mut response.results);

        for (i, mut r) in response_results.into_iter().enumerate() {
            match &mut r.score {
                None => continue,
                Some(sc) if sc.bm25 < request.min_score => {
                    // We can break here because the results are sorted by score
                    some_below_min_score = true;
                    break;
                }
                Some(sc) => {
                    sc.booster = total - (i as f32);
                    response.results.push(r);
                }
            }
        }

        if some_below_min_score {
            // We set next_page to false so that the client stops asking for more results
            response.next_page = false;
        }

        let v = time.elapsed().as_millis();
        debug!("{id:?} - Producing results: starts at {v} ms");

        let v = time.elapsed().as_millis();
        debug!("{id:?} - Ending at: {v} ms");

        Ok(response)
    }
    fn adapt_text(&self, parser: &QueryParser, text: &str) -> String {
        match text.trim() {
            "" => text.to_string(),
            text => parser
                .parse_query(text)
                .map(|_| text.to_string())
                .unwrap_or_else(|_| format!("\"{}\"", text.replace('"', ""))),
        }
    }
    fn is_valid_facet(maybe_facet: &str) -> bool {
        Facet::from_text(maybe_facet).is_ok()
    }
}

pub struct BatchProducer {
    total: usize,
    offset: usize,
    paragraph_field: Field,
    facet_field: Field,
    query: Box<dyn Query>,
    searcher: tantivy::Searcher,
}
impl BatchProducer {
    const BATCH: usize = 1000;
}
impl Iterator for BatchProducer {
    type Item = Vec<ParagraphItem>;
    fn next(&mut self) -> Option<Self::Item> {
        let time = Instant::now();
        if self.offset >= self.total {
            debug!("No more batches available");
            return None;
        }
        debug!("Producing a new batch with offset: {}", self.offset);

        let topdocs = TopDocs::with_limit(Self::BATCH).and_offset(self.offset);
        let Ok(top_docs) = self.searcher.search(&self.query, &topdocs) else {
            error!("Something went wrong");
            return None;
        };
        let mut items = vec![];
        for doc in top_docs
            .into_iter()
            .flat_map(|i| self.searcher.doc::<TantivyDocument>(i.1))
        {
            let id = doc
                .get_first(self.paragraph_field)
                .expect("document doesn't appear to have uuid.")
                .as_str()
                .unwrap()
                .to_string();

            let labels = extract_labels(doc.get_all(self.facet_field));
            items.push(ParagraphItem { id, labels });
        }
        self.offset += Self::BATCH;
        let v = time.elapsed().as_millis();
        debug!("New batch created, took {v} ms");

        Some(items)
    }
}

struct Searcher<'a> {
    request: &'a ParagraphSearchRequest,
    results: usize,
    offset: usize,
    facets: &'a [String],
    text: &'a str,
    only_faceted: bool,
}
impl Searcher<'_> {
    fn custom_order_collector(
        &self,
        order: OrderBy,
        limit: usize,
        offset: usize,
    ) -> impl Collector<Fruit = Vec<(DateTime, DocAddress)>> {
        let order_field = match order.sort_by() {
            OrderField::Created => "created",
            OrderField::Modified => "modified",
        };
        let order_direction = match order.r#type() {
            OrderType::Desc => Order::Desc,
            OrderType::Asc => Order::Asc,
        };
        TopDocs::with_limit(limit)
            .and_offset(offset)
            .order_by_fast_field(order_field, order_direction)
    }
    fn do_search(
        &self,
        termc: SharedTermC,
        query: Box<dyn Query>,
        service: &ParagraphReaderService,
        min_score: f32,
    ) -> anyhow::Result<ParagraphSearchResponse> {
        let searcher = service.reader.searcher();
        let facet_collector = self
            .facets
            .iter()
            .fold(FacetCollector::for_field("facets"), |mut collector, facet| {
                collector.add_facet(Facet::from(facet));
                collector
            });
        if self.only_faceted {
            // No query search, just facets
            let facets_count = searcher.search(&query, &facet_collector).unwrap();
            Ok(ParagraphSearchResponse::from(SearchFacetsResponse {
                text_service: service,
                facets_count: Some(facets_count),
                facets: self.facets.to_vec(),
            }))
        } else if self.facets.is_empty() {
            // Only query no facets
            let extra_result = self.results + 1;
            match self.request.order.clone() {
                Some(order) => {
                    let custom_collector = self.custom_order_collector(order, extra_result, self.offset);
                    let collector = &(Count, custom_collector);
                    let (total, top_docs) = searcher.search(&query, collector)?;
                    Ok(ParagraphSearchResponse::from(SearchIntResponse {
                        total,
                        facets_count: None,
                        facets: self.facets.to_vec(),
                        top_docs,
                        termc: termc.get_termc(),
                        text_service: service,
                        query: self.text,
                        page_number: self.request.page_number,
                        results_per_page: self.results as i32,
                        searcher,
                    }))
                }
                None => {
                    let topdocs_collector = TopDocs::with_limit(extra_result).and_offset(self.offset);
                    let collector = &(Count, topdocs_collector);
                    let (total, top_docs) = searcher.search(&query, collector)?;
                    Ok(ParagraphSearchResponse::from(SearchBm25Response {
                        total,
                        facets_count: None,
                        facets: self.facets.to_vec(),
                        top_docs,
                        termc: termc.get_termc(),
                        text_service: service,
                        query: self.text,
                        page_number: self.request.page_number,
                        results_per_page: self.results as i32,
                        searcher,
                        min_score,
                    }))
                }
            }
        } else {
            let extra_result = self.results + 1;

            match self.request.order.clone() {
                Some(order) => {
                    let custom_collector = self.custom_order_collector(order, extra_result, self.offset);
                    let collector = &(Count, facet_collector, custom_collector);
                    let (total, facets_count, top_docs) = searcher.search(&query, collector)?;
                    Ok(ParagraphSearchResponse::from(SearchIntResponse {
                        total,
                        top_docs,
                        facets_count: Some(facets_count),
                        facets: self.facets.to_vec(),
                        termc: termc.get_termc(),
                        text_service: service,
                        query: self.text,
                        page_number: self.request.page_number,
                        results_per_page: self.results as i32,
                        searcher,
                    }))
                }
                None => {
                    let topdocs_collector = TopDocs::with_limit(extra_result).and_offset(self.offset);
                    let collector = &(Count, facet_collector, topdocs_collector);
                    let (total, facets_count, top_docs) = searcher.search(&query, collector)?;
                    Ok(ParagraphSearchResponse::from(SearchBm25Response {
                        total,
                        top_docs,
                        facets_count: Some(facets_count),
                        facets: self.facets.to_vec(),
                        termc: termc.get_termc(),
                        text_service: service,
                        query: self.text,
                        page_number: self.request.page_number,
                        results_per_page: self.results as i32,
                        searcher,
                        min_score,
                    }))
                }
            }
        }
    }
}
