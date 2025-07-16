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
use std::fmt::Debug;
use std::time::*;

use crate::schema::decode_field_id;
use crate::search_query::filter_to_query;
use crate::{DocumentSearchRequest, prefilter::*};

use super::schema::TextSchema;
use super::search_query;
use itertools::Itertools;
use nidx_protos::order_by::{OrderField, OrderType};
use nidx_protos::{
    DocumentItem, DocumentResult, DocumentSearchResponse, FacetResult, FacetResults, OrderBy, ResultScore,
    StreamRequest,
};
use nidx_tantivy::utils::decode_facet;
use nidx_types::prefilter::{FieldId, PrefilterResult};
use tantivy::collector::{Collector, Count, FacetCollector, FacetCounts, SegmentCollector, TopDocs};
use tantivy::columnar::Column;
use tantivy::query::{AllQuery, BooleanQuery, Query, QueryParser, TermQuery};
use tantivy::schema::Value;
use tantivy::schema::*;
use tantivy::{DocAddress, Index, IndexReader, Searcher};
use tracing::*;

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

pub struct SearchResponse<'a, S> {
    pub query: &'a str,
    pub facets_count: FacetCounts,
    pub facets: Vec<String>,
    pub top_docs: Vec<(S, DocAddress)>,
    pub results_per_page: i32,
    pub total: usize,
}

pub struct TextReaderService {
    pub(crate) index: Index,
    pub schema: TextSchema,
    pub reader: IndexReader,
}

impl Debug for TextReaderService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FieldReaderService")
            .field("index", &self.index)
            .field("schema", &self.schema)
            .finish()
    }
}

struct FieldUuidSegmentCollectorV2 {
    encoded_field_id_reader: Column,
    results: Vec<FieldId>,
}

impl SegmentCollector for FieldUuidSegmentCollectorV2 {
    type Fruit = Vec<FieldId>;

    fn collect(&mut self, doc: tantivy::DocId, _score: tantivy::Score) {
        let mut data = Vec::new();
        self.encoded_field_id_reader.fill_vals(doc, &mut data);
        let (rid, fid) = decode_field_id(&data);

        self.results.push(FieldId {
            resource_id: rid,
            field_id: fid,
        });
    }

    fn harvest(self) -> Self::Fruit {
        self.results
    }
}

struct FieldUuidCollector;

impl Collector for FieldUuidCollector {
    type Fruit = Vec<FieldId>;

    type Child = FieldUuidSegmentCollectorV2;

    fn for_segment(
        &self,
        _segment_local_id: tantivy::SegmentOrdinal,
        segment: &tantivy::SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        let encoded_field_id_reader = segment.fast_fields().u64("encoded_field_id")?;
        Ok(FieldUuidSegmentCollectorV2 {
            encoded_field_id_reader,
            results: vec![],
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<Self::Child as tantivy::collector::SegmentCollector>::Fruit>,
    ) -> tantivy::Result<Self::Fruit> {
        Ok(segment_fruits
            .into_iter()
            .reduce(|mut a, mut b| {
                a.append(&mut b);
                a
            })
            .unwrap_or_default())
    }
}

impl TextReaderService {
    pub fn prefilter(&self, request: &PreFilterRequest) -> anyhow::Result<PrefilterResult> {
        let schema = &self.schema;
        let mut access_groups_queries: Vec<Box<dyn Query>> = Vec::new();

        if let Some(security) = request.security.as_ref() {
            for group_id in security.access_groups.iter() {
                let mut group_id_key = group_id.clone();
                if !group_id.starts_with('/') {
                    // Slash needs to be added to be compatible with tantivy facet fields
                    group_id_key = "/".to_string() + group_id;
                }
                let facet = Facet::from_text(&group_id_key).unwrap();
                let term = Term::from_facet(self.schema.groups_with_access, &facet);
                let term_query = TermQuery::new(term, IndexRecordOption::Basic);
                access_groups_queries.push(Box::new(term_query));
            }
        }

        let mut subqueries = vec![];
        if !access_groups_queries.is_empty() {
            let public_fields_query = Box::new(TermQuery::new(
                Term::from_field_u64(self.schema.groups_public, 1_u64),
                IndexRecordOption::Basic,
            ));
            access_groups_queries.push(public_fields_query);
            let access_groups_query: Box<dyn Query> = Box::new(BooleanQuery::union(access_groups_queries));
            subqueries.push(access_groups_query);
        }

        if let Some(expr) = &request.filter_expression {
            subqueries.push(filter_to_query(schema, expr));
        }

        if subqueries.is_empty() {
            return Ok(PrefilterResult::All);
        }

        let prefilter_query: Box<dyn Query> = Box::new(BooleanQuery::intersection(subqueries));
        let searcher = self.reader.searcher();
        let docs_fulfilled = searcher.search(&prefilter_query, &FieldUuidCollector)?;

        // If none of the fields match the pre-filter, thats all the query planner needs to know.
        if docs_fulfilled.is_empty() {
            return Ok(PrefilterResult::None);
        }

        // If all the fields match the pre-filter, thats all the query planner needs to know
        if docs_fulfilled.len() as u64 == searcher.num_docs() {
            return Ok(PrefilterResult::All);
        }

        // The fields matching the pre-filter are a non-empty subset of all the fields
        Ok(PrefilterResult::Some(docs_fulfilled))
    }

    pub fn iterator(&self, request: &StreamRequest) -> anyhow::Result<impl Iterator<Item = DocumentItem> + use<>> {
        let producer = BatchProducer {
            offset: 0,
            total: self.count()?,
            field_field: self.schema.field,
            uuid_field: self.schema.uuid,
            facet_field: self.schema.facets,
            searcher: self.reader.searcher(),
            query: search_query::create_streaming_query(&self.schema, request),
        };
        Ok(producer.flatten())
    }

    fn count(&self) -> anyhow::Result<usize> {
        let id: Option<String> = None;
        let time = Instant::now();
        let searcher = self.reader.searcher();
        let count = searcher.search(&AllQuery, &Count)?;
        let v = time.elapsed().as_millis();
        debug!("{id:?} - Ending at: {v} ms");

        Ok(count)
    }

    pub fn search(&self, request: &DocumentSearchRequest) -> anyhow::Result<DocumentSearchResponse> {
        self.do_search(request)
    }
}

impl TextReaderService {
    fn custom_order_collector(&self, order: OrderBy, limit: usize) -> impl Collector<Fruit = Vec<(i64, DocAddress)>> {
        use tantivy::{DocId, SegmentReader};
        let sorter = match order.r#type() {
            OrderType::Desc => |t: i64| t,
            OrderType::Asc => |t: i64| -t,
        };
        TopDocs::with_limit(limit).custom_score(move |segment_reader: &SegmentReader| {
            let reader = match order.sort_by() {
                OrderField::Created => segment_reader.fast_fields().date("created").unwrap(),
                OrderField::Modified => segment_reader.fast_fields().date("modified").unwrap(),
            };
            move |doc: DocId| sorter(reader.values_for_doc(doc).next().unwrap().into_timestamp_secs())
        })
    }

    fn convert_int_order(&self, response: SearchResponse<i64>, searcher: &Searcher) -> DocumentSearchResponse {
        let total = response.total as i32;
        let retrieved_results = response.results_per_page;
        let next_page = total > retrieved_results;
        let results_per_page = response.results_per_page as usize;
        let result_stream = response.top_docs.into_iter().take(results_per_page).enumerate();
        let mut results = Vec::with_capacity(results_per_page);

        for (id, (_, doc_address)) in result_stream {
            match searcher.doc::<TantivyDocument>(doc_address) {
                Ok(doc) => {
                    let score = Some(ResultScore {
                        bm25: 0.0,
                        booster: id as f32,
                    });
                    let uuid = String::from_utf8(
                        doc.get_first(self.schema.uuid)
                            .expect("document doesn't appear to have uuid.")
                            .as_bytes()
                            .unwrap()
                            .to_vec(),
                    )
                    .unwrap();

                    let field_facet = doc
                        .get_first(self.schema.field)
                        .expect("document doesn't appear to have field.")
                        .as_facet()
                        .unwrap();
                    let field = decode_facet(field_facet).to_path_string();

                    let labels = doc
                        .get_all(self.schema.facets)
                        .flat_map(|x| x.as_facet())
                        .map(|x| decode_facet(x).to_path_string())
                        .filter(|x| x.starts_with("/l/"))
                        .collect_vec();

                    let result = DocumentResult {
                        uuid,
                        field,
                        score,
                        labels,
                    };
                    results.push(result);
                }
                Err(e) => error!("Error retrieving document from index: {}", e),
            }
        }

        let facets = produce_facets(response.facets, response.facets_count);
        DocumentSearchResponse {
            total,
            results,
            facets,
            result_per_page: response.results_per_page,
            query: response.query.to_string(),
            next_page,
            bm25: false,
        }
    }

    fn convert_bm25_order(
        &self,
        response: SearchResponse<f32>,
        searcher: &Searcher,
        min_score: f32,
    ) -> DocumentSearchResponse {
        let total = response.total as i32;
        let retrieved_results = response.results_per_page;
        let next_page = total > retrieved_results;
        let results_per_page = response.results_per_page as usize;
        let result_stream = response.top_docs.into_iter().take(results_per_page).enumerate();

        let mut results = Vec::with_capacity(results_per_page);
        for (id, (score, doc_address)) in result_stream {
            if score < min_score {
                continue;
            }
            match searcher.doc::<TantivyDocument>(doc_address) {
                Ok(doc) => {
                    let score = Some(ResultScore {
                        bm25: score,
                        booster: id as f32,
                    });
                    let uuid = String::from_utf8(
                        doc.get_first(self.schema.uuid)
                            .expect("document doesn't appear to have uuid.")
                            .as_bytes()
                            .unwrap()
                            .to_vec(),
                    )
                    .unwrap();

                    let field_facet = doc
                        .get_first(self.schema.field)
                        .expect("document doesn't appear to have field.")
                        .as_facet()
                        .unwrap();
                    let field = decode_facet(field_facet).to_path_string();

                    let labels = doc
                        .get_all(self.schema.facets)
                        .map(|x| decode_facet(x.as_facet().unwrap()).to_path_string())
                        .filter(|x| x.starts_with("/l/"))
                        .collect_vec();

                    let result = DocumentResult {
                        uuid,
                        field,
                        score,
                        labels,
                    };
                    results.push(result);
                }
                Err(e) => error!("Error retrieving document from index: {}", e),
            }
        }

        let facets = produce_facets(response.facets, response.facets_count);
        DocumentSearchResponse {
            results,
            facets,
            total: response.total as i32,
            result_per_page: response.results_per_page,
            query: response.query.to_string(),
            next_page,
            bm25: true,
        }
    }

    fn adapt_text(parser: &QueryParser, text: &str) -> String {
        match text {
            "" => text.to_string(),
            text => parser
                .parse_query(text)
                .map(|_| text.to_string())
                .unwrap_or_else(|_| format!("\"{}\"", text.replace('"', ""))),
        }
    }

    fn do_search(&self, request: &DocumentSearchRequest) -> anyhow::Result<DocumentSearchResponse> {
        use crate::search_query::create_query;
        let id = Some(&request.id);
        let time = Instant::now();

        let v = time.elapsed().as_millis();
        debug!("{id:?} - Creating query: starts at {v} ms");

        let query_parser = {
            let mut query_parser = QueryParser::for_index(&self.index, vec![self.schema.text]);
            query_parser.set_conjunction_by_default();
            query_parser
        };
        let text = TextReaderService::adapt_text(&query_parser, &request.body);
        let advanced_query = request
            .advanced_query
            .as_ref()
            .map(|query| query_parser.parse_query(query))
            .transpose()?;
        let query = create_query(&query_parser, request, &self.schema, &text, advanced_query)?;

        // Offset to search from
        let results = request.result_per_page as usize;
        let extra_result = results + 1;
        let maybe_order = request.order.clone();
        let valid_facet_iter = request
            .faceted
            .iter()
            .flat_map(|v| v.labels.iter().filter(|s| TextReaderService::is_valid_facet(s)));

        let mut facets = vec![];
        let mut facet_collector = FacetCollector::for_field("facets");
        for facet in valid_facet_iter {
            facets.push(facet.clone());
            facet_collector.add_facet(Facet::from(facet));
        }
        let v = time.elapsed().as_millis();
        debug!("{id:?} - Creating query: ends at {v} ms");

        let v = time.elapsed().as_millis();
        debug!("{id:?} - Searching: starts at {v} ms");

        let searcher = self.reader.searcher();
        let v = time.elapsed().as_millis();
        debug!("{id:?} - Searching: ends at {v} ms");

        match maybe_order {
            _ if request.only_faceted => {
                // Just a facet search
                let facets_count = searcher.search(&query, &facet_collector)?;
                Ok(DocumentSearchResponse {
                    facets: produce_facets(facets, facets_count),
                    ..Default::default()
                })
            }
            Some(order_by) => {
                let topdocs_collector = self.custom_order_collector(order_by, extra_result);
                let multicollector = &(facet_collector, topdocs_collector, Count);
                let (facets_count, top_docs, total) = searcher.search(&query, multicollector)?;
                let result = self.convert_int_order(
                    SearchResponse {
                        facets_count,
                        facets,
                        top_docs,
                        total,
                        query: &text,
                        results_per_page: results as i32,
                    },
                    &searcher,
                );
                Ok(result)
            }
            None => {
                let topdocs_collector = TopDocs::with_limit(extra_result);
                let multicollector = &(facet_collector, topdocs_collector, Count);
                let (facets_count, top_docs, total) = searcher.search(&query, multicollector)?;
                let result = self.convert_bm25_order(
                    SearchResponse {
                        facets_count,
                        facets,
                        top_docs,
                        total,
                        query: &text,
                        results_per_page: results as i32,
                    },
                    &searcher,
                    request.min_score,
                );
                Ok(result)
            }
        }
    }

    fn is_valid_facet(maybe_facet: &str) -> bool {
        Facet::from_text(maybe_facet).is_ok()
    }
}

pub struct BatchProducer {
    total: usize,
    offset: usize,
    query: Box<dyn Query>,
    field_field: Field,
    uuid_field: Field,
    facet_field: Field,
    searcher: tantivy::Searcher,
}
impl BatchProducer {
    const BATCH: usize = 1000;
}
impl Iterator for BatchProducer {
    type Item = Vec<DocumentItem>;
    fn next(&mut self) -> Option<Self::Item> {
        let time = Instant::now();
        if self.offset >= self.total {
            debug!("No more batches available");
            return None;
        }
        debug!("Producing a new batch with offset: {}", self.offset);
        let top_docs = TopDocs::with_limit(Self::BATCH).and_offset(self.offset);
        let top_docs = self.searcher.search(&self.query, &top_docs).unwrap();
        let mut items = vec![];
        for doc in top_docs
            .into_iter()
            .flat_map(|i| self.searcher.doc::<TantivyDocument>(i.1))
        {
            let uuid = String::from_utf8(
                doc.get_first(self.uuid_field)
                    .expect("document doesn't appear to have uuid.")
                    .as_bytes()
                    .unwrap()
                    .to_vec(),
            )
            .unwrap();

            let field_facet = doc
                .get_first(self.field_field)
                .expect("document doesn't appear to have field.")
                .as_facet()
                .unwrap();
            let field = decode_facet(field_facet).to_path_string();

            let labels = doc
                .get_all(self.facet_field)
                .flat_map(|x| x.as_facet())
                .map(|x| decode_facet(x).to_path_string())
                .filter(|x| x.starts_with("/l/"))
                .collect_vec();
            items.push(DocumentItem { field, uuid, labels });
        }
        self.offset += Self::BATCH;
        let v = time.elapsed().as_millis();
        debug!("New batch created, took {v} ms");

        Some(items)
    }
}
