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

use crate::schema::{datetime_utc_to_timestamp, decode_field_id, encode_field_id_bytes};
use crate::search_query::filter_to_query;
use crate::{DocumentSearchRequest, FieldUid, ParagraphUid, prefilter::*};

use super::schema::TextSchema;
use super::search_query;
use itertools::Itertools;
use nidx_protos::document_result::SortValue;
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
use tantivy::{DateTime, DocAddress, Index, IndexReader, Searcher};
use tantivy::{Order, schema::*};
use tracing::*;
use uuid::Uuid;

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
        let (rid, fid) = decode_field_id(self.encoded_field_id_reader.values_for_doc(doc));

        self.results.push(FieldId {
            resource_id: rid,
            field_id: Some(fid),
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
        let mut subqueries = vec![];

        if let Some(security) = request.security.as_ref() {
            subqueries.push(crate::search_query::security_query(schema, security));
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
        let time = Instant::now();
        let searcher = self.reader.searcher();
        let count = searcher.search(&AllQuery, &Count)?;
        let v = time.elapsed().as_millis();
        debug!("Ending at: {v} ms");

        Ok(count)
    }

    pub fn search(&self, request: &DocumentSearchRequest) -> anyhow::Result<DocumentSearchResponse> {
        self.do_search(request)
    }
}

impl TextReaderService {
    fn custom_order_collector(
        &self,
        order: OrderBy,
        limit: usize,
    ) -> impl Collector<Fruit = Vec<(Option<DateTime>, DocAddress)>> {
        let order_field = match order.sort_by() {
            OrderField::Created => "created",
            OrderField::Modified => "modified",
        };
        let order_direction = match order.r#type() {
            OrderType::Desc => Order::Desc,
            OrderType::Asc => Order::Asc,
        };
        TopDocs::with_limit(limit).order_by_fast_field(order_field, order_direction)
    }

    fn convert_int_order(
        &self,
        response: SearchResponse<Option<DateTime>>,
        searcher: &Searcher,
    ) -> DocumentSearchResponse {
        let total = response.total as i32;
        let retrieved_results = response.results_per_page;
        let next_page = total > retrieved_results;
        let results_per_page = response.results_per_page as usize;
        let result_stream = response.top_docs.into_iter().take(results_per_page);
        let mut results = Vec::with_capacity(results_per_page);

        for (score, doc_address) in result_stream {
            match searcher.doc::<TantivyDocument>(doc_address) {
                Ok(doc) => {
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

                    let sort_value = score.as_ref().map(|s| SortValue::Date(datetime_utc_to_timestamp(s)));

                    let result = DocumentResult {
                        uuid,
                        field,
                        sort_value,
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
                    let score = ResultScore {
                        bm25: score,
                        booster: id as f32,
                    };
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
                        sort_value: Some(SortValue::Score(score)),
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
        let time = Instant::now();

        let v = time.elapsed().as_millis();
        debug!("Creating query: starts at {v} ms");

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
        let maybe_order = request.order;
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
        debug!("Creating query: ends at {v} ms");

        let v = time.elapsed().as_millis();
        debug!("Searching: starts at {v} ms");

        let searcher = self.reader.searcher();
        let v = time.elapsed().as_millis();
        debug!("Searching: ends at {v} ms");

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
                let topdocs_collector = TopDocs::with_limit(extra_result).order_by_score();
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

    pub fn get_fields_text(&self, field_uids: Vec<FieldUid>) -> anyhow::Result<HashMap<FieldUid, Option<String>>> {
        let searcher = self.reader.searcher();
        let results = self.search_fields(searcher.clone(), field_uids.iter())?;

        let mut texts = HashMap::new();
        for (_score, doc_id) in results {
            let doc = searcher.doc::<TantivyDocument>(doc_id)?;
            let doc_value = doc.get_first(self.schema.text);

            let rid = String::from_utf8(
                doc.get_first(self.schema.uuid)
                    .expect("document doesn't appear to have uuid.")
                    .as_bytes()
                    .unwrap()
                    .to_vec(),
            )
            .unwrap();
            let field = decode_facet(
                doc.get_first(self.schema.field)
                    .expect("document doesn't appear to have field.")
                    .as_facet()
                    .unwrap(),
            )
            .to_path_string();
            let parts: Vec<_> = field.split('/').collect(); // e.g. /a/title
            let field_uid = FieldUid {
                rid,
                field_type: parts[1].to_string(),
                field_name: parts[2].to_string(),
                split: parts.get(3).map(|x| x.to_string()),
            };

            let text = doc_value.map(|value| String::from(value.as_str().unwrap()));
            texts.insert(field_uid, text);
        }

        Ok(texts)
    }

    pub fn get_paragraphs_text(
        &self,
        paragraph_uids: Vec<ParagraphUid>,
    ) -> anyhow::Result<HashMap<ParagraphUid, String>> {
        let mut field_paragraph_ids = HashMap::new();
        for paragraph_id in paragraph_uids {
            let field_id = FieldUid::from(paragraph_id.clone());
            field_paragraph_ids
                .entry(field_id)
                .and_modify(|v: &mut Vec<ParagraphUid>| v.push(paragraph_id.clone()))
                .or_insert(vec![paragraph_id]);
        }

        let searcher = self.reader.searcher();
        let results = self.search_fields(searcher.clone(), field_paragraph_ids.keys())?;

        let mut paragraphs_text = HashMap::new();
        for (_score, doc_id) in results {
            let doc = searcher.doc::<TantivyDocument>(doc_id)?;

            let Some(text) = doc.get_first(self.schema.text).map(|value| value.as_str().unwrap()) else {
                // can't do anything without extracted text
                continue;
            };
            let rid = String::from_utf8(
                doc.get_first(self.schema.uuid)
                    .expect("document doesn't appear to have uuid.")
                    .as_bytes()
                    .unwrap()
                    .to_vec(),
            )
            .unwrap();
            let field = decode_facet(
                doc.get_first(self.schema.field)
                    .expect("document doesn't appear to have field.")
                    .as_facet()
                    .unwrap(),
            )
            .to_path_string();

            let parts: Vec<_> = field.split('/').collect(); // e.g. /a/title
            let field_uid = FieldUid {
                rid,
                field_type: parts[1].to_string(),
                field_name: parts[2].to_string(),
                split: parts.get(3).map(|x| x.to_string()),
            };

            if let Some(paragraph_ids) = field_paragraph_ids.remove(&field_uid) {
                // iterate the text by unicode characters only once, reusing the same iterator for
                // all paragraphs on the field. This is more useful for multiple paragraphs per
                // field on a large text
                let mut paragraphs = Self::extract_paragraphs(paragraph_ids.into_iter(), text.chars());
                for (k, v) in paragraphs.drain() {
                    paragraphs_text.insert(k, v);
                }
            }
        }

        Ok(paragraphs_text)
    }

    fn search_fields<'a>(
        &self,
        searcher: Searcher,
        field_uids: impl Iterator<Item = &'a FieldUid>,
    ) -> anyhow::Result<Vec<(f32, DocAddress)>> {
        // due to implementation details, we use here a BooleanQuery as it's
        // around 2 orders of magnitude faster than a TermSetQuery
        let mut subqueries: Vec<Box<dyn Query>> = vec![];
        for field_uid in field_uids {
            subqueries.push(Box::new(TermQuery::new(
                Term::from_field_bytes(
                    self.schema.encoded_field_id_bytes,
                    &encode_field_id_bytes(
                        Uuid::parse_str(&field_uid.rid)?,
                        &format!("{}/{}", field_uid.field_type, field_uid.field_name),
                    ),
                ),
                IndexRecordOption::Basic,
            )));
        }
        if subqueries.is_empty() {
            // skip, as we don't have anything to search and tantivy panics with limit=0
            return Ok(vec![]);
        }
        // we store a doc per field, so we expect at most the number of unique fields
        let limit = subqueries.len();
        let query: Box<dyn Query> = Box::new(BooleanQuery::union(subqueries));
        let collector = TopDocs::with_limit(limit).order_by_score();
        let results = searcher.search(&query, &collector)?;
        Ok(results)
    }

    fn extract_paragraphs(
        ids: impl Iterator<Item = ParagraphUid>,
        mut text: std::str::Chars<'_>,
    ) -> HashMap<ParagraphUid, String> {
        let mut paragraphs = HashMap::new();

        // sort paragraph_ids by (start, end) to avoid the need of already read chars from the text
        let mut ids = ids.sorted_by_key(|id| (id.paragraph_start, id.paragraph_end));

        let Some(first) = ids.next() else {
            return paragraphs;
        };
        let mut window = std::ops::Range {
            start: first.paragraph_start,
            end: first.paragraph_end,
        };
        let mut window_paragraphs = vec![first];

        let mut position = 0;

        for paragraph_id in ids {
            if paragraph_id.paragraph_start < window.end {
                // This paragraph overlaps with the window. We can't be sure if there will be more
                // in the future, so we widen the window and continue
                window.end = std::cmp::max(window.end, paragraph_id.paragraph_end);
                window_paragraphs.push(paragraph_id);
            } else {
                // A non-overlapping paragraph means we won't find any other paragraph that needs
                // the text from the window. We then read the window and extract the paragraphs
                let skip = window.start - position;
                let take = window.end - window.start;
                let chunk: Vec<char> = text.by_ref().skip(skip as usize).take(take as usize).collect();
                position = window.end;

                for id in window_paragraphs.drain(..) {
                    let start = (id.paragraph_start - window.start) as usize;
                    // clamp to chunk size, we don't have more text
                    let end = std::cmp::min((id.paragraph_end - window.start) as usize, chunk.len());
                    let paragraph: String = chunk[start..end].iter().collect();
                    paragraphs.insert(id, paragraph);
                }

                // As the new paragraph could overlap with future ones, we reset the window with it
                window = std::ops::Range {
                    start: paragraph_id.paragraph_start,
                    end: paragraph_id.paragraph_end,
                };
                window_paragraphs.push(paragraph_id);
            }
        }

        // with no more paragraphs, we can finish with the window
        position = window.start - position;
        let take = window.end - window.start;
        let chunk: Vec<char> = text.by_ref().skip(position as usize).take(take as usize).collect();

        for id in window_paragraphs.drain(..) {
            let start = (id.paragraph_start - window.start) as usize;
            let end = std::cmp::min((id.paragraph_end - window.start) as usize, chunk.len());
            let paragraph: String = chunk[start..end].iter().collect();
            paragraphs.insert(id, paragraph);
        }

        paragraphs
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
        let top_docs = TopDocs::with_limit(Self::BATCH)
            .and_offset(self.offset)
            .order_by_score();
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_paragraphs_using_a_single_iterator() {
        let text = "This is my test text";
        let word_positions = [(0, 4), (5, 7), (8, 10), (11, 15), (16, 20)];
        let words: Vec<ParagraphUid> = word_positions
            .into_iter()
            .map(|(start, end)| ParagraphUid {
                rid: "rid".to_string(),
                field_type: "a".to_string(),
                field_name: "title".to_string(),
                split: None,
                paragraph_start: start,
                paragraph_end: end,
            })
            .collect();

        // longer paragraphs overlapping with the words above
        let overlapping_positions = [
            (0, 7),
            // intersects with the above but has content outside
            (5, 15),
            // same as above
            (5, 15),
            // subset of the above
            (8, 15),
        ];
        let overlapping: Vec<ParagraphUid> = overlapping_positions
            .into_iter()
            .map(|(start, end)| ParagraphUid {
                rid: "rid".to_string(),
                field_type: "a".to_string(),
                field_name: "title".to_string(),
                split: None,
                paragraph_start: start,
                paragraph_end: end,
            })
            .collect();

        let out_of_bounds: Vec<ParagraphUid> = [(8, 100), (16, 100), (200, 300)]
            .into_iter()
            .map(|(start, end)| ParagraphUid {
                rid: "rid".to_string(),
                field_type: "a".to_string(),
                field_name: "title".to_string(),
                split: None,
                paragraph_start: start,
                paragraph_end: end,
            })
            .collect();

        let paragraphs = TextReaderService::extract_paragraphs(
            [
                overlapping[2].clone(),
                words[3].clone(),
                overlapping[3].clone(),
                out_of_bounds[2].clone(),
                words[1].clone(),
                overlapping[0].clone(),
                out_of_bounds[1].clone(),
                words[4].clone(),
                words[0].clone(),
                overlapping[1].clone(),
                out_of_bounds[0].clone(),
                words[2].clone(),
            ]
            .into_iter(),
            text.chars(),
        );
        assert_eq!(
            paragraphs,
            HashMap::from_iter([
                (words[0].clone(), ("This".to_string())),
                (overlapping[0].clone(), ("This is".to_string())),
                (words[1].clone(), ("is".to_string())),
                (overlapping[1].clone(), ("is my test".to_string())),
                (overlapping[2].clone(), ("is my test".to_string())),
                (words[2].clone(), ("my".to_string())),
                (overlapping[3].clone(), ("my test".to_string())),
                (out_of_bounds[0].clone(), ("my test text".to_string())),
                (words[3].clone(), ("test".to_string())),
                (words[4].clone(), ("text".to_string())),
                (out_of_bounds[1].clone(), ("text".to_string())),
                (out_of_bounds[2].clone(), ("".to_string())),
            ])
        );
    }
}
