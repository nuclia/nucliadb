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

use itertools::Itertools;
use nucliadb_core::metrics;
use nucliadb_core::metrics::request_time;
use nucliadb_core::prelude::*;
use nucliadb_core::protos::order_by::{OrderField, OrderType};
use nucliadb_core::protos::{
    DocumentItem, DocumentResult, DocumentSearchRequest, DocumentSearchResponse, FacetResult,
    FacetResults, OrderBy, ResultScore, StreamRequest,
};
use nucliadb_core::tracing::{self, *};
use tantivy::collector::{Collector, Count, DocSetCollector, FacetCollector, FacetCounts, TopDocs};
use tantivy::query::{AllQuery, Query, QueryParser};
use tantivy::schema::*;
use tantivy::{DocAddress, Index, IndexReader, LeasedItem, ReloadPolicy, Searcher};

use super::schema::TextSchema;
use super::search_query;

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
    pub order_by: Option<OrderBy>,
    pub page_number: i32,
    pub results_per_page: i32,
    pub total: usize,
}

pub struct TextReaderService {
    index: Index,
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

impl FieldReader for TextReaderService {
    #[tracing::instrument(skip_all)]
    fn iterator(&self, request: &StreamRequest) -> NodeResult<DocumentIterator> {
        let producer = BatchProducer {
            offset: 0,
            total: self.count()?,
            field_field: self.schema.field,
            uuid_field: self.schema.uuid,
            facet_field: self.schema.facets,
            searcher: self.reader.searcher(),
            query: search_query::create_streaming_query(&self.schema, request),
        };
        Ok(DocumentIterator::new(producer.flatten()))
    }

    #[tracing::instrument(skip_all)]
    fn count(&self) -> NodeResult<usize> {
        let id: Option<String> = None;
        let time = SystemTime::now();
        let searcher = self.reader.searcher();
        let count = searcher.search(&AllQuery, &Count)?;
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            debug!("{id:?} - Ending at: {v} ms");
        }
        Ok(count)
    }
}

impl ReaderChild for TextReaderService {
    type Request = DocumentSearchRequest;
    type Response = DocumentSearchResponse;
    #[tracing::instrument(skip_all)]
    fn search(&self, request: &Self::Request) -> NodeResult<Self::Response> {
        let time = SystemTime::now();

        let result = self.do_search(request)?;

        let metrics = metrics::get_metrics();
        let took = time.elapsed().map(|i| i.as_secs_f64()).unwrap_or(f64::NAN);
        let metric = request_time::RequestTimeKey::texts("search".to_string());
        metrics.record_request_time(metric, took);

        Ok(result)
    }
    #[tracing::instrument(skip_all)]
    fn stored_ids(&self) -> NodeResult<Vec<String>> {
        let time = SystemTime::now();

        let mut keys = vec![];
        let searcher = self.reader.searcher();
        for addr in searcher.search(&AllQuery, &DocSetCollector)? {
            let Some(key) = searcher
                .doc(addr)?
                .get_first(self.schema.uuid)
                .and_then(|i| i.as_text().map(String::from)) else { continue };
            keys.push(key);
        }

        let metrics = metrics::get_metrics();
        let took = time.elapsed().map(|i| i.as_secs_f64()).unwrap_or(f64::NAN);
        let metric = request_time::RequestTimeKey::texts("stored_ids".to_string());
        metrics.record_request_time(metric, took);

        Ok(keys)
    }
}

impl TextReaderService {
    fn custom_order_collector(
        &self,
        order: OrderBy,
        limit: usize,
        offset: usize,
    ) -> impl Collector<Fruit = Vec<(u64, DocAddress)>> {
        use tantivy::fastfield::{FastFieldReader, FastValue};
        use tantivy::{DocId, SegmentReader};
        let created = self.schema.created;
        let modified = self.schema.modified;
        let sorter = match order.r#type() {
            OrderType::Desc => |t: u64| t,
            OrderType::Asc => |t: u64| u64::MAX - t,
        };
        TopDocs::with_limit(limit).and_offset(offset).custom_score(
            move |segment_reader: &SegmentReader| {
                let reader = match order.sort_by() {
                    OrderField::Created => segment_reader.fast_fields().date(created).unwrap(),
                    OrderField::Modified => segment_reader.fast_fields().date(modified).unwrap(),
                };
                move |doc: DocId| sorter(reader.get(doc).to_u64())
            },
        )
    }

    #[tracing::instrument(skip_all)]
    pub fn start(config: &TextConfig) -> NodeResult<Self> {
        if !config.path.exists() {
            return Err(node_error!("Invalid path {:?}", config.path));
        }
        let field_schema = TextSchema::new();
        let index = Index::open_in_dir(&config.path)?;

        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommit)
            .try_into()?;

        Ok(TextReaderService {
            index,
            reader,
            schema: field_schema,
        })
    }

    fn convert_int_order(
        &self,
        response: SearchResponse<u64>,
        searcher: &Searcher,
    ) -> DocumentSearchResponse {
        let total = response.total as i32;
        let retrieved_results = (response.page_number + 1) * response.results_per_page;
        let next_page = total > retrieved_results;
        let mut results = Vec::with_capacity(response.top_docs.len());
        for (id, (_, doc_address)) in response.top_docs.into_iter().enumerate() {
            match searcher.doc(doc_address) {
                Ok(doc) => {
                    let score = Some(ResultScore {
                        bm25: 0.0,
                        booster: id as f32,
                    });
                    let uuid = doc
                        .get_first(self.schema.uuid)
                        .expect("document doesn't appear to have uuid.")
                        .as_text()
                        .unwrap()
                        .to_string();

                    let field = doc
                        .get_first(self.schema.field)
                        .expect("document doesn't appear to have field.")
                        .as_facet()
                        .unwrap()
                        .to_path_string();

                    let labels = doc
                        .get_all(self.schema.facets)
                        .map(|x| x.as_facet().unwrap().to_path_string())
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
            page_number: response.page_number,
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
    ) -> DocumentSearchResponse {
        let total = response.total as i32;
        let retrieved_results = (response.page_number + 1) * response.results_per_page;
        let next_page = total > retrieved_results;
        let results_per_page = response.results_per_page as usize;
        let result_stream = response
            .top_docs
            .into_iter()
            .take(results_per_page)
            .enumerate();

        let mut results = Vec::with_capacity(results_per_page);
        for (id, (score, doc_address)) in result_stream {
            match searcher.doc(doc_address) {
                Ok(doc) => {
                    let score = Some(ResultScore {
                        bm25: score,
                        booster: id as f32,
                    });
                    let uuid = doc
                        .get_first(self.schema.uuid)
                        .expect("document doesn't appear to have uuid.")
                        .as_text()
                        .unwrap()
                        .to_string();

                    let field = doc
                        .get_first(self.schema.field)
                        .expect("document doesn't appear to have field.")
                        .as_facet()
                        .unwrap()
                        .to_path_string();

                    let labels = doc
                        .get_all(self.schema.facets)
                        .flat_map(|x| x.as_facet())
                        .map(|x| x.to_path_string())
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
            page_number: response.page_number,
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

    #[tracing::instrument(skip_all)]
    fn do_search(&self, request: &DocumentSearchRequest) -> NodeResult<DocumentSearchResponse> {
        use crate::search_query::create_query;
        let id = Some(&request.id);
        let time = SystemTime::now();

        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            debug!("{id:?} - Creating query: starts at {v} ms");
        }
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
        let query = create_query(&query_parser, request, &self.schema, &text, advanced_query);

        // Offset to search from
        let results = request.result_per_page as usize;
        let offset = results * request.page_number as usize;
        let extra_result = results + 1;
        let maybe_order = request.order.clone();
        let valid_facet_iter = request.faceted.iter().flat_map(|v| {
            v.tags
                .iter()
                .filter(|s| TextReaderService::is_valid_facet(s))
        });

        let mut facets = vec![];
        let mut facet_collector = FacetCollector::for_field(self.schema.facets);
        for facet in valid_facet_iter {
            facets.push(facet.clone());
            facet_collector.add_facet(Facet::from(facet));
        }
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            debug!("{id:?} - Creating query: ends at {v} ms");
        }

        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            debug!("{id:?} - Searching: starts at {v} ms");
        }
        let searcher = self.reader.searcher();
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            debug!("{id:?} - Searching: ends at {v} ms");
        }
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
                let topdocs_collector = self.custom_order_collector(order_by, extra_result, offset);
                let multicollector = &(facet_collector, topdocs_collector, Count);
                let (facets_count, top_docs, total) = searcher.search(&query, multicollector)?;
                let result = self.convert_int_order(
                    SearchResponse {
                        facets_count,
                        facets,
                        top_docs,
                        total,
                        query: &text,
                        order_by: request.order.clone(),
                        page_number: request.page_number,
                        results_per_page: results as i32,
                    },
                    &searcher,
                );
                Ok(result)
            }
            None => {
                let topdocs_collector = TopDocs::with_limit(extra_result).and_offset(offset);
                let multicollector = &(facet_collector, topdocs_collector, Count);
                let (facets_count, top_docs, total) = searcher.search(&query, multicollector)?;
                let result = self.convert_bm25_order(
                    SearchResponse {
                        facets_count,
                        facets,
                        top_docs,
                        total,
                        query: &text,
                        order_by: request.order.clone(),
                        page_number: request.page_number,
                        results_per_page: results as i32,
                    },
                    &searcher,
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
    searcher: LeasedItem<tantivy::Searcher>,
}
impl BatchProducer {
    const BATCH: usize = 1000;
}
impl Iterator for BatchProducer {
    type Item = Vec<DocumentItem>;
    fn next(&mut self) -> Option<Self::Item> {
        let time = SystemTime::now();
        if self.offset >= self.total {
            debug!("No more batches available");
            return None;
        }
        debug!("Producing a new batch with offset: {}", self.offset);
        let top_docs = TopDocs::with_limit(Self::BATCH).and_offset(self.offset);
        let top_docs = self.searcher.search(&self.query, &top_docs).unwrap();
        let mut items = vec![];
        for doc in top_docs.into_iter().flat_map(|i| self.searcher.doc(i.1)) {
            let uuid = doc
                .get_first(self.uuid_field)
                .expect("document doesn't appear to have uuid.")
                .as_text()
                .unwrap()
                .to_string();

            let field = doc
                .get_first(self.field_field)
                .expect("document doesn't appear to have field.")
                .as_facet()
                .unwrap()
                .to_path_string();

            let labels = doc
                .get_all(self.facet_field)
                .flat_map(|x| x.as_facet())
                .map(|x| x.to_path_string())
                .filter(|x| x.starts_with("/l/"))
                .collect_vec();
            items.push(DocumentItem {
                field,
                uuid,
                labels,
            });
        }
        self.offset += Self::BATCH;
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            debug!("New batch created, took {v} ms");
        }
        Some(items)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::SystemTime;

    use nucliadb_core::protos::prost_types::Timestamp;
    use nucliadb_core::protos::resource::ResourceStatus;
    use nucliadb_core::protos::{
        Faceted, Filter, IndexMetadata, OrderBy, Resource, ResourceId, TextInformation, Timestamps,
    };
    use nucliadb_core::NodeResult;
    use tempfile::TempDir;

    use super::*;
    use crate::writer::TextWriterService;

    fn create_resource(shard_id: String) -> Resource {
        let resource_id = ResourceId {
            shard_id: shard_id.to_string(),
            uuid: "f56c58ac-b4f9-4d61-a077-ffccaadd0001".to_string(),
        };

        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();
        let timestamp = Timestamp {
            seconds: now.as_secs() as i64,
            nanos: 0,
        };

        let metadata = IndexMetadata {
            created: Some(timestamp.clone()),
            modified: Some(timestamp),
        };

        const DOC1_TI: &str = "This is the first document";
        const DOC1_P1: &str = "This is the text of the second paragraph.";
        const DOC1_P2: &str = "This should be enough to test the tantivy.";
        const DOC1_P3: &str = "But I wanted to make it three anyway.";

        let ti_title = TextInformation {
            text: DOC1_TI.to_string(),
            labels: vec!["/l/mylabel".to_string(), "/e/myentity".to_string()],
        };

        let ti_body = TextInformation {
            text: DOC1_P1.to_string() + DOC1_P2 + DOC1_P3,
            labels: vec!["/f/body".to_string(), "/l/mylabel2".to_string()],
        };

        let mut texts = HashMap::new();
        texts.insert("title".to_string(), ti_title);
        texts.insert("body".to_string(), ti_body);

        Resource {
            resource: Some(resource_id),
            metadata: Some(metadata),
            texts,
            status: ResourceStatus::Processed as i32,
            labels: vec![],
            paragraphs: HashMap::new(),
            paragraphs_to_delete: vec![],
            sentences_to_delete: vec![],
            relations_to_delete: vec![],
            relations: vec![],
            vectors: HashMap::default(),
            vectors_to_delete: HashMap::default(),
            shard_id,
        }
    }

    #[test]
    fn test_new_reader() -> NodeResult<()> {
        let dir = TempDir::new().unwrap();
        let fsc = TextConfig {
            path: dir.path().join("texts"),
        };

        let mut field_writer_service = TextWriterService::start(&fsc).unwrap();
        let resource1 = create_resource("shard1".to_string());
        let _ = field_writer_service.set_resource(&resource1);

        let field_reader_service = TextReaderService::start(&fsc).unwrap();

        let filter = Filter {
            tags: vec!["/l/mylabel2".to_string()],
        };

        let faceted = Faceted {
            tags: vec!["/".to_string(), "/l".to_string(), "/t".to_string()],
        };

        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();

        let timestamp = Timestamp {
            seconds: now.as_secs() as i64,
            nanos: 0,
        };

        let old_timestamp = Timestamp {
            seconds: 0_i64,
            nanos: 0,
        };

        let timestamps = Timestamps {
            from_modified: Some(old_timestamp.clone()),
            to_modified: Some(timestamp.clone()),
            from_created: Some(old_timestamp),
            to_created: Some(timestamp),
        };

        let order = OrderBy {
            sort_by: OrderField::Created as i32,
            r#type: 0,
            ..Default::default()
        };
        let search = DocumentSearchRequest {
            id: "shard1".to_string(),
            body: "\"enough test\"".to_string(),
            fields: vec!["body".to_string()],
            filter: Some(filter.clone()),
            faceted: Some(faceted.clone()),
            order: Some(order.clone()),
            page_number: 0,
            result_per_page: 20,
            timestamps: Some(timestamps.clone()),
            only_faceted: false,
            ..Default::default()
        };
        let result = field_reader_service.search(&search).unwrap();
        assert_eq!(result.total, 0);

        let search = DocumentSearchRequest {
            id: "shard1".to_string(),
            body: "enough test".to_string(),
            fields: vec!["body".to_string()],
            filter: Some(filter.clone()),
            faceted: Some(faceted.clone()),
            order: Some(order.clone()),
            page_number: 0,
            result_per_page: 20,
            timestamps: Some(timestamps.clone()),
            only_faceted: false,
            ..Default::default()
        };
        let result = field_reader_service.search(&search).unwrap();
        assert_eq!(result.total, 1);

        let search = DocumentSearchRequest {
            id: "shard1".to_string(),
            body: "enough - test".to_string(),
            fields: vec!["body".to_string()],
            filter: Some(filter.clone()),
            faceted: Some(faceted.clone()),
            order: Some(order.clone()),
            page_number: 0,
            result_per_page: 20,
            timestamps: Some(timestamps.clone()),
            only_faceted: false,
            ..Default::default()
        };
        let result = field_reader_service.search(&search).unwrap();
        assert_eq!(result.query, "\"enough - test\"");
        assert_eq!(result.total, 0);

        let search = DocumentSearchRequest {
            id: "shard1".to_string(),
            body: "enough - test\"".to_string(),
            fields: vec!["body".to_string()],
            filter: Some(filter.clone()),
            faceted: Some(faceted.clone()),
            order: Some(order.clone()),
            page_number: 0,
            result_per_page: 20,
            timestamps: Some(timestamps.clone()),
            only_faceted: false,
            ..Default::default()
        };
        let result = field_reader_service.search(&search).unwrap();
        assert_eq!(result.query, "\"enough - test\"");
        assert_eq!(result.total, 0);

        let search = DocumentSearchRequest {
            id: "shard1".to_string(),
            body: "".to_string(),
            fields: vec!["body".to_string()],
            filter: None,
            faceted: Some(faceted.clone()),
            order: Some(order.clone()),
            page_number: 0,
            result_per_page: 20,
            timestamps: Some(timestamps.clone()),
            only_faceted: false,
            ..Default::default()
        };

        let result = field_reader_service.search(&search).unwrap();
        assert_eq!(result.total, 1);

        let search = DocumentSearchRequest {
            id: "shard1".to_string(),
            body: "".to_string(),
            fields: vec!["body".to_string()],
            filter: Some(filter),
            faceted: Some(faceted),
            order: Some(order),
            page_number: 0,
            result_per_page: 20,
            timestamps: Some(timestamps),
            only_faceted: false,
            ..Default::default()
        };

        let result = field_reader_service.search(&search).unwrap();
        assert_eq!(result.total, 1);

        let request = StreamRequest {
            shard_id: None,
            filter: None,
            ..Default::default()
        };
        let iter = field_reader_service.iterator(&request).unwrap();
        let count = iter.count();
        assert_eq!(count, 2);

        Ok(())
    }
}
