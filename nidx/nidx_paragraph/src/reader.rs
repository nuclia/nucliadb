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
use std::path::Path;
use std::time::Instant;

use nidx_protos::order_by::{OrderField, OrderType};
use nidx_protos::{
    OrderBy, ParagraphItem, ParagraphSearchRequest, ParagraphSearchResponse, StreamRequest, SuggestRequest,
};
use search_query::{search_query, suggest_query};
use tantivy::collector::{Collector, Count, DocSetCollector, FacetCollector, TopDocs};
use tantivy::query::{AllQuery, Query, QueryParser};
use tantivy::{schema::*, DateTime, Order};
use tantivy::{DocAddress, Index, IndexReader};
use tracing::*;

use super::schema::ParagraphSchema;
use crate::search_query::{self, ParagraphsContext};
use crate::search_query::{ParagraphIterator, SharedTermC};
use crate::search_response::{extract_labels, SearchBm25Response, SearchFacetsResponse, SearchIntResponse};

const FUZZY_DISTANCE: u8 = 1;
const NUMBER_OF_RESULTS_SUGGEST: usize = 10;

pub struct ParagraphReaderService {
    index: Index,
    pub schema: ParagraphSchema,
    pub reader: IndexReader,
}

impl Debug for ParagraphReaderService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TextService").field("index", &self.index).field("schema", &self.schema).finish()
    }
}

impl ParagraphReaderService {
    fn count(&self) -> anyhow::Result<usize> {
        let searcher = self.reader.searcher();
        let count = searcher.search(&AllQuery, &Count).unwrap_or_default();
        Ok(count)
    }

    fn suggest(&self, request: &SuggestRequest) -> anyhow::Result<ParagraphSearchResponse> {
        let time = Instant::now();
        let id = Some(&request.shard);

        let v = time.elapsed().as_millis();
        debug!("{id:?} - Creating query: starts at {v} ms");

        let parser = QueryParser::for_index(&self.index, vec![self.schema.text]);
        let text = self.adapt_text(&parser, &request.body);
        let (original, termc, fuzzied) = suggest_query(&parser, &text, request, &self.schema, FUZZY_DISTANCE);
        let v = time.elapsed().as_millis();
        debug!("{id:?} - Creating query: ends at {v} ms");

        let v = time.elapsed().as_millis();
        debug!("{id:?} - Searching: starts at {v} ms");

        let searcher = self.reader.searcher();
        let topdocs = TopDocs::with_limit(NUMBER_OF_RESULTS_SUGGEST);
        let mut results = searcher.search(&original, &topdocs)?;
        let v = time.elapsed().as_millis();
        debug!("{id:?} - Searching: ends at {v} ms");

        if results.is_empty() {
            let v = time.elapsed().as_millis();
            debug!("{id:?} - Trying fuzzy: starts at {v} ms");

            let topdocs = TopDocs::with_limit(NUMBER_OF_RESULTS_SUGGEST);
            match searcher.search(&fuzzied, &topdocs) {
                Ok(mut fuzzied) => results.append(&mut fuzzied),
                Err(err) => error!("{err:?} during suggest"),
            }
            let v = time.elapsed().as_millis();
            debug!("{id:?} - Trying fuzzy: ends at {v} ms");
        }
        let v = time.elapsed().as_millis();
        debug!("{id:?} - Ending at: {v} ms");

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

    fn iterator(&self, request: &StreamRequest) -> anyhow::Result<ParagraphIterator> {
        let producer = BatchProducer {
            offset: 0,
            total: self.count()?,
            paragraph_field: self.schema.paragraph,
            facet_field: self.schema.facets,
            searcher: self.reader.searcher(),
            query: search_query::streaming_query(&self.schema, request),
        };
        Ok(ParagraphIterator::new(producer.flatten()))
    }

    fn search(
        &self,
        request: &ParagraphSearchRequest,
        context: &ParagraphsContext,
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

        let advanced = request.advanced_query.as_ref().map(|query| parser.parse_query(query)).transpose()?;
        #[rustfmt::skip] let (original, termc, fuzzied) = search_query(
            &parser,
            &text,
            request,
            &self.schema,
            context,
            FUZZY_DISTANCE,
            advanced
        );
        let searcher = Searcher {
            request,
            results,
            offset,
            schema: &self.schema,
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

    fn stored_ids(&self) -> anyhow::Result<Vec<String>> {
        let mut keys = vec![];
        let searcher = self.reader.searcher();
        for addr in searcher.search(&AllQuery, &DocSetCollector)? {
            let Some(key) = searcher
                .doc::<TantivyDocument>(addr)?
                .get_first(self.schema.uuid)
                .and_then(|i| i.as_str().map(String::from))
            else {
                continue;
            };
            keys.push(key);
        }

        Ok(keys)
    }
}

impl ParagraphReaderService {
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        if !path.exists() {
            return Err(anyhow::anyhow!("Invalid path {:?}", path));
        }
        let paragraph_schema = ParagraphSchema::default();
        let index = Index::open_in_dir(path)?;
        let reader = index.reader_builder().try_into()?;
        Ok(ParagraphReaderService {
            index,
            reader,
            schema: paragraph_schema,
        })
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
        for doc in top_docs.into_iter().flat_map(|i| self.searcher.doc::<TantivyDocument>(i.1)) {
            let id = doc
                .get_first(self.paragraph_field)
                .expect("document doesn't appear to have uuid.")
                .as_str()
                .unwrap()
                .to_string();

            let labels = extract_labels(doc.get_all(self.facet_field));
            items.push(ParagraphItem {
                id,
                labels,
            });
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
    schema: &'a ParagraphSchema,
    text: &'a str,
    only_faceted: bool,
}
impl<'a> Searcher<'a> {
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
        TopDocs::with_limit(limit).and_offset(offset).order_by_fast_field(order_field, order_direction)
    }
    fn do_search(
        &self,
        termc: SharedTermC,
        query: Box<dyn Query>,
        service: &ParagraphReaderService,
        min_score: f32,
    ) -> anyhow::Result<ParagraphSearchResponse> {
        let searcher = service.reader.searcher();
        let facet_collector = self.facets.iter().fold(FacetCollector::for_field("facets"), |mut collector, facet| {
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

#[cfg(test)]
mod tests {
    use nidx_protos::prost_types::Timestamp;
    use nidx_protos::resource::ResourceStatus;
    use nidx_protos::{
        Faceted, IndexMetadata, IndexParagraph, IndexParagraphs, OrderBy, Resource, ResourceId, TextInformation,
        Timestamps,
    };
    use nidx_types::query_language::{self, BooleanExpression};
    use std::collections::HashMap;
    use std::time::SystemTime;
    use tantivy::collector::Count;
    use tantivy::query::AllQuery;
    use tempfile::TempDir;

    use super::*;
    use crate::writer::ParagraphWriterService;

    const DOC1_TI: &str = "This is the first document";
    const DOC1_P1: &str = "This is the text of the second paragraph.";
    const DOC1_P2: &str = "This should be enough to test the tantivy.";
    const DOC1_P3: &str = "But I wanted to make it three anyway.";

    fn create_resource(shard_id: String, timestamp: Timestamp) -> Resource {
        const UUID: &str = "f56c58ac-b4f9-4d61-a077-ffccaadd0001";
        let resource_id = ResourceId {
            shard_id: shard_id.to_string(),
            uuid: UUID.to_string(),
        };

        let metadata = IndexMetadata {
            created: Some(timestamp.clone()),
            modified: Some(timestamp),
        };

        let ti_title = TextInformation {
            text: DOC1_TI.to_string(),
            labels: vec!["/e/mylabel".to_string()],
        };

        let ti_body = TextInformation {
            text: DOC1_P1.to_string() + DOC1_P2 + DOC1_P3,
            labels: vec!["/f/body".to_string(), "/l/mylabel2".to_string()],
        };

        let mut texts = HashMap::new();
        texts.insert("title".to_string(), ti_title);
        texts.insert("body".to_string(), ti_body);

        let p1 = IndexParagraph {
            start: 0,
            end: DOC1_P1.len() as i32,
            sentences: HashMap::new(),
            vectorsets_sentences: HashMap::new(),
            field: "body".to_string(),
            labels: vec!["/e/myentity".to_string()],
            index: 0,
            split: "".to_string(),
            repeated_in_field: false,
            metadata: None,
        };
        let p1_uuid = format!("{}/{}/{}-{}", UUID, "body", 0, DOC1_P1.len());

        let p2 = IndexParagraph {
            start: DOC1_P1.len() as i32,
            end: (DOC1_P1.len() + DOC1_P2.len()) as i32,
            sentences: HashMap::new(),
            vectorsets_sentences: HashMap::new(),
            field: "body".to_string(),
            labels: vec!["/tantivy".to_string(), "/test".to_string(), "/label1".to_string()],
            index: 1,
            split: "".to_string(),
            repeated_in_field: false,
            metadata: None,
        };
        let p2_uuid = format!("{}/{}/{}-{}", UUID, "body", DOC1_P1.len(), DOC1_P1.len() + DOC1_P2.len());

        let p3 = IndexParagraph {
            start: (DOC1_P1.len() + DOC1_P2.len()) as i32,
            end: (DOC1_P1.len() + DOC1_P2.len() + DOC1_P3.len()) as i32,
            sentences: HashMap::new(),
            vectorsets_sentences: HashMap::new(),
            field: "body".to_string(),
            labels: vec!["/three".to_string(), "/label2".to_string()],
            index: 2,
            split: "".to_string(),
            repeated_in_field: false,
            metadata: None,
        };
        let p3_uuid = format!(
            "{}/{}/{}-{}",
            UUID,
            "body",
            DOC1_P1.len() + DOC1_P2.len(),
            DOC1_P1.len() + DOC1_P2.len() + DOC1_P3.len()
        );

        let body_paragraphs = IndexParagraphs {
            paragraphs: [(p1_uuid, p1), (p2_uuid, p2), (p3_uuid, p3)].into_iter().collect(),
        };

        let p4 = IndexParagraph {
            start: 0,
            end: DOC1_TI.len() as i32,
            sentences: HashMap::new(),
            vectorsets_sentences: HashMap::new(),
            field: "title".to_string(),
            labels: vec!["/c/ool".to_string()],
            index: 3,
            split: "".to_string(),
            repeated_in_field: false,
            metadata: None,
        };
        let p4_uuid = format!("{}/{}/{}-{}", UUID, "body", 0, DOC1_TI.len());

        let title_paragraphs = IndexParagraphs {
            paragraphs: [(p4_uuid, p4)].into_iter().collect(),
        };

        let paragraphs =
            [("body".to_string(), body_paragraphs), ("title".to_string(), title_paragraphs)].into_iter().collect();

        Resource {
            resource: Some(resource_id),
            metadata: Some(metadata),
            texts,
            status: ResourceStatus::Processed as i32,
            labels: vec!["/l/mylabel_resource".to_string()],
            paragraphs,
            paragraphs_to_delete: vec![],
            sentences_to_delete: vec![],
            relations: vec![],
            vectors: HashMap::default(),
            vectors_to_delete: HashMap::default(),
            shard_id,
            ..Default::default()
        }
    }
    #[test]
    fn test_total_number_of_results() -> anyhow::Result<()> {
        const UUID: &str = "f56c58ac-b4f9-4d61-a077-ffccaadd0001";
        let dir = TempDir::new().unwrap();
        let shard_path = dir.path().join("paragraphs");

        let seconds = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).map(|t| t.as_secs() as i64).unwrap();
        let timestamp = Timestamp {
            seconds,
            nanos: 0,
        };

        let mut paragraph_writer_service = ParagraphWriterService::create(&shard_path).unwrap();
        let resource1 = create_resource("shard1".to_string(), timestamp);
        paragraph_writer_service.set_resource(&resource1)?;
        let paragraph_reader_service = ParagraphReaderService::open(&shard_path).unwrap();
        let context = ParagraphsContext::default();

        // Search on all paragraphs faceted
        let search = ParagraphSearchRequest {
            id: "shard1".to_string(),
            uuid: UUID.to_string(),
            body: "".to_string(),
            fields: vec!["body".to_string(), "title".to_string()],
            filter: None,
            faceted: None,
            order: None,
            page_number: 0,
            result_per_page: 20,
            timestamps: None,
            with_duplicates: false,
            only_faceted: false,
            ..Default::default()
        };
        let result = paragraph_reader_service.search(&search, &context).unwrap();
        assert_eq!(result.total, 4);
        assert_eq!(result.results.len(), 4);

        let search = ParagraphSearchRequest {
            id: "shard1".to_string(),
            uuid: UUID.to_string(),
            body: "".to_string(),
            fields: vec!["body".to_string(), "title".to_string()],
            filter: None,
            faceted: None,
            order: None,
            page_number: 0,
            result_per_page: 0,
            timestamps: None,
            with_duplicates: false,
            only_faceted: false,
            ..Default::default()
        };
        let result = paragraph_reader_service.search(&search, &context).unwrap();
        assert!(result.results.is_empty());
        assert_eq!(result.total, 4);

        Ok(())
    }

    #[test]
    fn test_filtered_search() -> anyhow::Result<()> {
        const UUID: &str = "f56c58ac-b4f9-4d61-a077-ffccaadd0001";
        let dir = TempDir::new().unwrap();
        let shard_path = dir.path().join("paragraphs");

        let seconds = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).map(|t| t.as_secs() as i64).unwrap();
        let timestamp = Timestamp {
            seconds,
            nanos: 0,
        };

        let mut paragraph_writer_service = ParagraphWriterService::create(&shard_path).unwrap();
        let resource1 = create_resource("shard1".to_string(), timestamp);
        paragraph_writer_service.set_resource(&resource1)?;

        let paragraph_reader_service = ParagraphReaderService::open(&shard_path).unwrap();

        let reader = paragraph_writer_service.index.reader()?;
        let searcher = reader.searcher();
        let mut context = ParagraphsContext::default();
        let (_top_docs, count) = searcher.search(&AllQuery, &(TopDocs::with_limit(10), Count))?;
        assert_eq!(count, 4);

        // Only one paragraph  matches
        let search = ParagraphSearchRequest {
            id: "shard1".to_string(),
            uuid: UUID.to_string(),
            body: "".to_string(),
            fields: vec!["body".to_string(), "title".to_string()],
            filter: None,
            faceted: None,
            order: None,
            page_number: 0,
            result_per_page: 20,
            timestamps: None,
            with_duplicates: false,
            only_faceted: false,
            ..Default::default()
        };
        context.filtering_formula = Some(BooleanExpression::Literal("/tantivy".to_string()));
        let result = paragraph_reader_service.search(&search, &context).unwrap();
        assert_eq!(result.total, 1);

        // Two matches due to OR
        let expression = query_language::BooleanOperation {
            operator: query_language::Operator::Or,
            operands: vec![
                BooleanExpression::Literal("/tantivy".to_string()),
                BooleanExpression::Literal("/label2".to_string()),
            ],
        };
        context.filtering_formula = Some(BooleanExpression::Operation(expression));
        let result = paragraph_reader_service.search(&search, &context).unwrap();
        assert_eq!(result.total, 2);

        // No matches due to AND
        let expression = query_language::BooleanOperation {
            operator: query_language::Operator::And,
            operands: vec![
                BooleanExpression::Literal("/tantivy".to_string()),
                BooleanExpression::Literal("/label2".to_string()),
            ],
        };
        context.filtering_formula = Some(BooleanExpression::Operation(expression));
        let result = paragraph_reader_service.search(&search, &context).unwrap();
        assert_eq!(result.total, 0);

        Ok(())
    }

    #[test]
    fn test_new_paragraph() -> anyhow::Result<()> {
        const UUID: &str = "f56c58ac-b4f9-4d61-a077-ffccaadd0001";
        let dir = TempDir::new().unwrap();
        let shard_path = dir.path().join("paragraphs");

        let seconds = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).map(|t| t.as_secs() as i64).unwrap();
        let timestamp = Timestamp {
            seconds,
            nanos: 0,
        };
        let mut paragraph_writer_service = ParagraphWriterService::create(&shard_path).unwrap();
        let resource1 = create_resource("shard1".to_string(), timestamp);
        paragraph_writer_service.set_resource(&resource1)?;
        let paragraph_reader_service = ParagraphReaderService::open(&shard_path).unwrap();
        let reader = paragraph_writer_service.index.reader()?;
        let searcher = reader.searcher();
        let empty_context = ParagraphsContext::default();
        let (_top_docs, count) = searcher.search(&AllQuery, &(TopDocs::with_limit(10), Count))?;
        assert_eq!(count, 4);

        let faceted = Faceted {
            labels: vec!["".to_string(), "/l".to_string(), "/e".to_string(), "/c".to_string()],
        };

        let order = OrderBy {
            sort_by: OrderField::Created as i32,
            r#type: 0,
            ..Default::default()
        };

        // Search on all paragraphs faceted
        let search = ParagraphSearchRequest {
            id: "shard1".to_string(),
            uuid: UUID.to_string(),
            body: "".to_string(),
            fields: vec!["body".to_string(), "title".to_string()],
            filter: None,
            faceted: None,
            order: None,
            page_number: 0,
            result_per_page: 20,
            timestamps: None,
            with_duplicates: false,
            only_faceted: false,
            ..Default::default()
        };
        let result = paragraph_reader_service.search(&search, &empty_context).unwrap();
        assert_eq!(result.total, 4);

        // Search on all paragraphs without fields
        let search = ParagraphSearchRequest {
            id: "shard1".to_string(),
            uuid: UUID.to_string(),
            body: "".to_string(),
            fields: vec![],
            filter: None,
            faceted: None,
            order: None,
            page_number: 0,
            result_per_page: 20,
            timestamps: None,
            with_duplicates: false,
            only_faceted: false,
            ..Default::default()
        };
        let result = paragraph_reader_service.search(&search, &empty_context).unwrap();
        assert_eq!(result.total, 4);

        // Search and filter by min_score
        let search = ParagraphSearchRequest {
            id: "shard1".to_string(),
            uuid: UUID.to_string(),
            body: "should enough".to_string(),
            fields: vec![],
            filter: None,
            faceted: None,
            order: None,
            page_number: 0,
            result_per_page: 20,
            timestamps: None,
            with_duplicates: false,
            only_faceted: false,
            min_score: 30.0,
            ..Default::default()
        };
        let result = paragraph_reader_service.search(&search, &empty_context).unwrap();
        assert_eq!(result.results.len(), 0);

        // Search on all paragraphs in resource with typo
        let search = ParagraphSearchRequest {
            id: "shard1".to_string(),
            uuid: UUID.to_string(),
            body: "shoupd enaugh".to_string(),
            fields: vec![],
            filter: None,
            faceted: None,
            order: None,
            page_number: 0,
            result_per_page: 20,
            timestamps: None,
            with_duplicates: false,
            only_faceted: false,
            ..Default::default()
        };
        let result = paragraph_reader_service.search(&search, &empty_context).unwrap();
        assert_eq!(result.total, 1);

        // Search on all paragraphs in resource with typo
        let search = ParagraphSearchRequest {
            id: "shard1".to_string(),
            uuid: UUID.to_string(),
            body: "\"should\" enaugh".to_string(),
            fields: vec![],
            filter: None,
            faceted: None,
            order: None,
            page_number: 0,
            result_per_page: 20,
            timestamps: None,
            with_duplicates: false,
            only_faceted: false,
            ..Default::default()
        };
        let result = paragraph_reader_service.search(&search, &empty_context).unwrap();
        assert_eq!(result.total, 1);

        // Search typo on all paragraph
        let search = ParagraphSearchRequest {
            id: "shard1".to_string(),
            uuid: "".to_string(),
            body: "shoupd enaugh".to_string(),
            fields: vec![],
            filter: None,
            faceted: None,
            order: None,
            page_number: 0,
            result_per_page: 20,
            timestamps: None,
            with_duplicates: false,
            only_faceted: false,
            ..Default::default()
        };
        let result = paragraph_reader_service.search(&search, &empty_context).unwrap();
        assert_eq!(result.total, 1);

        // Search with invalid and unbalanced grammar
        let search = ParagraphSearchRequest {
            id: "shard1".to_string(),
            uuid: "".to_string(),
            body: "shoupd + enaugh\"".to_string(),
            fields: vec![],
            filter: None,
            faceted: None,
            order: None,
            page_number: 0,
            result_per_page: 20,
            timestamps: None,
            with_duplicates: false,
            only_faceted: false,
            ..Default::default()
        };
        let result = paragraph_reader_service.search(&search, &empty_context).unwrap();
        assert_eq!(result.query, "\"shoupd + enaugh\"");
        assert_eq!(result.total, 0);

        // Search with invalid grammar
        let search = ParagraphSearchRequest {
            id: "shard1".to_string(),
            uuid: "".to_string(),
            body: "shoupd + enaugh".to_string(),
            fields: vec![],
            filter: None,
            faceted: None,
            order: None,
            page_number: 0,
            result_per_page: 20,
            timestamps: None,
            with_duplicates: false,
            only_faceted: false,
            ..Default::default()
        };
        let result = paragraph_reader_service.search(&search, &empty_context).unwrap();
        assert_eq!(result.query, "\"shoupd + enaugh\"");
        assert_eq!(result.total, 0);

        // Empty search
        let search = ParagraphSearchRequest {
            id: "shard1".to_string(),
            uuid: "".to_string(),
            body: "".to_string(),
            fields: vec![],
            filter: None,
            faceted: None,
            order: None,
            page_number: 0,
            result_per_page: 20,
            timestamps: None,
            with_duplicates: true,
            only_faceted: false,
            ..Default::default()
        };
        let result = paragraph_reader_service.search(&search, &empty_context).unwrap();
        assert_eq!(result.total, 4);

        // Search filter all paragraphs
        let search = ParagraphSearchRequest {
            id: "shard1".to_string(),
            uuid: "".to_string(),
            body: "this is the".to_string(),
            fields: vec![],
            filter: None,
            faceted: Some(faceted.clone()),
            order: Some(order),
            page_number: 0,
            result_per_page: 20,
            timestamps: None,
            with_duplicates: false,
            only_faceted: false,
            ..Default::default()
        };
        let result = paragraph_reader_service.search(&search, &empty_context).unwrap();
        assert_eq!(result.total, 3);

        // Search typo on all paragraph
        let search = ParagraphSearchRequest {
            id: "shard1".to_string(),
            uuid: "".to_string(),
            body: "\"shoupd\"".to_string(),
            fields: vec![],
            filter: None,
            faceted: None,
            order: None,
            page_number: 0,
            result_per_page: 20,
            timestamps: None,
            with_duplicates: false,
            only_faceted: false,
            ..Default::default()
        };
        let result = paragraph_reader_service.search(&search, &empty_context).unwrap();
        assert_eq!(result.total, 0);

        let request = StreamRequest {
            shard_id: None,
            filter: None,
            ..Default::default()
        };
        let iter = paragraph_reader_service.iterator(&request).unwrap();
        let count = iter.count();
        assert_eq!(count, 4);
        Ok(())
    }

    #[test]
    fn test_search_paragraph_with_timestamps() -> anyhow::Result<()> {
        let dir = TempDir::new().unwrap();
        let shard_path = dir.path().join("paragraphs");

        let time_baseline = Timestamp {
            seconds: 2,
            nanos: 0,
        };

        let mut paragraph_writer_service = ParagraphWriterService::create(&shard_path).unwrap();
        let resource1 = create_resource("shard1".to_string(), time_baseline.clone());

        paragraph_writer_service.set_resource(&resource1)?;

        let paragraph_reader_service = ParagraphReaderService::open(&shard_path).unwrap();

        let reader = paragraph_writer_service.index.reader()?;
        let searcher = reader.searcher();
        let (_top_docs, count) = searcher.search(&AllQuery, &(TopDocs::with_limit(10), Count))?;
        assert_eq!(count, 4);

        fn do_search(paragraph_reader_service: &ParagraphReaderService, timestamps: Timestamps) -> i32 {
            let search = ParagraphSearchRequest {
                id: "shard1".to_string(),
                uuid: "".to_string(),
                body: "this is the".to_string(),
                fields: vec![],
                filter: None,
                faceted: None,
                order: None, // Some(order),
                page_number: 0,
                result_per_page: 20,
                timestamps: Some(timestamps),
                with_duplicates: false,
                only_faceted: false,
                ..Default::default()
            };
            let context = ParagraphsContext::default();
            let result = paragraph_reader_service.search(&search, &context).unwrap();
            result.total
        }

        let total = do_search(
            &paragraph_reader_service,
            Timestamps {
                from_modified: Some(Timestamp {
                    seconds: time_baseline.seconds - 1,
                    nanos: time_baseline.nanos,
                }),
                to_modified: Some(Timestamp {
                    seconds: time_baseline.seconds + 1,
                    nanos: time_baseline.nanos,
                }),
                from_created: Some(Timestamp {
                    seconds: time_baseline.seconds - 1,
                    nanos: time_baseline.nanos,
                }),
                to_created: Some(Timestamp {
                    seconds: time_baseline.seconds + 1,
                    nanos: time_baseline.nanos,
                }),
            },
        );
        assert_eq!(total, 3);

        // only from modified before, all matches
        let total = do_search(
            &paragraph_reader_service,
            Timestamps {
                from_modified: Some(Timestamp {
                    seconds: time_baseline.seconds - 1,
                    nanos: time_baseline.nanos,
                }),
                to_modified: None,
                from_created: None,
                to_created: None,
            },
        );
        assert_eq!(total, 3);

        // only from modified after, no matches
        let total = do_search(
            &paragraph_reader_service,
            Timestamps {
                from_modified: Some(Timestamp {
                    seconds: time_baseline.seconds + 1,
                    nanos: time_baseline.nanos,
                }),
                to_modified: None,
                from_created: None,
                to_created: None,
            },
        );
        assert_eq!(total, 0);

        // only to modified after, all matches
        let total = do_search(
            &paragraph_reader_service,
            Timestamps {
                from_modified: None,
                to_modified: Some(Timestamp {
                    seconds: time_baseline.seconds + 1,
                    nanos: time_baseline.nanos,
                }),
                from_created: None,
                to_created: None,
            },
        );
        assert_eq!(total, 3);

        // only to modified before, no matches
        let total = do_search(
            &paragraph_reader_service,
            Timestamps {
                from_modified: None,
                to_modified: Some(Timestamp {
                    seconds: time_baseline.seconds - 1,
                    nanos: time_baseline.nanos,
                }),
                from_created: None,
                to_created: None,
            },
        );
        assert_eq!(total, 0);

        // only from created before, all matches
        let total = do_search(
            &paragraph_reader_service,
            Timestamps {
                from_modified: None,
                to_modified: None,
                from_created: Some(Timestamp {
                    seconds: time_baseline.seconds - 1,
                    nanos: time_baseline.nanos,
                }),
                to_created: None,
            },
        );
        assert_eq!(total, 3);

        // only from created after, no matches
        let total = do_search(
            &paragraph_reader_service,
            Timestamps {
                from_modified: None,
                to_modified: None,
                from_created: Some(Timestamp {
                    seconds: time_baseline.seconds + 1,
                    nanos: time_baseline.nanos,
                }),
                to_created: None,
            },
        );
        assert_eq!(total, 0);

        // only to created after, all matches
        let total = do_search(
            &paragraph_reader_service,
            Timestamps {
                from_modified: None,
                to_modified: None,
                from_created: None,
                to_created: Some(Timestamp {
                    seconds: time_baseline.seconds + 1,
                    nanos: time_baseline.nanos,
                }),
            },
        );
        assert_eq!(total, 3);

        // only to created before, no matches
        let total = do_search(
            &paragraph_reader_service,
            Timestamps {
                from_modified: None,
                to_modified: None,
                from_created: None,
                to_created: Some(Timestamp {
                    seconds: time_baseline.seconds - 1,
                    nanos: time_baseline.nanos,
                }),
            },
        );
        assert_eq!(total, 0);

        Ok(())
    }
}
