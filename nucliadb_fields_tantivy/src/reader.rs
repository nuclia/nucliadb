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
use std::fmt::{Debug, Display};
use std::fs;

use nucliadb_protos::{
    DocumentResult, DocumentSearchRequest, DocumentSearchResponse, FacetResult, FacetResults,
    OrderBy, ResourceId, ResultScore,
};
use nucliadb_service_interface::prelude::*;
use tantivy::collector::{
    Count, DocSetCollector, FacetCollector, FacetCounts, MultiCollector, TopDocs,
};
use tantivy::query::{AllQuery, Query, QueryParser, TermQuery};
use tantivy::schema::*;
use tantivy::{
    DocAddress, Index, IndexReader, IndexSettings, IndexSortByField, Order, ReloadPolicy, Searcher,
    TantivyError,
};
use tracing::*;

use super::schema::FieldSchema;

#[derive(Debug)]
struct TError(TantivyError);
impl InternalError for TError {}
impl Display for TError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub struct SearchResponse<'a, S> {
    pub query: &'a str,
    pub facets_count: FacetCounts,
    pub facets: Vec<String>,
    pub top_docs: Vec<(S, DocAddress)>,
    pub order_by: Option<OrderBy>,
    pub page_number: i32,
    pub results_per_page: i32,
}

pub struct FieldReaderService {
    index: Index,
    pub schema: FieldSchema,
    pub reader: IndexReader,
}

impl Debug for FieldReaderService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FieldReaderService")
            .field("index", &self.index)
            .field("schema", &self.schema)
            .finish()
    }
}

impl FieldReader for FieldReaderService {}

impl ReaderChild for FieldReaderService {
    type Request = DocumentSearchRequest;
    type Response = DocumentSearchResponse;

    fn stop(&self) -> InternalResult<()> {
        info!("Stopping Reader Text Service");
        Ok(())
    }

    fn count(&self) -> usize {
        let searcher = self.reader.searcher();
        searcher.search(&AllQuery, &Count).unwrap()
    }
    fn search(&self, request: &Self::Request) -> InternalResult<Self::Response> {
        info!("Document search at {}:{}", line!(), file!());
        let body = &request.body;
        let results = request.result_per_page;
        let only_facets = results == 0 || (body.is_empty() && request.filter.is_none());
        Ok(self.do_search(request, only_facets))
    }

    fn reload(&self) {
        self.reader.reload().unwrap();
    }
    fn stored_ids(&self) -> Vec<String> {
        self.keys()
    }
}

impl FieldReaderService {
    pub fn find_one(&self, resource_id: &ResourceId) -> tantivy::Result<Option<Document>> {
        let uuid_term = Term::from_field_text(self.schema.uuid, &resource_id.uuid);
        let uuid_query = TermQuery::new(uuid_term, IndexRecordOption::Basic);

        let searcher = self.reader.searcher();

        let top_docs = searcher.search(&uuid_query, &TopDocs::with_limit(1))?;
        match top_docs
            .first()
            .map(|(_, doc_address)| searcher.doc(*doc_address))
        {
            Some(Ok(a)) => Ok(Some(a)),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }

    pub fn find_resource(&self, resource_id: &ResourceId) -> tantivy::Result<Vec<Document>> {
        let uuid_field = self.schema.uuid;
        let uuid_term = Term::from_field_text(uuid_field, &resource_id.uuid);
        let uuid_query = TermQuery::new(uuid_term, IndexRecordOption::Basic);

        let searcher = self.reader.searcher();

        let top_docs = searcher.search(&uuid_query, &TopDocs::with_limit(1000))?;
        let mut docs = Vec::with_capacity(1000);

        for (_score, doc_address) in top_docs {
            let doc = searcher.doc(doc_address)?;
            docs.push(doc);
        }

        Ok(docs)
    }

    pub fn start(config: &FieldConfig) -> InternalResult<Self> {
        info!("Starting Text Service");
        match FieldReaderService::open(config) {
            Ok(service) => Ok(service),
            Err(_e) => {
                warn!("Text Service does not exists. Creating a new one.");
                match FieldReaderService::new(config) {
                    Ok(service) => Ok(service),
                    Err(e) => {
                        error!("Error starting Text service: {}", e);
                        Err(Box::new(FieldError { msg: e.to_string() }))
                    }
                }
            }
        }
    }
    pub fn new(config: &FieldConfig) -> InternalResult<Self> {
        match FieldReaderService::new_inner(config) {
            Ok(service) => Ok(service),
            Err(e) => Err(Box::new(FieldError { msg: e.to_string() })),
        }
    }
    pub fn open(config: &FieldConfig) -> InternalResult<Self> {
        match FieldReaderService::open_inner(config) {
            Ok(service) => Ok(service),
            Err(e) => Err(Box::new(FieldError { msg: e.to_string() })),
        }
    }
    pub fn new_inner(config: &FieldConfig) -> tantivy::Result<FieldReaderService> {
        let field_schema = FieldSchema::new();

        fs::create_dir_all(&config.path)?;

        let mut index_builder = Index::builder().schema(field_schema.schema.clone());
        let settings = IndexSettings {
            sort_by_field: Some(IndexSortByField {
                field: "created".to_string(),
                order: Order::Desc,
            }),
            ..Default::default()
        };

        index_builder = index_builder.settings(settings);

        let index = index_builder.create_in_dir(&config.path).unwrap();

        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommit)
            .try_into()?;

        Ok(FieldReaderService {
            index,
            reader,
            schema: field_schema,
        })
    }

    pub fn open_inner(config: &FieldConfig) -> tantivy::Result<FieldReaderService> {
        let field_schema = FieldSchema::new();
        let index = Index::open_in_dir(&config.path)?;

        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommit)
            .try_into()?;

        Ok(FieldReaderService {
            index,
            reader,
            schema: field_schema,
        })
    }

    fn get_order_field(&self, order: &Option<OrderBy>) -> Option<Field> {
        match order {
            Some(order) => match order.field.as_str() {
                "created" => Some(self.schema.created),
                "modified" => Some(self.schema.modified),
                _ => {
                    error!("Order by {} is not currently supported.", order.field);
                    None
                }
            },
            None => None,
        }
    }

    fn convert_int_order(
        &self,
        response: SearchResponse<u64>,
        searcher: &Searcher,
    ) -> DocumentSearchResponse {
        info!("Document query at {}:{}", line!(), file!());
        let mut total = response.top_docs.len();
        let next_page: bool;
        if total > response.results_per_page as usize {
            next_page = true;
            total = response.results_per_page as usize;
        } else {
            next_page = false;
        }
        let mut results = Vec::with_capacity(total);
        info!("Document query at {}:{}", line!(), file!());
        for (id, (_, doc_address)) in response.top_docs.into_iter().enumerate() {
            match searcher.doc(doc_address) {
                Ok(doc) => {
                    let score = Some(ResultScore {
                        bm25: 0.0,
                        booster: id as f32,
                    });
                    info!("Document query at {}:{}", line!(), file!());
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

                    let result = DocumentResult { uuid, field, score };
                    info!("Document query at {}:{}", line!(), file!());
                    results.push(result);
                }
                Err(e) => error!("Error retrieving document from index: {}", e),
            }
        }

        let facets = self.create_facets(response.facets, response.facets_count);
        info!("Document query at {}:{}", line!(), file!());
        DocumentSearchResponse {
            total: total as i32,
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
        info!("Document query at {}:{}", line!(), file!());
        let mut total = response.top_docs.len();
        let next_page: bool;
        if total > response.results_per_page as usize {
            next_page = true;
            total = response.results_per_page as usize;
        } else {
            next_page = false;
        }
        let mut results = Vec::with_capacity(total);
        info!("Document query at {}:{}", line!(), file!());
        for (id, (score, doc_address)) in response.top_docs.into_iter().take(total).enumerate() {
            match searcher.doc(doc_address) {
                Ok(doc) => {
                    let score = Some(ResultScore {
                        bm25: score,
                        booster: id as f32,
                    });
                    info!("Document query at {}:{}", line!(), file!());
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

                    let result = DocumentResult { uuid, field, score };
                    info!("Document query at {}:{}", line!(), file!());
                    results.push(result);
                }
                Err(e) => error!("Error retrieving document from index: {}", e),
            }
        }

        let facets = self.create_facets(response.facets, response.facets_count);
        info!("Document query at {}:{}", line!(), file!());
        DocumentSearchResponse {
            total: total as i32,
            results,
            facets,
            page_number: response.page_number,
            result_per_page: response.results_per_page,
            query: response.query.to_string(),
            next_page,
            bm25: true,
        }
    }

    fn facet_count(&self, facet: &str, facets_count: &FacetCounts) -> Vec<FacetResult> {
        facets_count
            .top_k(facet, 50)
            .into_iter()
            .map(|(facet, count)| FacetResult {
                tag: facet.to_string(),
                total: count as i32,
            })
            .collect()
    }

    fn create_facets(
        &self,
        facets: Vec<String>,
        facets_count: FacetCounts,
    ) -> HashMap<String, FacetResults> {
        facets
            .into_iter()
            .map(|facet| (&facets_count, facet))
            .map(|(facets_count, facet)| (self.facet_count(&facet, facets_count), facet))
            .filter(|(r, _)| !r.is_empty())
            .map(|(facetresults, facet)| (facet, FacetResults { facetresults }))
            .collect()
    }

    fn adapt_text(parser: &QueryParser, text: &str) -> String {
        match text {
            "" => text.to_string(),
            text => parser
                .parse_query(text)
                .map(|_| text.to_string())
                .unwrap_or_else(|e| {
                    tracing::error!("Error during parsing query: {e}. Input query: {text}");
                    format!("\"{}\"", text.replace('"', ""))
                }),
        }
    }

    fn do_search(
        &self,
        request: &DocumentSearchRequest,
        only_facets: bool,
    ) -> DocumentSearchResponse {
        use crate::search_query::create_query;
        let query_parser = {
            let mut query_parser = QueryParser::for_index(&self.index, vec![self.schema.text]);
            query_parser.set_conjunction_by_default();
            query_parser
        };
        let text = FieldReaderService::adapt_text(&query_parser, &request.body);
        let query = if !request.body.is_empty() {
            create_query(&query_parser, request, &self.schema, &text)
        } else {
            Box::new(AllQuery) as Box<dyn Query>
        };

        // Offset to search from
        let results = request.result_per_page as usize;
        let offset = results * request.page_number as usize;
        let extra_result = results + 1;
        let order_field = self.get_order_field(&request.order);
        let facets = request
            .faceted
            .as_ref()
            .map(|v| {
                v.tags
                    .iter()
                    .filter(|s| FieldReaderService::is_valid_facet(s))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        let mut facet_collector = FacetCollector::for_field(self.schema.facets);
        for facet in &facets {
            match Facet::from_text(facet) {
                Ok(facet) => facet_collector.add_facet(facet),
                Err(_) => error!("Invalid facet: {}", facet),
            }
        }
        let searcher = self.reader.searcher();
        match order_field {
            _ if only_facets => {
                // Just a facet search
                let facets_count = searcher.search(&query, &facet_collector).unwrap();
                self.convert_bm25_order(
                    SearchResponse {
                        facets,
                        query: &text,
                        top_docs: vec![],
                        facets_count,
                        order_by: request.order.clone(),
                        page_number: request.page_number,
                        results_per_page: results as i32,
                    },
                    &searcher,
                )
            }
            Some(order_field) => {
                let mut multicollector = MultiCollector::new();
                let facet_handler = multicollector.add_collector(facet_collector);
                let topdocs_collector = TopDocs::with_limit(extra_result)
                    .and_offset(offset)
                    .order_by_u64_field(order_field);
                let topdocs_handler = multicollector.add_collector(topdocs_collector);
                let mut multi_fruit = searcher.search(&query, &multicollector).unwrap();
                let facets_count = facet_handler.extract(&mut multi_fruit);
                let top_docs = topdocs_handler.extract(&mut multi_fruit);
                self.convert_int_order(
                    SearchResponse {
                        facets_count,
                        facets,
                        top_docs,
                        query: &text,
                        order_by: request.order.clone(),
                        page_number: request.page_number,
                        results_per_page: results as i32,
                    },
                    &searcher,
                )
            }
            None => {
                let mut multicollector = MultiCollector::new();
                let facet_handler = multicollector.add_collector(facet_collector);
                let topdocs_collector = TopDocs::with_limit(extra_result).and_offset(offset);
                let topdocs_handler = multicollector.add_collector(topdocs_collector);
                let mut multi_fruit = searcher.search(&query, &multicollector).unwrap();
                let facets_count = facet_handler.extract(&mut multi_fruit);
                let top_docs = topdocs_handler.extract(&mut multi_fruit);
                self.convert_bm25_order(
                    SearchResponse {
                        facets_count,
                        facets,
                        top_docs,
                        query: &text,
                        order_by: request.order.clone(),
                        page_number: request.page_number,
                        results_per_page: results as i32,
                    },
                    &searcher,
                )
            }
        }
    }
    fn keys(&self) -> Vec<String> {
        let searcher = self.reader.searcher();
        searcher
            .search(&AllQuery, &DocSetCollector)
            .unwrap()
            .into_iter()
            .map(|addr| {
                searcher
                    .doc(addr)
                    .unwrap()
                    .get_first(self.schema.uuid)
                    .expect("document doesn't appear to have uuid.")
                    .as_text()
                    .unwrap()
                    .to_string()
            })
            .collect()
    }
    fn is_valid_facet(maybe_facet: &str) -> bool {
        Facet::from_text(maybe_facet)
            .map_err(|_| error!("Invalid facet: {maybe_facet}"))
            .is_ok()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::SystemTime;

    use nucliadb_protos::{Faceted, Filter, OrderBy, Resource, ResourceId, Timestamps};
    use prost_types::Timestamp;
    use tempdir::TempDir;

    use super::*;
    use crate::writer::FieldWriterService;

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

        let metadata = nucliadb_protos::IndexMetadata {
            created: Some(timestamp.clone()),
            modified: Some(timestamp),
        };

        const DOC1_TI: &str = "This is the first document";
        const DOC1_P1: &str = "This is the text of the second paragraph.";
        const DOC1_P2: &str = "This should be enough to test the tantivy.";
        const DOC1_P3: &str = "But I wanted to make it three anyway.";

        let ti_title = nucliadb_protos::TextInformation {
            text: DOC1_TI.to_string(),
            labels: vec!["/l/mylabel".to_string(), "/e/myentity".to_string()],
        };

        let ti_body = nucliadb_protos::TextInformation {
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
            status: nucliadb_protos::resource::ResourceStatus::Processed as i32,
            labels: vec![],
            paragraphs: HashMap::new(),
            paragraphs_to_delete: vec![],
            sentences_to_delete: vec![],
            relations_to_delete: vec![],
            relations: vec![],
            shard_id,
        }
    }

    #[test]
    fn test_new_reader() -> anyhow::Result<()> {
        let dir = TempDir::new("payload_dir").unwrap();
        let fsc = FieldConfig {
            path: dir.path().as_os_str().to_os_string().into_string().unwrap(),
        };

        let mut field_writer_service = FieldWriterService::start(&fsc).unwrap();
        let resource1 = create_resource("shard1".to_string());
        let _ = field_writer_service.set_resource(&resource1);

        let field_reader_service = FieldReaderService::start(&fsc).unwrap();

        let rid = ResourceId {
            shard_id: "shard1".to_string(),
            uuid: "f56c58ac-b4f9-4d61-a077-ffccaadd0001".to_string(),
        };
        let result = field_reader_service.find_one(&rid).unwrap();
        assert!(result.is_some());

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
            field: "created".to_string(),
            r#type: 0,
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
            reload: false,
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
            reload: false,
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
            reload: false,
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
            reload: false,
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
            reload: false,
        };

        let result = field_reader_service.search(&search).unwrap();

        assert_eq!(result.total, 0);
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
            reload: false,
        };

        let result = field_reader_service.search(&search).unwrap();
        assert_eq!(result.total, 2);
        Ok(())
    }
}
