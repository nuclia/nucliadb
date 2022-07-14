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
use std::panic;

use async_std::fs;
use async_trait::async_trait;
use nucliadb_protos::{
    DocumentResult, DocumentSearchRequest, DocumentSearchResponse, FacetResult, FacetResults,
    OrderBy, ResourceId,
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
use crate::search_query::SearchQuery;

#[derive(Debug)]
struct TError(TantivyError);
impl InternalError for TError {}
impl Display for TError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub struct SearchResponse<S> {
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

impl RService for FieldReaderService {}
impl FieldServiceReader for FieldReaderService {}
impl FieldReaderOnly for FieldReaderService {}

#[async_trait]
impl ServiceChild for FieldReaderService {
    async fn stop(&self) -> InternalResult<()> {
        info!("Stopping Reader Text Service");
        Ok(())
    }

    fn count(&self) -> usize {
        let searcher = self.reader.searcher();
        searcher.search(&AllQuery, &Count).unwrap()
    }
}

impl ReaderChild for FieldReaderService {
    type Request = DocumentSearchRequest;
    type Response = DocumentSearchResponse;
    fn search(&self, request: &Self::Request) -> InternalResult<Self::Response> {
        info!("Document search at {}:{}", line!(), file!());
        self.do_search(request)
            .map_err(|e| Box::new(TError(e)) as Box<dyn InternalError>)
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

    pub async fn start(config: &FieldServiceConfiguration) -> InternalResult<Self> {
        info!("Starting Text Service");
        match FieldReaderService::open(config).await {
            Ok(service) => Ok(service),
            Err(_e) => {
                warn!("Text Service does not exists. Creating a new one.");
                match FieldReaderService::new(config).await {
                    Ok(service) => Ok(service),
                    Err(e) => {
                        error!("Error starting Text service: {}", e);
                        Err(Box::new(FieldError { msg: e.to_string() }))
                    }
                }
            }
        }
    }
    pub async fn new(config: &FieldServiceConfiguration) -> InternalResult<Self> {
        match FieldReaderService::new_inner(config).await {
            Ok(service) => Ok(service),
            Err(e) => Err(Box::new(FieldError { msg: e.to_string() })),
        }
    }
    pub async fn open(config: &FieldServiceConfiguration) -> InternalResult<Self> {
        match FieldReaderService::open_inner(config).await {
            Ok(service) => Ok(service),
            Err(e) => Err(Box::new(FieldError { msg: e.to_string() })),
        }
    }
    pub async fn new_inner(
        config: &FieldServiceConfiguration,
    ) -> tantivy::Result<FieldReaderService> {
        let field_schema = FieldSchema::new();

        fs::create_dir_all(&config.path).await?;

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

    pub async fn open_inner(
        config: &FieldServiceConfiguration,
    ) -> tantivy::Result<FieldReaderService> {
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
        let total = response.top_docs.len();
        let mut results = Vec::with_capacity(total);
        info!("Document query at {}:{}", line!(), file!());
        for (score, doc_address) in response.top_docs {
            match searcher.doc(doc_address) {
                Ok(doc) => {
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

                    let result = DocumentResult {
                        uuid,
                        field,
                        score,
                        ..Default::default()
                    };
                    info!("Document query at {}:{}", line!(), file!());
                    results.push(result);
                }
                Err(e) => error!("Error retrieving document from index: {}", e),
            }
        }

        let facets = response.facets;
        let facets_count = response.facets_count;
        let do_count = |facets_count: &FacetCounts| -> Vec<FacetResult> {
            facets_count
                .top_k("/t", 50)
                .into_iter()
                .map(|(facet, count)| FacetResult {
                    tag: facet.to_string(),
                    total: count as i32,
                })
                .collect()
        };
        let facets = facets
            .into_iter()
            .map(|facet| (&facets_count, facet))
            .map(|(facets_count, facet)| (do_count(facets_count), facet))
            .filter(|(r, _)| !r.is_empty())
            .map(|(facetresults, facet)| (facet, FacetResults { facetresults }))
            .collect();
        info!("Document query at {}:{}", line!(), file!());
        DocumentSearchResponse {
            total: total as i32,
            results,
            facets,
            page_number: response.page_number,
            result_per_page: response.results_per_page,
        }
    }

    fn convert_bm25_order(
        &self,
        response: SearchResponse<f32>,
        searcher: &Searcher,
    ) -> DocumentSearchResponse {
        info!("Document query at {}:{}", line!(), file!());
        let total = response.top_docs.len();
        let mut results = Vec::with_capacity(total);
        info!("Document query at {}:{}", line!(), file!());
        for (score, doc_address) in response.top_docs {
            match searcher.doc(doc_address) {
                Ok(doc) => {
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

                    let result = DocumentResult {
                        uuid,
                        field,
                        score_bm25: score,
                        ..Default::default()
                    };
                    info!("Document query at {}:{}", line!(), file!());
                    results.push(result);
                }
                Err(e) => error!("Error retrieving document from index: {}", e),
            }
        }

        let mut facets = HashMap::new();
        info!("Document query at {}:{}", line!(), file!());

        for facet in response.facets {
            info!("Document query at {}:{}", line!(), file!());
            let count: Vec<_> = response
                .facets_count
                .top_k("/t", 50)
                .iter()
                .map(|(facet, count)| FacetResult {
                    tag: facet.to_string(),
                    total: *count as i32,
                })
                .collect();

            facets.insert(
                facet,
                FacetResults {
                    facetresults: count,
                },
            );
            info!("Document query at {}:{}", line!(), file!());
        }

        info!("Document query at {}:{}", line!(), file!());
        DocumentSearchResponse {
            total: total as i32,
            results,
            facets,
            page_number: response.page_number,
            result_per_page: response.results_per_page,
        }
    }

    fn do_search(
        &self,
        request: &DocumentSearchRequest,
    ) -> tantivy::Result<DocumentSearchResponse> {
        info!("Document search starts {}:{}", line!(), file!());
        let query = if !request.body.is_empty() {
            info!("Body was not empty document search {}:{}", line!(), file!());
            let extended_query = SearchQuery::document(request).unwrap();
            info!("{}", extended_query);
            let mut query_parser = QueryParser::for_index(&self.index, vec![self.schema.text]);
            query_parser.set_conjunction_by_default();
            info!("Request parsed {}:{}", line!(), file!());
            query_parser.parse_query(&extended_query)
        } else {
            info!("Document search at {}:{}", line!(), file!());
            Ok(Box::new(AllQuery) as Box<dyn Query>)
        }?;
        info!("Document search at {}:{}", line!(), file!());
        let results = request.result_per_page as usize;
        let offset = results * request.page_number as usize;

        info!("Document search at {}:{}", line!(), file!());
        let order_field = self.get_order_field(&request.order);
        let facets = request
            .faceted
            .as_ref()
            .map(|v| v.tags.clone())
            .unwrap_or_default();

        // let facets = match &request.faceted {
        //     Some(faceted) => faceted.tags.clone(),
        //     None => Vec::new(),
        // };
        info!("Document search at {}:{}", line!(), file!());
        let mut facet_collector = FacetCollector::for_field(self.schema.facets);
        info!("Document search at {}:{}", line!(), file!());
        for facet in &facets {
            match panic::catch_unwind(|| Facet::from(facet.as_str())) {
                Ok(facet) => facet_collector.add_facet(facet),
                Err(_e) => {
                    error!("Invalid facet: {}", facet);
                }
            }
        }
        info!("Document search at {}:{}", line!(), file!());
        let topdocs = TopDocs::with_limit(results).and_offset(offset);

        info!("Document search at {}:{}", line!(), file!());
        let mut multicollector = MultiCollector::new();
        let facet_handler = multicollector.add_collector(facet_collector);
        match order_field {
            Some(order_field) => {
                info!("Document search at {}:{}", line!(), file!());
                let topdocs_collector = topdocs.order_by_u64_field(order_field);
                let topdocs_handler = multicollector.add_collector(topdocs_collector);
                info!("Document search at {}:{}", line!(), file!());
                let searcher = self.reader.searcher();
                let mut multi_fruit = searcher.search(&query, &multicollector).unwrap();
                info!("Document search at {}:{}", line!(), file!());
                let facets_count = facet_handler.extract(&mut multi_fruit);
                let top_docs = topdocs_handler.extract(&mut multi_fruit);
                info!("Document search at {}:{}", line!(), file!());
                Ok(self.convert_int_order(
                    SearchResponse {
                        facets_count,
                        facets,
                        top_docs,
                        order_by: request.order.clone(),
                        page_number: request.page_number,
                        results_per_page: results as i32,
                    },
                    &searcher,
                ))
            }
            None => {
                info!("Document search at {}:{}", line!(), file!());
                let topdocs_handler = multicollector.add_collector(topdocs);
                info!("Document search at {}:{}", line!(), file!());
                let searcher = self.reader.searcher();
                let mut multi_fruit = searcher.search(&query, &multicollector).unwrap();
                info!("Document search at {}:{}", line!(), file!());
                let facets_count = facet_handler.extract(&mut multi_fruit);
                let top_docs = topdocs_handler.extract(&mut multi_fruit);
                info!("Document search at {}:{}", line!(), file!());
                Ok(self.convert_bm25_order(
                    SearchResponse {
                        facets_count,
                        facets,
                        top_docs,
                        order_by: request.order.clone(),
                        page_number: request.page_number,
                        results_per_page: results as i32,
                    },
                    &searcher,
                ))
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

    #[tokio::test]
    async fn test_new_reader() -> anyhow::Result<()> {
        let dir = TempDir::new("payload_dir").unwrap();
        let fsc = FieldServiceConfiguration {
            path: dir.path().as_os_str().to_os_string().into_string().unwrap(),
        };

        let mut field_writer_service = FieldWriterService::start(&fsc).await.unwrap();
        let resource1 = create_resource("shard1".to_string());
        let _ = field_writer_service.set_resource(&resource1);

        let field_reader_service = FieldReaderService::start(&fsc).await.unwrap();

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
            tags: vec!["/l".to_string(), "/t".to_string()],
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
