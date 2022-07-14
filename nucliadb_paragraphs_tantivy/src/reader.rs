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
use std::panic;

use async_std::fs;
use async_trait::async_trait;
use nucliadb_protos::{ParagraphSearchRequest, ParagraphSearchResponse, ResourceId};
use nucliadb_service_interface::prelude::*;
use tantivy::collector::{
    Count, DocSetCollector, FacetCollector, FacetCounts, MultiCollector, TopDocs,
};
use tantivy::query::{AllQuery, BooleanQuery, Occur, Query, QueryParser, TermQuery};
use tantivy::schema::*;
use tantivy::{
    DocAddress, Index, IndexReader, IndexSettings, IndexSortByField, Order, ReloadPolicy,
};
use tracing::*;

use super::schema::ParagraphSchema;
use crate::search_query::{self, Distance};
use crate::search_response::SearchResponse;
pub struct ParagraphReaderService {
    index: Index,
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

impl RService for ParagraphReaderService {}
impl ParagraphServiceReader for ParagraphReaderService {}
impl ParagraphReaderOnly for ParagraphReaderService {}

#[async_trait]
impl ServiceChild for ParagraphReaderService {
    async fn stop(&self) -> InternalResult<()> {
        info!("Stopping Paragraph Reader Service");
        Ok(())
    }
    fn count(&self) -> usize {
        let searcher = self.reader.searcher();
        searcher.search(&AllQuery, &Count).unwrap()
    }
}

impl ReaderChild for ParagraphReaderService {
    type Request = ParagraphSearchRequest;
    type Response = ParagraphSearchResponse;
    fn search(&self, request: &Self::Request) -> InternalResult<Self::Response> {
        let query = {
            let parser = QueryParser::for_index(&self.index, vec![self.schema.text]);
            let first_attemp =
                search_query::process(&parser, request, &self.schema, Distance::Low).unwrap();
            if first_attemp.is_empty() {
                search_query::process(&parser, request, &self.schema, Distance::High).unwrap()
            } else {
                first_attemp
            }
        };
        let results = request.result_per_page as usize;
        let offset = results * request.page_number as usize;

        let facets = request
            .faceted
            .as_ref()
            .map(|v| v.tags.clone())
            .unwrap_or_default();

        let (top_docs, facets_count) = self.do_search(query, results, offset, &facets);
        Ok(ParagraphSearchResponse::from(SearchResponse {
            text_service: self,
            facets_count,
            facets,
            top_docs,
            order_by: request.order.clone(),
            page_number: request.page_number,
            results_per_page: results as i32,
        }))
    }
    fn reload(&self) {
        self.reader.reload().unwrap();
    }
    fn stored_ids(&self) -> Vec<String> {
        self.keys()
    }
}

impl ParagraphReaderService {
    pub fn find_one(&self, resource_id: &ResourceId) -> tantivy::Result<Option<Document>> {
        let uuid_field = self.schema.uuid;
        let uuid_term = Term::from_field_text(uuid_field, &resource_id.uuid);
        let uuid_query = TermQuery::new(uuid_term, IndexRecordOption::Basic);

        let searcher = self.reader.searcher();

        let top_docs = searcher.search(&uuid_query, &TopDocs::with_limit(1))?;

        top_docs
            .first()
            .map(|(_, doc_address)| searcher.doc(*doc_address))
            .transpose()
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

    pub async fn start(config: &ParagraphServiceConfiguration) -> InternalResult<Self> {
        info!("Starting Paragraph Service");
        match ParagraphReaderService::open(config).await {
            Ok(service) => Ok(service),
            Err(_e) => {
                warn!("Paragraph Service does not exists. Creating a new one.");
                match ParagraphReaderService::new(config).await {
                    Ok(service) => Ok(service),
                    Err(e) => {
                        error!("Error starting Paragraph service: {}", e);
                        Err(Box::new(ParagraphError { msg: e.to_string() }))
                    }
                }
            }
        }
    }
    pub async fn new(
        config: &ParagraphServiceConfiguration,
    ) -> InternalResult<ParagraphReaderService> {
        match ParagraphReaderService::new_inner(config).await {
            Ok(service) => Ok(service),
            Err(e) => Err(Box::new(ParagraphError { msg: e.to_string() })),
        }
    }
    pub async fn open(
        config: &ParagraphServiceConfiguration,
    ) -> InternalResult<ParagraphReaderService> {
        match ParagraphReaderService::open_inner(config).await {
            Ok(service) => Ok(service),
            Err(e) => Err(Box::new(ParagraphError { msg: e.to_string() })),
        }
    }

    pub async fn new_inner(
        config: &ParagraphServiceConfiguration,
    ) -> tantivy::Result<ParagraphReaderService> {
        let paragraph_schema = ParagraphSchema::new();

        fs::create_dir_all(&config.path).await?;

        debug!("Creating index builder {}:{}", line!(), file!());
        let mut index_builder = Index::builder().schema(paragraph_schema.schema.clone());
        let settings = IndexSettings {
            sort_by_field: Some(IndexSortByField {
                field: "created".to_string(),
                order: Order::Desc,
            }),
            ..Default::default()
        };

        index_builder = index_builder.settings(settings);

        let index = index_builder.create_in_dir(&config.path).unwrap();
        debug!("Index builder created  {}:{}", line!(), file!());

        debug!("Creating index  {}:{}", line!(), file!());
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommit)
            .try_into()?;
        debug!("Index created  {}:{}", line!(), file!());
        Ok(ParagraphReaderService {
            index,
            reader,
            schema: paragraph_schema,
        })
    }

    pub async fn open_inner(
        config: &ParagraphServiceConfiguration,
    ) -> tantivy::Result<ParagraphReaderService> {
        let paragraph_schema = ParagraphSchema::new();
        let index = Index::open_in_dir(&config.path)?;

        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommit)
            .try_into()?;

        Ok(ParagraphReaderService {
            index,
            reader,
            schema: paragraph_schema,
        })
    }

    fn do_search(
        &self,
        query: Vec<(Occur, Box<dyn Query>)>,
        results: usize,
        offset: usize,
        facets: &[String],
    ) -> (Vec<(f32, DocAddress)>, FacetCounts) {
        let mut facet_collector = FacetCollector::for_field(self.schema.facets);
        for facet in facets {
            match panic::catch_unwind(|| Facet::from(facet.as_str())) {
                Ok(facet) => facet_collector.add_facet(facet),
                Err(_e) => {
                    error!("Invalid facet: {}", facet);
                }
            }
        }

        let topdocs = TopDocs::with_limit(results).and_offset(offset);

        let mut multicollector = MultiCollector::new();
        let facet_handler = multicollector.add_collector(facet_collector);
        let topdocs_handler = multicollector.add_collector(topdocs);

        let query = BooleanQuery::new(query);
        let searcher = self.reader.searcher();
        debug!("{:?}", query);
        let mut multi_fruit = searcher.search(&query, &multicollector).unwrap();
        let facets_count = facet_handler.extract(&mut multi_fruit);
        let top_docs = topdocs_handler.extract(&mut multi_fruit);
        debug!("{:?}", top_docs);
        // top_docs.retain(|(v, _)| *v > 0.2f32);
        (top_docs, facets_count)
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

    use nucliadb_protos::{
        Faceted, Filter, IndexParagraph, IndexParagraphs, OrderBy, Resource, ResourceId, Timestamps,
    };
    use prost_types::Timestamp;
    use tantivy::collector::Count;
    use tantivy::query::AllQuery;
    use tempdir::TempDir;

    use super::*;
    use crate::writer::ParagraphWriterService;

    fn create_resource(shard_id: String) -> Resource {
        const UUID: &str = "f56c58ac-b4f9-4d61-a077-ffccaadd0001";
        let resource_id = ResourceId {
            shard_id: shard_id.to_string(),
            uuid: UUID.to_string(),
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
            labels: vec!["/e/mylabel".to_string()],
        };

        let ti_body = nucliadb_protos::TextInformation {
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
            field: "body".to_string(),
            labels: vec!["/e/myentity".to_string()],
            index: 0,
            split: "".to_string(),
        };
        let p1_uuid = format!("{}/{}/{}-{}", UUID, "body", 0, DOC1_P1.len());

        let p2 = IndexParagraph {
            start: DOC1_P1.len() as i32,
            end: (DOC1_P1.len() + DOC1_P2.len()) as i32,
            sentences: HashMap::new(),
            field: "body".to_string(),
            labels: vec![
                "/tantivy".to_string(),
                "/test".to_string(),
                "/label1".to_string(),
            ],
            index: 1,
            split: "".to_string(),
        };
        let p2_uuid = format!(
            "{}/{}/{}-{}",
            UUID,
            "body",
            DOC1_P1.len(),
            DOC1_P1.len() + DOC1_P2.len()
        );

        let p3 = IndexParagraph {
            start: (DOC1_P1.len() + DOC1_P2.len()) as i32,
            end: (DOC1_P1.len() + DOC1_P2.len() + DOC1_P3.len()) as i32,
            sentences: HashMap::new(),
            field: "body".to_string(),
            labels: vec!["/three".to_string(), "/label2".to_string()],
            index: 2,
            split: "".to_string(),
        };
        let p3_uuid = format!(
            "{}/{}/{}-{}",
            UUID,
            "body",
            DOC1_P1.len() + DOC1_P2.len(),
            DOC1_P1.len() + DOC1_P2.len() + DOC1_P3.len()
        );

        let body_paragraphs = IndexParagraphs {
            paragraphs: [(p1_uuid, p1), (p2_uuid, p2), (p3_uuid, p3)]
                .into_iter()
                .collect(),
        };

        let p4 = IndexParagraph {
            start: 0,
            end: DOC1_TI.len() as i32,
            sentences: HashMap::new(),
            field: "title".to_string(),
            labels: vec!["/c/ool".to_string()],
            index: 3,
            split: "".to_string(),
        };
        let p4_uuid = format!("{}/{}/{}-{}", UUID, "body", 0, DOC1_TI.len());

        let title_paragraphs = IndexParagraphs {
            paragraphs: [(p4_uuid, p4)].into_iter().collect(),
        };

        let paragraphs = [
            ("body".to_string(), body_paragraphs),
            ("title".to_string(), title_paragraphs),
        ]
        .into_iter()
        .collect();

        Resource {
            resource: Some(resource_id),
            metadata: Some(metadata),
            texts,
            status: nucliadb_protos::resource::ResourceStatus::Processed as i32,
            labels: vec!["/l/mylabel_resource".to_string()],
            paragraphs,
            paragraphs_to_delete: vec![],
            sentences_to_delete: vec![],
            relations_to_delete: vec![],
            relations: vec![],
            shard_id,
        }
    }

    #[tokio::test]
    async fn test_new_paragraph() -> anyhow::Result<()> {
        let dir = TempDir::new("payload_dir").unwrap();
        let psc = ParagraphServiceConfiguration {
            path: dir.path().as_os_str().to_os_string().into_string().unwrap(),
        };
        let mut paragraph_writer_service = ParagraphWriterService::start(&psc).await.unwrap();
        let resource1 = create_resource("shard1".to_string());
        let _ = paragraph_writer_service.set_resource(&resource1);

        let paragraph_reader_service = ParagraphReaderService::start(&psc).await.unwrap();

        let reader = paragraph_writer_service.index.reader()?;
        let searcher = reader.searcher();

        let (_top_docs, count) = searcher.search(&AllQuery, &(TopDocs::with_limit(10), Count))?;
        assert_eq!(count, 4);

        const UUID: &str = "f56c58ac-b4f9-4d61-a077-ffccaadd0001";
        let rid = ResourceId {
            shard_id: "shard1".to_string(),
            uuid: UUID.to_string(),
        };

        let result = paragraph_reader_service.find_resource(&rid).unwrap();
        assert!(!result.is_empty());
        let result = paragraph_reader_service.find_one(&rid).unwrap();
        assert!(result.is_some());

        // Testing filtering one filter from resource, one from field and one from paragraph

        let filter = Filter {
            tags: vec![
                "/l/mylabel_resource".to_string(),
                "/c/ool".to_string(),
                "/e/mylabel".to_string(),
            ],
        };

        let faceted = Faceted {
            tags: vec!["/l".to_string(), "/e".to_string(), "/c".to_string()],
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

        // Search on all paragraphs
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
            reload: false,
        };
        let result = paragraph_reader_service.search(&search).unwrap();
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
            reload: false,
        };
        let result = paragraph_reader_service.search(&search).unwrap();
        assert_eq!(result.total, 4);

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
            reload: false,
        };
        let result = paragraph_reader_service.search(&search).unwrap();
        assert_eq!(result.total, 1);

        // Search on all paragraphs in resource with typo
        let search = ParagraphSearchRequest {
            id: "shard1".to_string(),
            uuid: UUID.to_string(),
            body: "\"shoupd\" enaugh".to_string(),
            fields: vec![],
            filter: None,
            faceted: None,
            order: None,
            page_number: 0,
            result_per_page: 20,
            timestamps: None,
            reload: false,
        };
        let result = paragraph_reader_service.search(&search).unwrap();
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
            reload: false,
        };
        let result = paragraph_reader_service.search(&search).unwrap();
        assert_eq!(result.total, 1);

        // Search filter all paragraphs
        let search = ParagraphSearchRequest {
            id: "shard1".to_string(),
            uuid: "".to_string(),
            body: "this is the".to_string(),
            fields: vec![],
            filter: None,
            faceted: Some(faceted.clone()),
            order: Some(order.clone()),
            page_number: 0,
            result_per_page: 20,
            timestamps: Some(timestamps.clone()),
            reload: false,
        };
        let result = paragraph_reader_service.search(&search).unwrap();
        assert_eq!(result.total, 2);
        // for (key, facet) in result.facets {
        //     println!("KEY {}", key);
        //     for facetresult in facet.facetresults {
        //         println!("{}", facetresult.tag);
        //     }
        // }
        // task::sleep(Duration::from_secs(1)).await;
        // Search typo on all paragraph
        let search = ParagraphSearchRequest {
            id: "shard1".to_string(),
            uuid: "".to_string(),
            body: "this is the".to_string(),
            fields: vec![],
            filter: Some(filter),
            faceted: Some(faceted),
            order: Some(order),
            page_number: 0,
            result_per_page: 20,
            timestamps: Some(timestamps),
            reload: false,
        };
        let result = paragraph_reader_service.search(&search).unwrap();
        assert_eq!(result.total, 1);
        Ok(())
    }
}
