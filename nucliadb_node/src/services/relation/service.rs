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
use std::panic;

use super::schema::ParagraphSchema;
use crate::services::text::search_query::SearchQuery;
use crate::services::text::search_response::SearchResponse;
use async_trait::async_trait;
use nucliadb_protos::{
    DocumentSearchRequest, DocumentSearchResponse, ParagraphSearchRequest, ParagraphSearchResponse,
};
use nucliadb_protos::{OrderBy, Paragraphs, Resource, ResourceId, TextInformation};
use tantivy::chrono::{DateTime, Utc};
use tantivy::collector::{FacetCollector, FacetCounts, MultiCollector, TopDocs};
use tantivy::query::{Query, QueryParser, TermQuery};
use tantivy::schema::*;
use tantivy::{DocAddress, Index, IndexReader, IndexWriter, TantivyError};
use tracing::*;

pub struct TextServiceConfiguration {
    pub path: String,
}

pub struct TextService {
    index: Index,
    pub schema: ParagraphSchema,
    writer: IndexWriter,
    pub reader: IndexReader,
}

impl Debug for TextService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TextService")
            .field("index", &self.index)
            .field("schema", &self.schema)
            .finish()
    }
}

#[async_trait]
pub trait Writer {
    async fn delete_resource(
        &mut self,
        resource_id: &ResourceId,
        commit: bool,
    ) -> tantivy::Result<u64>;
    async fn set_resource(&mut self, resource: &Resource) -> Result<(), String>;
}

#[async_trait]
pub trait Reader {
    async fn search_documents(
        &self,
        request: &DocumentSearchRequest,
    ) -> tantivy::Result<DocumentSearchResponse>;
    async fn search_paragraphs(
        &self,
        request: &ParagraphSearchRequest,
    ) -> tantivy::Result<ParagraphSearchResponse>;
    async fn find_one(&self, resource_id: &ResourceId) -> tantivy::Result<Option<Document>>;
    async fn find_resource(&self, resource_id: &ResourceId) -> tantivy::Result<Vec<Document>>;
}

impl TextService {
    async fn delete_if_exists(&mut self, resource_id: &ResourceId) -> Option<DateTime<Utc>> {
        match self.find_one(resource_id).await {
            Ok(document) => match document {
                Some(doc) => {
                    trace!("Resource already exists, deleting!");

                    let creation_date = doc
                        .get_first(self.schema.created)
                        .expect("document doesn't appear to creation date.")
                        .date_value()
                        .unwrap();

                    if let Err(e) = self.delete_resource(resource_id, false).await {
                        error!("Error deleting resource: {}", e);
                    }

                    return Some(*creation_date);
                }
                None => trace!("No documents found for this resource, adding!"),
            },
            Err(e) => trace!("Error in find_resource: {}", e),
        };

        None
    }

    async fn index_paragraph(
        &mut self,
        mut doc: Document,
        texts: &HashMap<String, TextInformation>,
        paragraphs: &HashMap<String, Paragraphs>,
    ) -> tantivy::Result<()> {
        doc.add_facet(self.schema.facets, Facet::from("/paragraph"));
        for (doc_field, paragraphs) in paragraphs {
            let text = &texts
                .get(doc_field)
                .expect("Error obtaining text field of document")
                .text;

            for p in paragraphs.paragraphs.values() {
                let start_pos = p.start as u64;
                let end_pos = p.end as u64;
                let labels = &p.labels;

                if end_pos as usize > text.len() {
                    let err = format!(
                        "Paragraph end position {} is greater than text len.",
                        end_pos
                    );
                    error!("{}", err);
                    return Err(TantivyError::InvalidArgument(err));
                }
                let text = &text[start_pos as usize..end_pos as usize];
                let document_field = format!("/f/{}", doc_field);

                doc.add_text(self.schema.text, text);
                doc.add_u64(self.schema.start_pos, start_pos);
                doc.add_u64(self.schema.end_pos, end_pos);
                doc.add_facet(self.schema.document_field, document_field.as_str());

                for label in labels {
                    let path = format!("/l/{}", label);
                    doc.add_facet(self.schema.facets, Facet::from(path.as_str()));
                }

                trace!(
                    "Adding paragraph for {} with labels as {:?} [{} - {}]: {}",
                    document_field,
                    labels,
                    start_pos,
                    end_pos,
                    text
                );
                self.writer.add_document(doc.clone());
            }
        }

        Ok(())
    }

    async fn index_document(
        &mut self,
        mut doc: Document,
        texts: &HashMap<String, TextInformation>,
    ) {
        let mut text = String::new();
        for text_info in texts.values() {
            text.push_str(&text_info.text);
            text.push('\n');
        }

        doc.add_facet(self.schema.facets, Facet::from("/document"));
        doc.add_text(self.schema.text, &text);
        trace!("Adding document: {}", text);
        self.writer.add_document(doc.clone());
    }
}

#[async_trait]
impl Writer for TextService {
    async fn delete_resource(
        &mut self,
        resource_id: &ResourceId,
        commit: bool,
    ) -> tantivy::Result<u64> {
        let uuid_field = self.schema.uuid;
        let uuid_term = Term::from_field_text(uuid_field, &resource_id.uuid);
        let opstamp = self.writer.delete_term(uuid_term);

        if commit {
            self.writer.commit()?;
        }

        Ok(opstamp)
    }

    async fn set_resource(&mut self, resource: &Resource) -> Result<(), String> {
        trace!("======== SET RESOURCE ===========");
        let paragraph_map = &resource.paragraphs;
        let resource_id = resource.resource.as_ref().unwrap();
        let creation_date = self.delete_if_exists(resource_id).await;
        let base_document = ParagraphSchema::base_document(resource, &creation_date);

        self.index_document(base_document.clone(), &resource.texts)
            .await;
        if let Err(e) = self
            .index_paragraph(base_document.clone(), &resource.texts, paragraph_map)
            .await
        {
            return Err(format!("Error indexing document: {}", e));
        }

        // TODO: Spawn a task here! and make the function async!
        match self.writer.commit() {
            Ok(opstamp) => trace!("Commit {}!", opstamp),
            Err(e) => error!("Error doing commit: {}", e),
        }

        trace!("=================================");
        Ok(())
    }
}

impl TextService {
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

    fn do_search(
        &self,
        query: Box<dyn Query>,
        order_field: Option<Field>,
        results: usize,
        offset: usize,
        facets: &[String],
    ) -> (Vec<(u64, DocAddress)>, FacetCounts) {
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

        let topdocs_collector = match order_field {
            Some(order_field) => topdocs.order_by_u64_field(order_field),
            None => topdocs.order_by_u64_field(self.schema.modified),
        };

        let mut multicollector = MultiCollector::new();
        let facet_handler = multicollector.add_collector(facet_collector);
        let topdocs_handler = multicollector.add_collector(topdocs_collector);

        if let Err(e) = self.reader.reload() {
            error!("Error reloading index: {}", e);
        }

        let searcher = self.reader.searcher();
        let mut multi_fruit = searcher.search(&query, &multicollector).unwrap();

        let facets_count = facet_handler.extract(&mut multi_fruit);
        let top_docs = topdocs_handler.extract(&mut multi_fruit);

        (top_docs, facets_count)
    }
}

#[async_trait]
impl Reader for TextService {
    async fn find_one(&self, resource_id: &ResourceId) -> tantivy::Result<Option<Document>> {
        let uuid_field = self.schema.uuid;
        let uuid_term = Term::from_field_text(uuid_field, &resource_id.uuid);
        let uuid_query = TermQuery::new(uuid_term, IndexRecordOption::Basic);

        let searcher = self.reader.searcher();

        let top_docs = searcher.search(&uuid_query, &TopDocs::with_limit(1))?;

        if let Some((_score, doc_address)) = top_docs.first() {
            let doc = searcher.doc(*doc_address)?;
            Ok(Some(doc))
        } else {
            Ok(None)
        }
    }

    async fn find_resource(&self, resource_id: &ResourceId) -> tantivy::Result<Vec<Document>> {
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

    async fn search_paragraphs(
        &self,
        request: &ParagraphSearchRequest,
    ) -> tantivy::Result<ParagraphSearchResponse> {
        let query: String = SearchQuery::paragraph(request)?.query;
        trace!("Query: {}", query);

        let query_parser = QueryParser::for_index(&self.index, vec![self.schema.text]);
        let query = query_parser.parse_query(&query)?;
        debug!("Paragraph query: {:?}", query);

        let results = request.result_per_page as usize;
        let offset = results * request.page_number as usize;

        let order_field = self.get_order_field(&request.order);

        let mut facets = Vec::new();

        if let Some(faceted) = &request.faceted_document {
            facets.extend(faceted.tags.clone());
        };
        if let Some(faceted) = &request.faceted_paragraph {
            facets.extend(faceted.tags.clone());
        };

        let (top_docs, facets_count) = self.do_search(query, order_field, results, offset, &facets);

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

    async fn search_documents(
        &self,
        request: &DocumentSearchRequest,
    ) -> tantivy::Result<DocumentSearchResponse> {
        let query: String = SearchQuery::document(request)?.query;
        trace!("Query: {}", query);

        let query_parser = QueryParser::for_index(&self.index, vec![self.schema.text]);
        let query = query_parser.parse_query(&query)?;
        debug!("Document query: {:?}", query);

        let results = request.result_per_page as usize;
        let offset = results * request.page_number as usize;

        let order_field = self.get_order_field(&request.order);

        let facets = match &request.faceted {
            Some(faceted) => faceted.tags.clone(),
            None => Vec::new(),
        };

        let (top_docs, facets_count) = self.do_search(query, order_field, results, offset, &facets);

        Ok(DocumentSearchResponse::from(SearchResponse {
            text_service: self,
            facets_count,
            facets,
            top_docs,
            order_by: request.order.clone(),
            page_number: request.page_number,
            results_per_page: results as i32,
        }))
    }
}

impl TextService {
    pub async fn start(config: &TextServiceConfiguration) -> Result<Self, String> {
        info!("Starting Text Service");
        match Self::load(config).await {
            Ok(service) => Ok(service),
            Err(_e) => {
                warn!("Text Service does not exists. Creating a new one.");
                match Self::new(config).await {
                    Ok(service) => Ok(service),
                    Err(e) => {
                        let err = format!("Error starting Text service: {}", e);
                        error!("{}", err);
                        Err(err)
                    }
                }
            }
        }
    }

    pub async fn stop(&mut self) -> Result<(), String> {
        info!("Stopping Text Service");
        // self.builder.build();
        match self.writer.commit() {
            Ok(_) => Ok(()),
            Err(_) => Err("Error commiting pending changes.".to_string()),
        }
    }

    async fn new(config: &TextServiceConfiguration) -> tantivy::Result<TextService> {
        let paragraph = ParagraphSchema::new();

        std::fs::create_dir_all(&config.path)?;
        let index = Index::create_in_dir(&config.path, paragraph.schema.clone())?;

        // Here we use a buffer of 100MB that will be split between indexing threads.
        let writer = index.writer(100_000_000)?;

        let reader = index.reader()?;

        Ok(TextService {
            index,
            reader,
            writer,
            schema: paragraph,
        })
    }

    async fn load(config: &TextServiceConfiguration) -> tantivy::Result<TextService> {
        let paragraph = ParagraphSchema::new();
        let index = Index::open_in_dir(&config.path)?;

        let reader = index.reader()?;
        let writer = index.writer(100_000_000).unwrap();

        Ok(TextService {
            index,
            reader,
            writer,
            schema: paragraph,
        })
    }
}
