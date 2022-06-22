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
use std::sync::RwLock;

use async_std::fs;
use async_trait::async_trait;
use nucliadb_protos::resource::ResourceStatus;
use nucliadb_protos::{IndexParagraphs, Resource, ResourceId};
use nucliadb_service_interface::prelude::*;
use regex::Regex;
use tantivy::collector::Count;
use tantivy::query::AllQuery;
use tantivy::schema::*;
use tantivy::{doc, Index, IndexSettings, IndexSortByField, IndexWriter, Order};
use tracing::*;

use super::schema::ParagraphSchema;
use crate::schema::timestamp_to_datetime_utc;

lazy_static::lazy_static! {
    static ref REGEX: Regex = Regex::new(r"\\[a-zA-Z1-9]").unwrap();
}

pub struct ParagraphWriterService {
    pub index: Index,
    pub schema: ParagraphSchema,
    writer: RwLock<IndexWriter>,
}

impl Debug for ParagraphWriterService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TextService")
            .field("index", &self.index)
            .field("schema", &self.schema)
            .finish()
    }
}

impl WService for ParagraphWriterService {}

#[async_trait]
impl ServiceChild for ParagraphWriterService {
    async fn stop(&self) -> InternalResult<()> {
        info!("Stopping Paragraph Service");
        self.writer.write().unwrap().commit().unwrap();
        Ok(())
    }

    fn count(&self) -> usize {
        let reader = self.index.reader().unwrap();
        let searcher = reader.searcher();
        searcher.search(&AllQuery, &Count).unwrap_or(0)
    }
}

impl WriterChild for ParagraphWriterService {
    fn set_resource(&mut self, resource: &Resource) -> InternalResult<()> {
        let resource_id = resource.resource.as_ref().unwrap();
        let mut modified = false;

        if !resource_id.uuid.is_empty() {
            let uuid_term = Term::from_field_text(self.schema.uuid, &resource_id.uuid);
            self.writer.write().unwrap().delete_term(uuid_term);
            modified = true;
        }

        if resource.status != ResourceStatus::Delete as i32 {
            let _ = self.index_paragraph(resource);
            modified = true;
        }

        for paragraph_id in &resource.paragraphs_to_delete {
            let uuid_term = Term::from_field_text(self.schema.paragraph, paragraph_id);
            self.writer.write().unwrap().delete_term(uuid_term);
            modified = true;
        }

        match self.writer.write().unwrap().commit() {
            _ if !modified => Ok(()),
            Ok(opstamp) => {
                info!("Commit {}!", opstamp);
                Ok(())
            }
            Err(e) => {
                error!("Error starting Paragraph service: {}", e);
                Err(Box::new(ParagraphError { msg: e.to_string() }))
            }
        }
    }
    fn delete_resource(&mut self, resource_id: &ResourceId) -> InternalResult<()> {
        let uuid_field = self.schema.uuid;
        let uuid_term = Term::from_field_text(uuid_field, &resource_id.uuid);
        self.writer.write().unwrap().delete_term(uuid_term);
        match self.writer.write().unwrap().commit() {
            Ok(opstamp) => {
                info!("Commit {}!", opstamp);
                Ok(())
            }
            Err(e) => {
                error!("Error starting Paragraph service: {}", e);
                Err(Box::new(ParagraphError { msg: e.to_string() }))
            }
        }
    }

    fn garbage_collection(&mut self) {}
}

impl ParagraphWriterService {
    pub async fn start(config: &ParagraphServiceConfiguration) -> InternalResult<Self> {
        info!("Starting Paragraph Service");
        match ParagraphWriterService::open(config).await {
            Ok(service) => Ok(service),
            Err(e) => {
                warn!("Paragraph Service Open failed {}. Creating a new one.", e);
                match ParagraphWriterService::new(config).await {
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
    ) -> InternalResult<ParagraphWriterService> {
        match ParagraphWriterService::new_inner(config).await {
            Ok(service) => Ok(service),
            Err(e) => Err(Box::new(ParagraphError { msg: e.to_string() })),
        }
    }
    pub async fn open(
        config: &ParagraphServiceConfiguration,
    ) -> InternalResult<ParagraphWriterService> {
        match ParagraphWriterService::open_inner(config).await {
            Ok(service) => Ok(service),
            Err(e) => Err(Box::new(ParagraphError { msg: e.to_string() })),
        }
    }
    pub async fn new_inner(
        config: &ParagraphServiceConfiguration,
    ) -> tantivy::Result<ParagraphWriterService> {
        let paragraph_schema = ParagraphSchema::new();

        fs::create_dir_all(&config.path).await?;

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

        let writer = RwLock::new(index.writer_with_num_threads(1, 6_000_000).unwrap());

        Ok(ParagraphWriterService {
            index,
            writer,
            schema: paragraph_schema,
        })
    }

    pub async fn open_inner(
        config: &ParagraphServiceConfiguration,
    ) -> tantivy::Result<ParagraphWriterService> {
        let paragraph_schema = ParagraphSchema::new();

        let index = Index::open_in_dir(&config.path)?;

        let writer = RwLock::new(index.writer_with_num_threads(1, 6_000_000).unwrap());

        Ok(ParagraphWriterService {
            index,
            writer,
            schema: paragraph_schema,
        })
    }

    fn index_paragraph(&mut self, resource: &Resource) -> tantivy::Result<()> {
        let metadata = resource.metadata.as_ref().unwrap();

        let modified = metadata.modified.as_ref().unwrap();
        let created = metadata.created.as_ref().unwrap();

        let mut doc = doc!(
            self.schema.uuid => resource.resource.as_ref().unwrap().uuid.as_str(),
            self.schema.modified => timestamp_to_datetime_utc(modified),
            self.schema.created => timestamp_to_datetime_utc(created),
            self.schema.status => resource.status as u64,
        );

        #[allow(clippy::iter_cloned_collect)]
        let resource_labels: Vec<String> = resource.labels.iter().cloned().collect();
        for label in &resource_labels {
            let facet = Facet::from(label.as_str());
            doc.add_facet(self.schema.facets, facet);
        }

        let empty_paragraph = IndexParagraphs {
            paragraphs: HashMap::with_capacity(0),
        };
        for (field, text_info) in &resource.texts {
            let mut field_doc = doc.clone();
            let paragraphs = resource.paragraphs.get(field).unwrap_or(&empty_paragraph);
            // TODO: Make sure we do not copy
            // #[allow(clippy::iter_cloned_collect)]
            // let field_labels: Vec<String> = text_info.labels.iter().cloned().collect();
            for label in text_info.labels.iter() {
                field_doc.add_facet(self.schema.facets, Facet::from(label));
            }
            let facet_field = format!("/{}", field);
            field_doc.add_facet(self.schema.field, Facet::from(facet_field.as_str()));

            let chars: Vec<char> = REGEX.replace_all(&text_info.text, " ").chars().collect();

            for (paragraph_id, p) in &paragraphs.paragraphs {
                let mut subdoc = field_doc.clone();
                let start_pos = p.start as u64;
                let end_pos = p.end as u64;
                let index = p.index as u64;
                let labels = &p.labels;
                let lower_bound = std::cmp::min(start_pos as usize, chars.len());
                let upper_bound = std::cmp::min(end_pos as usize, chars.len());
                let mut text = String::new();
                for elem in &chars[lower_bound..upper_bound] {
                    text.push(*elem);
                }
                subdoc.add_text(self.schema.paragraph, paragraph_id.clone());
                subdoc.add_text(self.schema.text, &text);
                subdoc.add_u64(self.schema.start_pos, start_pos);
                subdoc.add_u64(self.schema.end_pos, end_pos);
                subdoc.add_u64(self.schema.index, index);

                let split = &p.split;
                subdoc.add_text(self.schema.split, split);

                #[allow(clippy::iter_cloned_collect)]
                let paragraph_labels: Vec<String> = labels.iter().cloned().collect();
                for label in paragraph_labels {
                    subdoc.add_facet(self.schema.facets, Facet::from(label.as_str()));
                }

                info!(
                    "Adding paragraph for {} with labels as {:?} [{} - {}]: {} ({})",
                    field, labels, start_pos, end_pos, text, paragraph_id
                );
                self.writer.write().unwrap().add_document(subdoc).unwrap();
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::SystemTime;

    use nucliadb_protos::{IndexParagraph, IndexParagraphs, Resource, ResourceId};
    use prost_types::Timestamp;
    use tantivy::collector::{Count, TopDocs};
    use tantivy::query::{AllQuery, TermQuery};
    use tempdir::TempDir;

    use super::*;

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

        const DOC1_TI: &str = "This the first document";
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

        let p1 = IndexParagraph {
            start: 0,
            end: DOC1_P1.len() as i32,
            sentences: HashMap::new(),
            field: "body".to_string(),
            labels: vec!["/nsfw".to_string()],
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
            labels: vec!["/cool".to_string()],
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
    async fn test_new_writer() -> anyhow::Result<()> {
        let dir = TempDir::new("payload_dir").unwrap();
        let psc = ParagraphServiceConfiguration {
            path: dir.path().as_os_str().to_os_string().into_string().unwrap(),
        };

        let mut paragraph_writer_service = ParagraphWriterService::start(&psc).await.unwrap();
        let resource1 = create_resource("shard1".to_string());
        let _ = paragraph_writer_service.set_resource(&resource1);
        let _ = paragraph_writer_service.set_resource(&resource1);

        let reader = paragraph_writer_service.index.reader()?;
        let searcher = reader.searcher();

        let query = TermQuery::new(
            Term::from_field_text(paragraph_writer_service.schema.text, "document"),
            IndexRecordOption::Basic,
        );

        let (_top_docs, count) = searcher.search(&query, &(TopDocs::with_limit(2), Count))?;
        assert_eq!(count, 1);

        let (_top_docs, count) = searcher.search(&AllQuery, &(TopDocs::with_limit(10), Count))?;
        assert_eq!(count, 4);
        Ok(())
    }
}
