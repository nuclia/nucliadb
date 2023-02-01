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
use std::fs;
use std::time::SystemTime;

use nucliadb_core::prelude::*;
use nucliadb_core::protos::prost::Message;
use nucliadb_core::protos::resource::ResourceStatus;
use nucliadb_core::protos::{Resource, ResourceId};
use nucliadb_core::tracing::{self, *};
use regex::Regex;
use tantivy::collector::Count;
use tantivy::query::AllQuery;
use tantivy::schema::*;
use tantivy::{doc, Index, IndexSettings, IndexSortByField, IndexWriter, Order};

use super::schema::ParagraphSchema;
use crate::schema::timestamp_to_datetime_utc;

lazy_static::lazy_static! {
    static ref REGEX: Regex = Regex::new(r"\\[a-zA-Z1-9]").unwrap();
}

pub struct ParagraphWriterService {
    pub index: Index,
    pub schema: ParagraphSchema,
    writer: IndexWriter,
}

impl Debug for ParagraphWriterService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TextService")
            .field("index", &self.index)
            .field("schema", &self.schema)
            .finish()
    }
}

impl ParagraphWriter for ParagraphWriterService {}

impl WriterChild for ParagraphWriterService {
    fn stop(&mut self) -> NodeResult<()> {
        info!("Stopping Paragraph Service");
        Ok(())
    }
    #[tracing::instrument(skip_all)]
    fn count(&self) -> usize {
        let id: Option<String> = None;
        let time = SystemTime::now();
        let reader = self.index.reader().unwrap();
        let searcher = reader.searcher();
        let count = searcher.search(&AllQuery, &Count).unwrap_or(0);
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            info!("{id:?} - Ending at: {v} ms");
        }
        count
    }
    #[tracing::instrument(skip_all)]
    fn set_resource(&mut self, resource: &Resource) -> NodeResult<()> {
        let id = Some(&resource.shard_id);
        let time = SystemTime::now();

        if resource.status != ResourceStatus::Delete as i32 {
            if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
                info!("{id:?} - Indexing paragraphs: starts at {v} ms");
            }
            let _ = self.index_paragraph(resource);
            if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
                info!("{id:?} - Indexing paragraphs: ends at {v} ms");
            }
        }

        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            info!("{id:?} - Processing paragraphs to delete: starts at {v} ms");
        }
        for paragraph_id in &resource.paragraphs_to_delete {
            let uuid_term = Term::from_field_text(self.schema.paragraph, paragraph_id);
            self.writer.delete_term(uuid_term);
        }
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            info!("{id:?} - Processing paragraphs to delete: ends at {v} ms");
        }
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            info!("{id:?} - Commit: starts at {v} ms");
        }
        self.writer.commit()?;
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            info!("{id:?} - Commit: ends at {v} ms");
        }
        Ok(())
    }
    #[tracing::instrument(skip_all)]
    fn delete_resource(&mut self, resource_id: &ResourceId) -> NodeResult<()> {
        let id = Some(&resource_id.shard_id);
        let time = SystemTime::now();
        let uuid_field = self.schema.uuid;
        let uuid_term = Term::from_field_text(uuid_field, &resource_id.uuid);
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            info!("{id:?} - Delete term: starts at {v} ms");
        }
        self.writer.delete_term(uuid_term);
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            info!("{id:?} - Delete term: ends at {v} ms");
        }
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            info!("{id:?} - Commit: starts at {v} ms");
        }
        self.writer.commit()?;
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            info!("{id:?} - Commit: ends at {v} ms");
        }
        Ok(())
    }
    #[tracing::instrument(skip_all)]
    fn garbage_collection(&mut self) {}
}

impl ParagraphWriterService {
    #[tracing::instrument(skip_all)]
    pub fn start(config: &ParagraphConfig) -> NodeResult<Self> {
        let path = std::path::Path::new(&config.path);
        if !path.exists() {
            match ParagraphWriterService::new(config) {
                Err(e) if path.exists() => {
                    std::fs::remove_dir(path)?;
                    Err(e)
                }
                Err(e) => Err(e),
                Ok(v) => Ok(v),
            }
        } else {
            Ok(ParagraphWriterService::open(config)?)
        }
    }
    #[tracing::instrument(skip_all)]
    pub fn new(config: &ParagraphConfig) -> NodeResult<ParagraphWriterService> {
        let paragraph_schema = ParagraphSchema::default();

        fs::create_dir_all(&config.path)?;

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

        let writer = index.writer_with_num_threads(1, 6_000_000).unwrap();

        Ok(ParagraphWriterService {
            index,
            writer,
            schema: paragraph_schema,
        })
    }
    #[tracing::instrument(skip_all)]
    pub fn open(config: &ParagraphConfig) -> NodeResult<ParagraphWriterService> {
        let paragraph_schema = ParagraphSchema::default();

        let index = Index::open_in_dir(&config.path)?;

        let writer = index.writer_with_num_threads(1, 6_000_000).unwrap();

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
        let empty_paragraph = HashMap::with_capacity(0);
        let inspect_paragraph = |field: &str| {
            resource
                .paragraphs
                .get(field)
                .map_or_else(|| &empty_paragraph, |i| &i.paragraphs)
        };
        let resource_facets = resource
            .labels
            .iter()
            .map(Facet::from_text)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| tantivy::TantivyError::InvalidArgument(e.to_string()))?;
        let mut paragraph_counter = 0;
        for (field, text_info) in &resource.texts {
            let text_labels = text_info
                .labels
                .iter()
                .map(Facet::from_text)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| tantivy::TantivyError::InvalidArgument(e.to_string()))?;
            for (paragraph_id, p) in inspect_paragraph(field) {
                paragraph_counter += 1;
                let paragraph_term = Term::from_field_text(self.schema.paragraph, paragraph_id);
                let chars: Vec<char> = REGEX.replace_all(&text_info.text, " ").chars().collect();
                let start_pos = p.start as u64;
                let end_pos = p.end as u64;
                let index = p.index;
                let split = &p.split;
                let lower_bound = std::cmp::min(start_pos as usize, chars.len());
                let upper_bound = std::cmp::min(end_pos as usize, chars.len());
                let text: String = chars[lower_bound..upper_bound].iter().collect();
                let facet_field = format!("/{field}");
                let paragraph_labels = p
                    .labels
                    .iter()
                    .map(Facet::from_text)
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| tantivy::TantivyError::InvalidArgument(e.to_string()))?;

                let mut doc = doc!(
                    self.schema.uuid => resource.resource.as_ref().unwrap().uuid.as_str(),
                    self.schema.modified => timestamp_to_datetime_utc(modified),
                    self.schema.created => timestamp_to_datetime_utc(created),
                    self.schema.status => resource.status as u64,
                    self.schema.repeated_in_field => p.repeated_in_field as u64,
                );

                if let Some(ref metadata) = p.metadata {
                    doc.add_bytes(self.schema.metadata, metadata.encode_to_vec());
                }

                resource_facets
                    .iter()
                    .chain(text_labels.iter())
                    .chain(paragraph_labels.iter())
                    .cloned()
                    .for_each(|facet| doc.add_facet(self.schema.facets, facet));
                doc.add_facet(self.schema.field, Facet::from(&facet_field));
                doc.add_text(self.schema.paragraph, paragraph_id.clone());
                doc.add_text(self.schema.text, &text);
                doc.add_u64(self.schema.start_pos, start_pos);
                doc.add_u64(self.schema.end_pos, end_pos);
                doc.add_u64(self.schema.index, index);
                doc.add_text(self.schema.split, split);
                self.writer.delete_term(paragraph_term);
                self.writer.add_document(doc).unwrap();
                if paragraph_counter % 500 == 0 {
                    self.writer.commit().unwrap();
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::SystemTime;

    use nucliadb_core::protos::prost_types::Timestamp;
    use nucliadb_core::protos::resource::ResourceStatus;
    use nucliadb_core::protos::{
        IndexMetadata, IndexParagraph, IndexParagraphs, Resource, ResourceId, TextInformation,
    };
    use nucliadb_core::NodeResult;
    use tantivy::collector::{Count, TopDocs};
    use tantivy::query::{AllQuery, TermQuery};
    use tempfile::TempDir;

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

        let metadata = IndexMetadata {
            created: Some(timestamp.clone()),
            modified: Some(timestamp),
        };

        const DOC1_TI: &str = "This the first document";
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

        let p1 = IndexParagraph {
            start: 0,
            end: DOC1_P1.len() as i32,
            sentences: HashMap::new(),
            field: "body".to_string(),
            labels: vec!["/nsfw".to_string()],
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
            field: "body".to_string(),
            labels: vec![
                "/tantivy".to_string(),
                "/test".to_string(),
                "/label1".to_string(),
            ],
            index: 1,
            split: "".to_string(),
            repeated_in_field: false,
            metadata: None,
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
            repeated_in_field: false,
            metadata: None,
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
            status: ResourceStatus::Processed as i32,
            labels: vec!["/l/mylabel_resource".to_string()],
            paragraphs,
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
    fn test_new_writer() -> NodeResult<()> {
        let dir = TempDir::new().unwrap();
        let psc = ParagraphConfig {
            path: dir.path().join("paragraphs"),
        };

        let mut paragraph_writer_service = ParagraphWriterService::start(&psc).unwrap();
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
