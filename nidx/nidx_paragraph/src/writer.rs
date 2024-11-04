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
use std::path::Path;
use std::time::Instant;

use itertools::Itertools;
use nidx_protos::prost::*;
use nidx_protos::resource::ResourceStatus;
use nidx_protos::{Resource, ResourceId};
use regex::Regex;
use tantivy::collector::Count;
use tantivy::query::AllQuery;
use tantivy::schema::*;
use tantivy::{doc, Index, IndexSettings, IndexWriter};
use tracing::*;

use super::schema::ParagraphSchema;
use crate::schema::timestamp_to_datetime_utc;
use crate::search_response::is_label;

lazy_static::lazy_static! {
    static ref REGEX: Regex = Regex::new(r"\\[a-zA-Z0-9]").unwrap();
}

pub struct ParagraphWriterService {
    pub index: Index,
    pub schema: ParagraphSchema,
    writer: IndexWriter,
}

impl Debug for ParagraphWriterService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TextService").field("index", &self.index).field("schema", &self.schema).finish()
    }
}

impl ParagraphWriterService {
    fn count(&self) -> anyhow::Result<usize> {
        let time = Instant::now();
        let id: Option<String> = None;

        let v = time.elapsed().as_millis();
        debug!("{id:?} - Count starting at {v} ms");

        let reader = self.index.reader()?;
        let searcher = reader.searcher();
        let count = searcher.search(&AllQuery, &Count)?;
        let v = time.elapsed().as_millis();
        debug!("{id:?} - Ending at: {v} ms");

        Ok(count)
    }

    pub fn set_resource(&mut self, resource: &Resource) -> anyhow::Result<()> {
        let time = Instant::now();
        let id = Some(&resource.shard_id);

        let v = time.elapsed().as_millis();
        debug!("{id:?} - Processing paragraphs to delete: starts at {v} ms");

        // Delete all paragraphs matching the field_uuid
        for field_or_paragraph_id in &resource.paragraphs_to_delete {
            let field_uuid_term = Term::from_field_text(self.schema.field_uuid, field_or_paragraph_id);
            self.writer.delete_term(field_uuid_term);
        }

        self.writer.commit()?;

        let v = time.elapsed().as_millis();
        debug!("{id:?} - Processing paragraphs to delete: ends at {v} ms");

        if resource.status != ResourceStatus::Delete as i32 {
            let v = time.elapsed().as_millis();
            debug!("{id:?} - Indexing paragraphs: starts at {v} ms");

            let _ = self.index_paragraph(resource);
            let v = time.elapsed().as_millis();
            debug!("{id:?} - Indexing paragraphs: ends at {v} ms");
        }

        let v = time.elapsed().as_millis();
        debug!("{id:?} - Commit: starts at {v} ms");

        self.writer.commit()?;
        let v = time.elapsed().as_millis();
        debug!("{id:?} - Commit: ends at {v} ms");

        Ok(())
    }

    fn delete_resource(&mut self, resource_id: &ResourceId) -> anyhow::Result<()> {
        let time = Instant::now();
        let id = Some(&resource_id.shard_id);

        let uuid_field = self.schema.uuid;
        let uuid_term = Term::from_field_text(uuid_field, &resource_id.uuid);
        let v = time.elapsed().as_millis();
        debug!("{id:?} - Delete term: starts at {v} ms");

        self.writer.delete_term(uuid_term);
        let v = time.elapsed().as_millis();
        debug!("{id:?} - Delete term: ends at {v} ms");

        let v = time.elapsed().as_millis();
        debug!("{id:?} - Commit: starts at {v} ms");

        self.writer.commit()?;
        let v = time.elapsed().as_millis();
        debug!("{id:?} - Commit: ends at {v} ms");

        Ok(())
    }

    fn garbage_collection(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
    fn get_segment_ids(&self) -> anyhow::Result<Vec<String>> {
        Ok(self.index.searchable_segment_ids()?.iter().map(|s| s.uuid_string()).collect())
    }
}

impl ParagraphWriterService {
    pub fn create(path: &Path) -> anyhow::Result<ParagraphWriterService> {
        let paragraph_schema = ParagraphSchema::default();

        fs::create_dir(path)?;

        let mut index_builder = Index::builder().schema(paragraph_schema.schema.clone());
        let settings = IndexSettings {
            ..Default::default()
        };

        index_builder = index_builder.settings(settings);
        let index = index_builder.create_in_dir(path)?;

        let writer = index.writer_with_num_threads(1, 15_000_000)?;

        Ok(ParagraphWriterService {
            index,
            writer,
            schema: paragraph_schema,
        })
    }
    pub fn open(path: &Path) -> anyhow::Result<ParagraphWriterService> {
        let paragraph_schema = ParagraphSchema::default();

        let index = Index::open_in_dir(path)?;

        let writer = index.writer_with_num_threads(1, 15_000_000)?;

        Ok(ParagraphWriterService {
            index,
            writer,
            schema: paragraph_schema,
        })
    }

    fn index_paragraph(&mut self, resource: &Resource) -> anyhow::Result<()> {
        let Some(metadata) = resource.metadata.as_ref() else {
            return Err(anyhow::anyhow!("Missing resource metadata"));
        };
        let Some(modified) = metadata.modified.as_ref() else {
            return Err(anyhow::anyhow!("Missing resource modified date in metadata"));
        };
        let Some(created) = metadata.created.as_ref() else {
            return Err(anyhow::anyhow!("Missing resource created date in metadata"));
        };

        let empty_paragraph = HashMap::with_capacity(0);
        let inspect_paragraph =
            |field: &str| resource.paragraphs.get(field).map_or_else(|| &empty_paragraph, |i| &i.paragraphs);
        let mut paragraph_counter = 0;
        let resource_labels = resource
            .labels
            .iter()
            .map(Facet::from_text)
            .filter_ok(is_label)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| tantivy::TantivyError::InvalidArgument(e.to_string()))?;

        for (field, text_info) in &resource.texts {
            let chars: Vec<char> = REGEX.replace_all(&text_info.text, " ").chars().collect();
            let field_labels = text_info
                .labels
                .iter()
                .map(Facet::from_text)
                .filter_ok(is_label)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| tantivy::TantivyError::InvalidArgument(e.to_string()))?;

            for (paragraph_id, p) in inspect_paragraph(field) {
                paragraph_counter += 1;
                let paragraph_term = Term::from_field_text(self.schema.paragraph, paragraph_id);
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
                    self.schema.uuid => resource.resource.as_ref().expect("Missing resource details").uuid.as_str(),
                    self.schema.modified => timestamp_to_datetime_utc(modified),
                    self.schema.created => timestamp_to_datetime_utc(created),
                    self.schema.status => resource.status as u64,
                    self.schema.repeated_in_field => p.repeated_in_field as u64,
                );

                if let Some(ref metadata) = p.metadata {
                    doc.add_bytes(self.schema.metadata, metadata.encode_to_vec());
                }

                paragraph_labels
                    .into_iter()
                    .chain(field_labels.iter().cloned())
                    .chain(resource_labels.iter().cloned())
                    .for_each(|facet| doc.add_facet(self.schema.facets, facet));
                doc.add_facet(self.schema.field, Facet::from(&facet_field));
                doc.add_text(self.schema.paragraph, paragraph_id.clone());
                doc.add_text(self.schema.text, &text);
                doc.add_u64(self.schema.start_pos, start_pos);
                doc.add_u64(self.schema.end_pos, end_pos);
                doc.add_u64(self.schema.index, index);
                doc.add_text(self.schema.split, split);
                let field_uuid = format!("{}/{}", resource.resource.as_ref().unwrap().uuid, field);
                doc.add_text(self.schema.field_uuid, field_uuid.clone());

                self.writer.delete_term(paragraph_term);
                self.writer.add_document(doc)?;
                if paragraph_counter % 500 == 0 {
                    self.writer.commit()?;
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

    use nidx_protos::prost_types::Timestamp;
    use nidx_protos::resource::ResourceStatus;
    use nidx_protos::{IndexMetadata, IndexParagraph, IndexParagraphs, Resource, ResourceId, TextInformation};
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

        let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap();
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
            vectorsets_sentences: HashMap::new(),
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
    fn test_new_writer() -> anyhow::Result<()> {
        let dir = TempDir::new().unwrap();

        let mut paragraph_writer_service = ParagraphWriterService::create(&dir.path().join("paragraphs")).unwrap();
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

    #[test]
    fn test_set_resource_replaces_documents() -> anyhow::Result<()> {
        let dir = TempDir::new().unwrap();

        // Create a resource
        let mut paragraph_writer_service = ParagraphWriterService::create(&dir.path().join("paragraphs")).unwrap();
        let resource1 = create_resource("shard1".to_string());
        paragraph_writer_service.set_resource(&resource1)?;

        // Check that it exists
        let reader = paragraph_writer_service.index.reader()?;
        let searcher = reader.searcher();
        let query = TermQuery::new(
            Term::from_field_text(paragraph_writer_service.schema.text, "document"),
            IndexRecordOption::Basic,
        );
        let (_top_docs, count) = searcher.search(&query, &(TopDocs::with_limit(2), Count))?;
        assert_eq!(count, 1);

        // Edit the resource
        let mut resource_update = create_resource("shard1".to_string());
        resource_update.paragraphs_to_delete =
            resource_update.paragraphs.values().flat_map(|p| p.paragraphs.keys().cloned()).collect();
        paragraph_writer_service.set_resource(&resource_update)?;

        // Check that it is updated
        let reader = paragraph_writer_service.index.reader()?;
        let searcher = reader.searcher();
        let query = TermQuery::new(
            Term::from_field_text(paragraph_writer_service.schema.text, "document"),
            IndexRecordOption::Basic,
        );

        let (_top_docs, count) = searcher.search(&query, &(TopDocs::with_limit(2), Count))?;
        assert_eq!(count, 1);

        Ok(())
    }
}
