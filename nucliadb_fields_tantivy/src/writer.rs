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
use std::fs;

use nucliadb_protos::resource::ResourceStatus;
use nucliadb_protos::{Resource, ResourceId};
use nucliadb_service_interface::prelude::*;
use tantivy::collector::Count;
use tantivy::query::AllQuery;
use tantivy::schema::*;
use tantivy::{doc, Index, IndexSettings, IndexSortByField, IndexWriter, Order};
use tracing::*;

use super::schema::{timestamp_to_datetime_utc, FieldSchema};

pub struct FieldWriterService {
    index: Index,
    pub schema: FieldSchema,
    writer: IndexWriter,
}

impl Debug for FieldWriterService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FieldWriterService")
            .field("index", &self.index)
            .field("schema", &self.schema)
            .finish()
    }
}

impl FieldWriter for FieldWriterService {}

impl WriterChild for FieldWriterService {
    fn stop(&mut self) -> InternalResult<()> {
        info!("Stopping Text Service");
        Ok(())
    }

    fn count(&self) -> usize {
        let reader = self.index.reader().unwrap();
        let searcher = reader.searcher();
        searcher.search(&AllQuery, &Count).unwrap_or(0)
    }
    fn set_resource(&mut self, resource: &Resource) -> InternalResult<()> {
        let resource_id = resource.resource.as_ref().unwrap();

        let uuid_field = self.schema.uuid;
        let uuid_term = Term::from_field_text(uuid_field, &resource_id.uuid);
        self.writer.delete_term(uuid_term);

        if resource.status != ResourceStatus::Delete as i32 {
            self.index_document(resource);
        }
        match self.writer.commit() {
            Ok(opstamp) => trace!("Commit {}!", opstamp),
            Err(e) => error!("Error doing commit: {}", e),
        }
        Ok(())
    }
    fn delete_resource(&mut self, resource_id: &ResourceId) -> InternalResult<()> {
        let uuid_field = self.schema.uuid;
        let uuid_term = Term::from_field_text(uuid_field, &resource_id.uuid);
        self.writer.delete_term(uuid_term);
        match self.writer.commit() {
            Ok(opstamp) => {
                trace!("Commit {}!", opstamp);
                Ok(())
            }
            Err(e) => {
                error!("Error starting Text service: {}", e);
                Err(Box::new(FieldError { msg: e.to_string() }))
            }
        }
    }
    fn garbage_collection(&mut self) {}
}

impl FieldWriterService {
    pub fn start(config: &FieldConfig) -> InternalResult<Self> {
        info!("Starting Text Service");
        match FieldWriterService::open(config) {
            Ok(service) => Ok(service),
            Err(e) => {
                warn!("Field Service Open failed {}. Creating a new one.", e);
                match FieldWriterService::new(config) {
                    Ok(service) => Ok(service),
                    Err(e) => {
                        error!("FieldConfigce: {}", e);
                        Err(Box::new(FieldError { msg: e.to_string() }))
                    }
                }
            }
        }
    }

    pub fn new(config: &FieldConfig) -> InternalResult<Self> {
        match FieldWriterService::new_inner(config) {
            Ok(service) => Ok(service),
            Err(e) => Err(Box::new(FieldError { msg: e.to_string() })),
        }
    }
    pub fn open(config: &FieldConfig) -> InternalResult<Self> {
        match FieldWriterService::open_inner(config) {
            Ok(service) => Ok(service),
            Err(e) => Err(Box::new(FieldError { msg: e.to_string() })),
        }
    }
    pub fn new_inner(config: &FieldConfig) -> tantivy::Result<FieldWriterService> {
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

        let writer = index.writer_with_num_threads(1, 6_000_000).unwrap();

        Ok(FieldWriterService {
            index,
            writer,
            schema: field_schema,
        })
    }

    pub fn open_inner(config: &FieldConfig) -> tantivy::Result<FieldWriterService> {
        let field_schema = FieldSchema::new();

        let index = Index::open_in_dir(&config.path)?;

        let writer = index.writer_with_num_threads(1, 6_000_000).unwrap();

        Ok(FieldWriterService {
            index,
            writer,
            schema: field_schema,
        })
    }

    fn index_document(&mut self, resource: &Resource) {
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
        for label in resource_labels {
            let facet = Facet::from(label.as_str());
            doc.add_facet(self.schema.facets, facet);
        }

        for (field, text_info) in &resource.texts {
            let mut subdoc = doc.clone();
            let mut facet_key: String = "/".to_owned();
            facet_key.push_str(field.as_str());
            let facet_field = Facet::from(facet_key.as_str());
            subdoc.add_facet(self.schema.field, facet_field);
            subdoc.add_text(self.schema.text, &text_info.text);

            #[allow(clippy::iter_cloned_collect)]
            let field_labels: Vec<String> = text_info.labels.iter().cloned().collect();
            for label in field_labels {
                let facet = Facet::from(label.as_str());
                subdoc.add_facet(self.schema.facets, facet);
            }
            self.writer.add_document(subdoc.clone()).unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::SystemTime;

    use prost_types::Timestamp;
    use tantivy::collector::{Count, TopDocs};
    use tantivy::query::{AllQuery, TermQuery};
    use tempdir::TempDir;

    use super::*;

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
            labels: vec!["/l/mylabel".to_string()],
        };

        let ti_body = nucliadb_protos::TextInformation {
            text: DOC1_P1.to_string() + DOC1_P2 + DOC1_P3,
            labels: vec!["/f/body".to_string()],
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
    fn test_new_writer() -> anyhow::Result<()> {
        let dir = TempDir::new("payload_dir").unwrap();
        let fsc = FieldConfig {
            path: dir.path().as_os_str().to_os_string().into_string().unwrap(),
        };

        let mut field_writer_service = FieldWriterService::start(&fsc).unwrap();
        let resource1 = create_resource("shard1".to_string());
        let _ = field_writer_service.set_resource(&resource1);
        let _ = field_writer_service.set_resource(&resource1);

        let reader = field_writer_service.index.reader()?;
        let searcher = reader.searcher();

        let query = TermQuery::new(
            Term::from_field_text(field_writer_service.schema.text, "document"),
            IndexRecordOption::Basic,
        );

        let (_top_docs, count) = searcher.search(&query, &(TopDocs::with_limit(2), Count))?;
        assert_eq!(count, 1);

        let (_top_docs, count) = searcher.search(&AllQuery, &(TopDocs::with_limit(10), Count))?;
        assert_eq!(count, 2);
        Ok(())
    }
}
