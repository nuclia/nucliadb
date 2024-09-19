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
use std::path::Path;
use std::time::Instant;

use nucliadb_core::prelude::*;
use nucliadb_core::protos::resource::ResourceStatus;
use nucliadb_core::protos::{Resource, ResourceId};
use nucliadb_core::texts::*;
use nucliadb_core::tracing::{self, *};
use nucliadb_core::{tantivy_replica, IndexFiles};
use nucliadb_procs::measure;
use tantivy::collector::Count;
use tantivy::query::AllQuery;
use tantivy::schema::*;
use tantivy::{doc, Index, IndexSettings, IndexSortByField, IndexWriter, Order};

use super::schema::{timestamp_to_datetime_utc, TextSchema};

const TANTIVY_INDEX_ARENA_MEMORY: usize = 6_000_000;

pub struct TextWriterService {
    index: Index,
    pub schema: TextSchema,
    writer: IndexWriter,
    config: TextConfig,
}

impl Debug for TextWriterService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FieldWriterService").field("index", &self.index).field("schema", &self.schema).finish()
    }
}

impl FieldWriter for TextWriterService {
    #[measure(actor = "texts", metric = "count")]
    #[tracing::instrument(skip_all)]
    fn count(&self) -> NodeResult<usize> {
        let time = Instant::now();

        let id: Option<String> = None;
        let reader = self.index.reader()?;
        let searcher = reader.searcher();
        let count = searcher.search(&AllQuery, &Count)?;

        let took = time.elapsed().as_secs_f64();
        debug!("{id:?} - Ending at {took}");

        Ok(count)
    }

    #[measure(actor = "texts", metric = "set_resource")]
    #[tracing::instrument(skip_all)]
    fn set_resource(&mut self, resource: &Resource) -> NodeResult<()> {
        let id = Some(&resource.shard_id);
        let Some(resource_id) = resource.resource.as_ref() else {
            return Err(node_error!("Missing resource ID"));
        };

        let v = time.elapsed().as_millis();
        debug!("{id:?} - Delete existing uuid: starts at {v} ms");

        let uuid_field = self.schema.uuid;
        let uuid_term = Term::from_field_bytes(uuid_field, resource_id.uuid.as_bytes());
        self.writer.delete_term(uuid_term);
        let v = time.elapsed().as_millis();
        debug!("{id:?} - Delete existing uuid: ends at {v} ms");

        let v = time.elapsed().as_millis();
        debug!("{id:?} - Indexing document: starts at {v} ms");

        if resource.status != ResourceStatus::Delete as i32 {
            self.index_document(resource)?;
        }
        let v = time.elapsed().as_millis();
        debug!("{id:?} - Indexing document: ends at {v} ms");

        let v = time.elapsed().as_millis();
        debug!("{id:?} - Commit: starts at {v} ms");

        self.writer.commit()?;
        let v = time.elapsed().as_millis();
        debug!("{id:?} - Commit: ends at {v} ms");

        let took = time.elapsed().as_secs_f64();
        debug!("{id:?} - Ending at {took}");

        Ok(())
    }

    #[measure(actor = "texts", metric = "delete_resource")]
    #[tracing::instrument(skip_all)]
    fn delete_resource(&mut self, resource_id: &ResourceId) -> NodeResult<()> {
        let time = Instant::now();
        let id = Some(&resource_id.shard_id);

        let v = time.elapsed().as_millis();
        debug!("{id:?} - Delete existing uuid: starts at {v} ms");

        let uuid_field = self.schema.uuid;
        let uuid_term = Term::from_field_text(uuid_field, &resource_id.uuid);
        self.writer.delete_term(uuid_term);
        let v = time.elapsed().as_millis();
        debug!("{id:?} - Delete existing uuid: ends at {v} ms");

        let v = time.elapsed().as_millis();
        debug!("{id:?} - Commit: starts at {v} ms");

        self.writer.commit()?;
        let v = time.elapsed().as_millis();
        debug!("{id:?} - Commit: ends at {v} ms");

        let took = time.elapsed().as_secs_f64();
        debug!("{id:?} - Ending at {took}");

        Ok(())
    }

    fn garbage_collection(&mut self) -> NodeResult<()> {
        Ok(())
    }

    fn get_segment_ids(&self) -> NodeResult<Vec<String>> {
        Ok(self.index.searchable_segment_ids()?.iter().map(|s| s.uuid_string()).collect())
    }

    fn get_index_files(&self, ignored_segment_ids: &[String]) -> NodeResult<IndexFiles> {
        let params = tantivy_replica::ReplicationParameters {
            path: &self.config.path,
            on_replica: ignored_segment_ids,
        };
        let safe_state = tantivy_replica::compute_safe_replica_state(params, &self.index)?;
        Ok(IndexFiles::Tantivy(safe_state))
    }
}

impl TextWriterService {
    #[tracing::instrument(skip_all)]
    pub fn open(config: &TextConfig) -> NodeResult<Self> {
        let field_schema = TextSchema::new();
        let index = Index::open_in_dir(&config.path)?;
        let writer = index.writer_with_num_threads(1, TANTIVY_INDEX_ARENA_MEMORY).unwrap();

        Ok(TextWriterService {
            index,
            writer,
            schema: field_schema,
            config: config.clone(),
        })
    }

    #[tracing::instrument(skip_all)]
    pub fn create(config: TextConfig) -> NodeResult<Self> {
        Self::try_create_index_dir(&config.path)?;

        let settings = IndexSettings {
            sort_by_field: Some(IndexSortByField {
                field: "created".to_string(),
                order: Order::Desc,
            }),
            ..Default::default()
        };
        let field_schema = TextSchema::new();
        let mut index_builder = Index::builder().schema(field_schema.schema.clone());
        index_builder = index_builder.settings(settings);
        let index = index_builder.create_in_dir(&config.path).expect("Index directory should exist");
        let writer = index.writer_with_num_threads(1, TANTIVY_INDEX_ARENA_MEMORY).unwrap();

        Ok(TextWriterService {
            index,
            writer,
            schema: field_schema,
            config,
        })
    }

    fn try_create_index_dir(path: &Path) -> NodeResult<()> {
        let result = fs::create_dir(path);
        if let Err(error) = result {
            if path.exists() {
                // operation failed but directory exists, we must delete it
                if let Err(remove_error) = std::fs::remove_dir(path) {
                    return Err(node_error!(
                        "Double error creating and removing texts directory: \nFirst: {error} \
                         \nSecond: {remove_error}"
                    ));
                }
            }
            return Err(node_error!("Error while creating texts directory: {error}"));
        }

        Ok(())
    }

    fn index_document(&mut self, resource: &Resource) -> NodeResult<()> {
        let Some(resource_id) = resource.resource.as_ref().map(|r| r.uuid.as_str()) else {
            return Err(node_error!("Missing resource ID"));
        };
        let Some(metadata) = resource.metadata.as_ref() else {
            return Err(node_error!("Missing resource metadata"));
        };
        let Some(modified) = metadata.modified.as_ref() else {
            return Err(node_error!("Missing resource modified date in metadata"));
        };
        let Some(created) = metadata.created.as_ref() else {
            return Err(node_error!("Missing resource created date in metadata"));
        };

        let mut base_doc = doc!(
            self.schema.uuid => resource_id.as_bytes(),
            self.schema.modified => timestamp_to_datetime_utc(modified),
            self.schema.created => timestamp_to_datetime_utc(created),
            self.schema.status => resource.status as u64,
        );

        let resource_security = resource.security.as_ref();
        if let Some(security_groups) = resource_security.filter(|i| !i.access_groups.is_empty()) {
            base_doc.add_u64(self.schema.groups_public, 0_u64);
            for group_id in security_groups.access_groups.iter() {
                let mut group_id_key = group_id.clone();
                if !group_id.starts_with('/') {
                    // Slash needs to be added to be compatible with tantivy facet fields
                    group_id_key = "/".to_string() + group_id;
                }
                let facet = Facet::from(group_id_key.as_str());
                base_doc.add_facet(self.schema.groups_with_access, facet)
            }
        } else {
            base_doc.add_u64(self.schema.groups_public, 1_u64);
        }

        for label in resource.labels.iter() {
            let facet = Facet::from(label.as_str());
            base_doc.add_facet(self.schema.facets, facet);
        }

        for (field, text_info) in &resource.texts {
            let mut field_doc = base_doc.clone();
            let mut facet_key: String = "/".to_owned();
            facet_key.push_str(field.as_str());
            let facet_field = Facet::from(facet_key.as_str());
            field_doc.add_facet(self.schema.field, facet_field);
            field_doc.add_text(self.schema.text, &text_info.text);

            for label in text_info.labels.iter() {
                let facet = Facet::from(label.as_str());
                field_doc.add_facet(self.schema.facets, facet);
            }
            self.writer.add_document(field_doc).unwrap();
        }

        Ok(())
    }
}
