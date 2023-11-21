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

use nucliadb_core::prelude::*;
use nucliadb_core::protos::prost::Message;
use nucliadb_core::protos::resource::ResourceStatus;
use nucliadb_core::protos::{Resource, ResourceId};
use nucliadb_core::tracing::{self, *};
use nucliadb_core::IndexFiles;
use nucliadb_procs::measure;
use tantivy::collector::Count;
use tantivy::query::AllQuery;
use tantivy::schema::Term;
use tantivy::{doc, Index, IndexSettings, IndexWriter};

use crate::io_maps;
use crate::schema::{normalize, Schema};

const TANTIVY_INDEX_ARENA_MEMORY: usize = 4_000_000;

pub struct RelationsWriterService {
    index: Index,
    pub schema: Schema,
    writer: IndexWriter,
    config: RelationConfig,
}

impl Debug for RelationsWriterService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FieldWriterService")
            .field("index", &self.index)
            .field("schema", &self.schema)
            .finish()
    }
}

impl FieldWriter for RelationsWriterService {}

impl RelationWriter for RelationsWriterService {}

impl WriterChild for RelationsWriterService {
    #[measure(actor = "texts", metric = "count")]
    #[tracing::instrument(skip_all)]
    fn count(&self) -> NodeResult<usize> {
        let reader = self.index.reader()?;
        let searcher = reader.searcher();
        let count = searcher.search(&AllQuery, &Count)?;

        Ok(count)
    }

    #[measure(actor = "texts", metric = "set_resource")]
    #[tracing::instrument(skip_all)]
    fn set_resource(&mut self, resource: &Resource) -> NodeResult<()> {
        let resource_id = resource.resource.as_ref().expect("Missing resource ID");
        let uuid_field = self.schema.resource_id;
        let uuid_term = Term::from_field_text(uuid_field, &resource_id.uuid);
        self.writer.delete_term(uuid_term);
        if resource.status != ResourceStatus::Delete as i32 {
            self.index_document(resource)?;
        }

        self.writer.commit()?;
        Ok(())
    }

    #[measure(actor = "texts", metric = "delete_resource")]
    #[tracing::instrument(skip_all)]
    fn delete_resource(&mut self, resource_id: &ResourceId) -> NodeResult<()> {
        let uuid_field = self.schema.resource_id;
        let uuid_term = Term::from_field_text(uuid_field, &resource_id.uuid);
        self.writer.delete_term(uuid_term);
        self.writer.commit()?;
        Ok(())
    }

    fn garbage_collection(&mut self) -> NodeResult<()> {
        Ok(())
    }

    fn get_segment_ids(&self) -> NodeResult<Vec<String>> {
        Ok(self
            .index
            .searchable_segment_ids()?
            .iter()
            .map(|s| s.uuid_string())
            .collect())
    }

    fn get_index_files(&self, ignored_segment_ids: &[String]) -> NodeResult<IndexFiles> {
        // Should be called along with a lock at a higher level to be safe
        let mut meta_files = HashMap::new();
        let path = self.config.path.join("meta.json");
        meta_files.insert("text/meta.json".to_string(), fs::read(path)?);

        let mut files = Vec::new();

        for segment_meta in self.index.searchable_segment_metas()? {
            if ignored_segment_ids.contains(&segment_meta.id().uuid_string()) {
                continue;
            }
            for seg_file in segment_meta.list_files() {
                files.push(format!("text/{}", seg_file.to_string_lossy()));
            }
        }

        if files.is_empty() {
            // exit with no changes
            return Ok(IndexFiles {
                metadata_files: HashMap::new(),
                files,
            });
        }

        Ok(IndexFiles {
            metadata_files: meta_files,
            files,
        })
    }
}

impl RelationsWriterService {
    #[tracing::instrument(skip_all)]
    pub fn start(config: &RelationConfig) -> NodeResult<Self> {
        let path = std::path::Path::new(&config.path);
        if !path.exists() {
            Self::new(config)
        } else {
            Self::open(config)
        }
    }

    #[tracing::instrument(skip_all)]
    pub fn open(config: &RelationConfig) -> NodeResult<Self> {
        let field_schema = Schema::new();
        let index = Index::open_in_dir(&config.path)?;
        let writer = index
            .writer_with_num_threads(1, TANTIVY_INDEX_ARENA_MEMORY)
            .unwrap();

        Ok(RelationsWriterService {
            index,
            writer,
            schema: field_schema,
            config: config.clone(),
        })
    }

    #[tracing::instrument(skip_all)]
    fn new(config: &RelationConfig) -> NodeResult<Self> {
        Self::try_create_index_dir(&config.path)?;

        let settings = IndexSettings {
            ..Default::default()
        };
        let field_schema = Schema::new();
        let mut index_builder = Index::builder().schema(field_schema.schema.clone());
        index_builder = index_builder.settings(settings);
        let index = index_builder
            .create_in_dir(&config.path)
            .expect("Index directory should exist");
        let writer = index
            .writer_with_num_threads(1, TANTIVY_INDEX_ARENA_MEMORY)
            .unwrap();

        Ok(RelationsWriterService {
            index,
            writer,
            schema: field_schema,
            config: config.clone(),
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
        let resource_id = resource
            .resource
            .as_ref()
            .map(|r| r.uuid.as_str())
            .expect("Missing resource ID");

        let iter = resource
            .relations
            .iter()
            .filter(|rel| rel.to.is_some() || rel.source.is_some());

        for relation in iter {
            let source = relation.source.as_ref().expect("Missing source");
            let source_value = source.value.as_str();
            let source_type = io_maps::node_type_to_u64(source.ntype());
            let soruce_subtype = source.subtype.as_str();

            let target = relation.to.as_ref().expect("Missing target");
            let target_value = target.value.as_str();
            let target_type = io_maps::node_type_to_u64(target.ntype());
            let target_subtype = target.subtype.as_str();

            let label = relation.relation_label.as_str();
            let relationship = io_maps::relation_type_to_u64(relation.relation());
            let normalized_source_value = normalize(source_value);
            let normalized_target_value = normalize(target_value);

            let mut new_doc = doc!(
                self.schema.normalized_source_value => normalized_source_value,
                self.schema.normalized_target_value => normalized_target_value,
                self.schema.resource_id => resource_id,
                self.schema.source_value => source_value,
                self.schema.source_type => source_type,
                self.schema.source_subtype => soruce_subtype,
                self.schema.target_value => target_value,
                self.schema.target_type => target_type,
                self.schema.target_subtype => target_subtype,
                self.schema.relationship => relationship,
                self.schema.label => label,
            );

            if let Some(metadata) = relation.metadata.as_ref() {
                let encoded_metadata = metadata.encode_to_vec();
                new_doc.add_bytes(self.schema.label, encoded_metadata);
            }

            self.writer.add_document(new_doc)?;
        }
        Ok(())
    }
}
