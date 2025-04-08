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

pub mod prefilter;
mod query_io;
mod reader;
mod request_types;
mod resource_indexer;
mod schema;
mod search_query;

use std::path::Path;

use nidx_protos::{DocumentItem, DocumentSearchResponse, StreamRequest};
use nidx_tantivy::index_reader::{DeletionQueryBuilder, open_index_with_deletions};
use nidx_tantivy::{TantivyIndexer, TantivyMeta, TantivySegmentMetadata};
use nidx_types::OpenIndexMetadata;
use nidx_types::prefilter::PrefilterResult;
use prefilter::PreFilterRequest;
use reader::TextReaderService;
use resource_indexer::index_document;
use schema::TextSchema;
use schema::encode_field_id_bytes;
use serde::{Deserialize, Serialize};
use tantivy::indexer::merge_indices;
use tantivy::schema::Field;
use tantivy::{
    Term,
    directory::MmapDirectory,
    query::{Query, TermSetQuery},
};
use tracing::*;
use uuid::Uuid;

pub use request_types::DocumentSearchRequest;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TextConfig {
    #[serde(default = "default_version")]
    pub version: u64,
}

impl Default for TextConfig {
    fn default() -> Self {
        Self {
            // This is the default version when creating a new index.
            // Should typically be set to the latest supported version
            version: 4,
        }
    }
}

// This is the default version when reading from serde, i.e: no info on database
// This should always be 1
fn default_version() -> u64 {
    1
}

pub struct TextIndexer;

pub struct TextDeletionQueryBuilder {
    resource_uuid: Field,
    encoded_field_id_bytes: Field,
}
impl DeletionQueryBuilder for TextDeletionQueryBuilder {
    fn query<'a>(&self, keys: impl Iterator<Item = &'a String>) -> Box<dyn Query> {
        Box::new(TermSetQuery::new(keys.filter_map(|k| {
            // Our keys can be resource ids or encoded field ids (e.g: uuid/t/text)
            // match the corresponding tantivy field
            let is_field = k.len() > 32;
            if is_field {
                let Ok(uuid) = Uuid::parse_str(&k[0..32]) else {
                    error!(?k, "Invalid deletion key in nidx_text");
                    return None;
                };
                let field_id = &k[33..];
                Some(Term::from_field_bytes(
                    self.encoded_field_id_bytes,
                    &encode_field_id_bytes(uuid, field_id)[..],
                ))
            } else {
                Some(Term::from_field_bytes(self.resource_uuid, k.as_bytes()))
            }
        })))
    }
}
impl TextDeletionQueryBuilder {
    fn new(schema: &TextSchema) -> Self {
        TextDeletionQueryBuilder {
            resource_uuid: schema.schema.get_field("uuid").unwrap(),
            encoded_field_id_bytes: schema.schema.get_field("encoded_field_id_bytes").unwrap(),
        }
    }
}

impl TextIndexer {
    #[instrument(name = "text::index_resource", skip_all)]
    pub fn index_resource(
        &self,
        output_dir: &Path,
        config: TextConfig,
        resource: &nidx_protos::Resource,
    ) -> anyhow::Result<Option<TantivySegmentMetadata>> {
        if resource.skip_texts {
            return Ok(None);
        }

        let field_schema = TextSchema::new(config.version);
        let mut indexer = TantivyIndexer::new(output_dir.to_path_buf(), field_schema.schema.clone())?;

        index_document(&mut indexer, resource, field_schema)?;
        indexer.finalize()
    }

    pub fn deletions_for_resource(&self, resource: &nidx_protos::Resource) -> Vec<String> {
        resource.texts_to_delete.clone()
    }

    #[instrument(name = "text::merge", skip_all)]
    pub fn merge(
        &self,
        work_dir: &Path,
        config: TextConfig,
        open_index: impl OpenIndexMetadata<TantivyMeta>,
    ) -> anyhow::Result<TantivySegmentMetadata> {
        let text_schema = TextSchema::new(config.version);
        let query_builder = TextDeletionQueryBuilder::new(&text_schema);
        let schema = text_schema.clone().schema;
        let index = open_index_with_deletions(schema, open_index, query_builder)?;

        let output_index = merge_indices(&[index], MmapDirectory::open(work_dir)?)?;
        let segment = &output_index.searchable_segment_metas()?[0];

        Ok(TantivySegmentMetadata {
            path: work_dir.to_path_buf(),
            records: segment.num_docs() as usize,
            index_metadata: TantivyMeta {
                segment_id: segment.id().uuid_string(),
            },
        })
    }
}

pub struct TextSearcher {
    pub reader: TextReaderService,
}

impl TextSearcher {
    #[instrument(name = "text::open", skip_all)]
    pub fn open(config: TextConfig, open_index: impl OpenIndexMetadata<TantivyMeta>) -> anyhow::Result<Self> {
        let schema = TextSchema::new(config.version);
        let index = open_index_with_deletions(
            schema.schema.clone(),
            open_index,
            TextDeletionQueryBuilder::new(&schema),
        )?;

        Ok(Self {
            reader: TextReaderService {
                index: index.clone(),
                schema,
                reader: index
                    .reader_builder()
                    .reload_policy(tantivy::ReloadPolicy::Manual)
                    .try_into()?,
            },
        })
    }

    #[instrument(name = "text::search", skip_all)]
    pub fn search(&self, request: &DocumentSearchRequest) -> anyhow::Result<DocumentSearchResponse> {
        self.reader.search(request)
    }

    #[instrument(name = "text::prefilter", skip_all)]
    pub fn prefilter(&self, request: &PreFilterRequest) -> anyhow::Result<PrefilterResult> {
        self.reader.prefilter(request)
    }

    pub fn iterator(&self, request: &StreamRequest) -> anyhow::Result<impl Iterator<Item = DocumentItem> + use<>> {
        self.reader.iterator(request)
    }

    pub fn space_usage(&self) -> usize {
        let usage = self.reader.reader.searcher().space_usage();
        if let Ok(usage) = usage {
            usage.total().get_bytes() as usize
        } else {
            0
        }
    }
}
