// Copyright 2021 Bosutech XXI S.L.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

mod fuzzy_query;
mod query_io;
mod query_parser;
mod reader;
mod request_types;
mod resource_indexer;
mod schema;
mod search_query;
mod search_response;
mod set_query;

use nidx_protos::{ParagraphIndexConfig, ParagraphItem, ParagraphSearchResponse, StreamRequest};
use nidx_tantivy::tokenizers::magic_char::MAGIC_CHAR_TOKENIZER_NAME;
use nidx_tantivy::{
    TantivyIndexer, TantivyIndexerOptions, TantivyMeta, TantivySegmentMetadata,
    index_reader::{DeletionQueryBuilder, open_index_with_deletions},
};
use nidx_types::{OpenIndexMetadata, prefilter::PrefilterResult};
use reader::ParagraphReaderService;
use resource_indexer::index_paragraphs;
use schema::ParagraphSchema;
use serde::{Deserialize, Serialize};
use std::path::Path;
use tantivy::{
    Term,
    directory::MmapDirectory,
    indexer::merge_indices,
    query::{Query, TermSetQuery},
    schema::{Field, Schema},
};
use tracing::instrument;

pub use request_types::*;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ParagraphConfig {
    /// Custom tokenizer used *only* for indexing
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub custom_indexing_tokenizer: Option<CustomTokenizer>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub enum CustomTokenizer {
    MagicChar,
}

impl CustomTokenizer {
    pub fn tokenizer_name(&self) -> &'static str {
        match self {
            Self::MagicChar => MAGIC_CHAR_TOKENIZER_NAME,
        }
    }
}

impl ParagraphConfig {
    pub fn from_proto(proto: ParagraphIndexConfig) -> Self {
        let custom_indexing_tokenizer = match proto.indexing_tokenizer() {
            nidx_protos::paragraph_index_config::Tokenizer::Default => None,
            nidx_protos::paragraph_index_config::Tokenizer::MagicChar => Some(CustomTokenizer::MagicChar),
        };

        Self {
            custom_indexing_tokenizer,
        }
    }
}

pub struct ParagraphIndexer;

pub struct ParagraphDeletionQueryBuilder {
    field: Field,
    resource: Field,
}
impl DeletionQueryBuilder for ParagraphDeletionQueryBuilder {
    fn query<'a>(&self, keys: impl Iterator<Item = &'a String>) -> Box<dyn Query> {
        Box::new(TermSetQuery::new(keys.map(|k| {
            // Our keys can be resource or field ids, match the corresponding tantivy field
            let is_field = k.len() > 32;
            let tantivy_field = if is_field { self.field } else { self.resource };
            Term::from_field_bytes(tantivy_field, k.as_bytes())
        })))
    }
}
impl ParagraphDeletionQueryBuilder {
    fn new(schema: &Schema) -> Self {
        ParagraphDeletionQueryBuilder {
            resource: schema.get_field("uuid").unwrap(),
            field: schema.get_field("field_uuid").unwrap(),
        }
    }
}

impl ParagraphIndexer {
    #[instrument(name = "paragraph::index_resource", skip_all)]
    pub fn index_resource(
        &self,
        output_dir: &Path,
        config: ParagraphConfig,
        resource: &nidx_protos::Resource,
    ) -> anyhow::Result<Option<TantivySegmentMetadata>> {
        if resource.skip_paragraphs {
            return Ok(None);
        }

        let indexer_options = TantivyIndexerOptions {
            // enable custom tokenizers if we are using one
            use_custom_tokenizers: config.custom_indexing_tokenizer.is_some(),
        };
        let field_schema = ParagraphSchema::new(&config);
        let mut indexer = TantivyIndexer::new(output_dir.to_path_buf(), field_schema.schema.clone(), indexer_options)?;

        index_paragraphs(&mut indexer, resource, field_schema)?;
        indexer.finalize()
    }

    pub fn deletions_for_resource(&self, resource: &nidx_protos::Resource) -> Vec<String> {
        resource.paragraphs_to_delete.clone()
    }

    #[instrument(name = "paragraph::merge", skip_all)]
    pub fn merge(
        &self,
        work_dir: &Path,
        config: ParagraphConfig,
        open_index: impl OpenIndexMetadata<TantivyMeta>,
    ) -> anyhow::Result<TantivySegmentMetadata> {
        let schema = ParagraphSchema::new(&config).schema;
        let deletions_query = ParagraphDeletionQueryBuilder::new(&schema);
        let index = open_index_with_deletions(schema, open_index, deletions_query)?;

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

pub struct ParagraphSearcher {
    pub reader: ParagraphReaderService,
}

impl ParagraphSearcher {
    #[instrument(name = "paragraph::open", skip_all)]
    pub fn open(config: ParagraphConfig, open_index: impl OpenIndexMetadata<TantivyMeta>) -> anyhow::Result<Self> {
        let schema = ParagraphSchema::new(&config.clone()).schema;
        let index = open_index_with_deletions(schema.clone(), open_index, ParagraphDeletionQueryBuilder::new(&schema))?;

        Ok(Self {
            reader: ParagraphReaderService {
                index: index.clone(),
                schema: ParagraphSchema::new(&config),
                reader: index
                    .reader_builder()
                    .reload_policy(tantivy::ReloadPolicy::Manual)
                    .try_into()?,
            },
        })
    }

    #[instrument(name = "paragraph::search", skip_all)]
    pub fn search(
        &self,
        request: &ParagraphSearchRequest,
        prefilter: &PrefilterResult,
    ) -> anyhow::Result<ParagraphSearchResponse> {
        self.reader.search(request, prefilter)
    }

    #[instrument(name = "paragraph::suggest", skip_all)]
    pub fn suggest(
        &self,
        request: &ParagraphSuggestRequest,
        prefilter: &PrefilterResult,
    ) -> anyhow::Result<ParagraphSearchResponse> {
        self.reader.suggest(request, prefilter)
    }

    pub fn iterator(&self, request: &StreamRequest) -> anyhow::Result<impl Iterator<Item = ParagraphItem> + use<>> {
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
