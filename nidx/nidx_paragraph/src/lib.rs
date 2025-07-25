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

use nidx_protos::{ParagraphItem, ParagraphSearchResponse, StreamRequest};
use nidx_tantivy::{
    TantivyIndexer, TantivyMeta, TantivySegmentMetadata,
    index_reader::{DeletionQueryBuilder, open_index_with_deletions},
};
use nidx_types::{OpenIndexMetadata, prefilter::PrefilterResult};
use reader::ParagraphReaderService;
use resource_indexer::index_paragraphs;
use schema::ParagraphSchema;
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
        resource: &nidx_protos::Resource,
    ) -> anyhow::Result<Option<TantivySegmentMetadata>> {
        if resource.skip_paragraphs {
            return Ok(None);
        }
        let field_schema = ParagraphSchema::new();
        let mut indexer = TantivyIndexer::new(output_dir.to_path_buf(), field_schema.schema.clone())?;

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
        open_index: impl OpenIndexMetadata<TantivyMeta>,
    ) -> anyhow::Result<TantivySegmentMetadata> {
        let schema = ParagraphSchema::new().schema;
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
    pub fn open(open_index: impl OpenIndexMetadata<TantivyMeta>) -> anyhow::Result<Self> {
        let schema = ParagraphSchema::new().schema;
        let index = open_index_with_deletions(schema.clone(), open_index, ParagraphDeletionQueryBuilder::new(&schema))?;

        Ok(Self {
            reader: ParagraphReaderService {
                index: index.clone(),
                schema: ParagraphSchema::new(),
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
