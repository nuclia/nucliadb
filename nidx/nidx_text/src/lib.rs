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
mod resource_indexer;
mod schema;
mod search_query;

use std::path::Path;

use nidx_protos::{DocumentSearchRequest, DocumentSearchResponse, StreamRequest};
use nidx_tantivy::index_reader::{open_index_with_deletions, DeletionQueryBuilder};
use nidx_tantivy::{TantivyIndexer, TantivyMeta, TantivySegmentMetadata};
use nidx_types::OpenIndexMetadata;
use prefilter::{PreFilterRequest, PreFilterResponse};
use reader::{DocumentIterator, TextReaderService};
use resource_indexer::index_document;
use schema::TextSchema;

pub use search_query::TextContext;
use tantivy::indexer::merge_indices;
use tantivy::schema::Field;
use tantivy::{
    directory::MmapDirectory,
    query::{Query, TermSetQuery},
    Term,
};

pub struct TextIndexer;

pub struct TextDeletionQueryBuilder(Field);
impl DeletionQueryBuilder for TextDeletionQueryBuilder {
    fn query<'a>(&self, keys: impl Iterator<Item = &'a String>) -> Box<dyn Query> {
        Box::new(TermSetQuery::new(keys.map(|k| Term::from_field_bytes(self.0, k.as_bytes()))))
    }
}

impl TextIndexer {
    pub fn index_resource(
        &self,
        output_dir: &Path,
        resource: &nidx_protos::Resource,
    ) -> anyhow::Result<Option<TantivySegmentMetadata>> {
        let field_schema = TextSchema::new();
        let mut indexer = TantivyIndexer::new(output_dir.to_path_buf(), field_schema.schema.clone())?;

        index_document(&mut indexer, resource, field_schema)?;
        Ok(Some(indexer.finalize()?))
    }

    pub fn deletions_for_resource(&self, resource: &nidx_protos::Resource) -> Vec<String> {
        vec![resource.resource.as_ref().unwrap().uuid.clone()]
    }

    pub fn merge(
        &self,
        work_dir: &Path,
        open_index: impl OpenIndexMetadata<TantivyMeta>,
    ) -> anyhow::Result<TantivySegmentMetadata> {
        let schema = TextSchema::new().schema;
        let query_builder = TextDeletionQueryBuilder(schema.get_field("uuid").unwrap());
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
    pub fn open(open_index: impl OpenIndexMetadata<TantivyMeta>) -> anyhow::Result<Self> {
        let schema = TextSchema::new().schema;
        let index = open_index_with_deletions(
            schema.clone(),
            open_index,
            TextDeletionQueryBuilder(schema.get_field("uuid").unwrap()),
        )?;

        Ok(Self {
            reader: TextReaderService {
                index: index.clone(),
                schema: TextSchema::new(),
                reader: index.reader_builder().reload_policy(tantivy::ReloadPolicy::Manual).try_into()?,
            },
        })
    }

    pub fn search(
        &self,
        request: &DocumentSearchRequest,
        context: &TextContext,
    ) -> anyhow::Result<DocumentSearchResponse> {
        self.reader.search(request, context)
    }

    pub fn prefilter(&self, request: &PreFilterRequest) -> anyhow::Result<PreFilterResponse> {
        self.reader.prefilter(request)
    }

    pub fn iterator(&self, request: &StreamRequest) -> anyhow::Result<DocumentIterator> {
        self.reader.iterator(request)
    }
}
