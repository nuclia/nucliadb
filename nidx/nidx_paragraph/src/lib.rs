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
mod reader;
mod resource_indexer;
mod schema;
mod search_query;
mod search_response;
mod set_query;
mod stop_words;

use nidx_protos::{
    resource::ResourceStatus, ParagraphSearchRequest, ParagraphSearchResponse, StreamRequest, SuggestRequest,
};
use nidx_tantivy::{
    index_reader::{open_index_with_deletions, DeletionQueryBuilder},
    TantivyIndexer, TantivyMeta, TantivySegmentMetadata,
};
use nidx_types::OpenIndexMetadata;
use reader::ParagraphReaderService;
use resource_indexer::index_paragraphs;
use schema::ParagraphSchema;
use search_query::ParagraphIterator;
pub use search_query::ParagraphsContext;
use std::{collections::HashSet, path::Path};
use tantivy::{
    directory::MmapDirectory,
    indexer::merge_indices,
    query::{Query, TermSetQuery},
    schema::Field,
    Term,
};

pub struct ParagraphIndexer;

pub struct TextDeletionQueryBuilder(Field);
impl DeletionQueryBuilder for TextDeletionQueryBuilder {
    fn query<'a>(&self, keys: impl Iterator<Item = &'a String>) -> Box<dyn Query> {
        // TODO: Support deleting by resource id and paragraph id
        Box::new(TermSetQuery::new(keys.map(|k| Term::from_field_bytes(self.0, k.as_bytes()))))
    }
}

impl ParagraphIndexer {
    pub fn index_resource(
        &self,
        output_dir: &Path,
        resource: &nidx_protos::Resource,
    ) -> anyhow::Result<Option<TantivySegmentMetadata>> {
        let field_schema = ParagraphSchema::new();
        let mut indexer = TantivyIndexer::new(output_dir.to_path_buf(), field_schema.schema.clone())?;

        if resource.status == ResourceStatus::Delete as i32 {
            return Err(anyhow::anyhow!("This is a deletion, not a set resource"));
        }

        index_paragraphs(&mut indexer, resource, field_schema)?;
        Ok(Some(indexer.finalize()?))
    }

    pub fn deletions_for_resource(&self, _resource: &nidx_protos::Resource) -> Vec<String> {
        // TODO!
        vec![]
    }

    pub fn merge(
        &self,
        work_dir: &Path,
        open_index: impl OpenIndexMetadata<TantivyMeta>,
    ) -> anyhow::Result<TantivySegmentMetadata> {
        let schema = ParagraphSchema::new().schema;
        let query_builder = TextDeletionQueryBuilder(schema.get_field("uuid").unwrap());
        let index = open_index_with_deletions(schema, open_index, query_builder)?;

        let output_index = merge_indices(&[index], MmapDirectory::open(work_dir)?)?;
        let segment = &output_index.searchable_segment_metas()?[0];

        Ok(TantivySegmentMetadata {
            path: work_dir.to_path_buf(),
            records: segment.num_docs() as usize,
            tags: HashSet::new(),
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
    pub fn open(open_index: impl OpenIndexMetadata<TantivyMeta>) -> anyhow::Result<Self> {
        let schema = ParagraphSchema::new().schema;
        let index = open_index_with_deletions(
            schema.clone(),
            open_index,
            TextDeletionQueryBuilder(schema.get_field("uuid").unwrap()),
        )?;

        Ok(Self {
            reader: ParagraphReaderService {
                index: index.clone(),
                schema: ParagraphSchema::new(),
                reader: index.reader_builder().reload_policy(tantivy::ReloadPolicy::Manual).try_into()?,
            },
        })
    }

    pub fn search(
        &self,
        request: &ParagraphSearchRequest,
        context: &ParagraphsContext,
    ) -> anyhow::Result<ParagraphSearchResponse> {
        self.reader.search(request, context)
    }

    pub fn suggest(&self, request: &SuggestRequest) -> anyhow::Result<ParagraphSearchResponse> {
        self.reader.suggest(request)
    }

    pub fn iterator(&self, request: &StreamRequest) -> anyhow::Result<ParagraphIterator> {
        self.reader.iterator(request)
    }
}
