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

mod reader;
mod resource_indexer;
mod schema;
pub mod search;

pub use tantivy::DateTime;

use std::collections::HashSet;
use std::path::Path;

use nidx_tantivy::{
    TantivyIndexer, TantivyMeta, TantivySegmentMetadata,
    index_reader::{DeletionQueryBuilder, open_index_with_deletions},
};
use nidx_types::OpenIndexMetadata;

use reader::JsonReaderService;
use resource_indexer::index_json_fields;
use schema::JsonSchema;
use search::{JsonSearchRequest, build_tantivy_query};
use tantivy::{
    Term,
    directory::MmapDirectory,
    indexer::merge_indices,
    query::{Query, TermSetQuery},
    schema::Field,
};
use tracing::instrument;
use uuid::Uuid;

struct JsonDeletionQueryBuilder {
    rid: Field,
}

impl DeletionQueryBuilder for JsonDeletionQueryBuilder {
    fn query<'a>(&self, keys: impl Iterator<Item = &'a String>) -> Box<dyn Query> {
        Box::new(TermSetQuery::new(keys.filter_map(|k| {
            // Keys are either a bare resource UUID (32 hex chars) or a
            // "{uuid}/{field}" string. In both cases we delete by resource UUID.
            let raw_uuid = if k.len() > 32 { &k[..32] } else { k.as_str() };
            Uuid::parse_str(raw_uuid)
                .ok()
                .map(|_| Term::from_field_bytes(self.rid, raw_uuid.as_bytes()))
        })))
    }
}

impl JsonDeletionQueryBuilder {
    fn new(schema: &JsonSchema) -> Self {
        JsonDeletionQueryBuilder {
            rid: schema.schema.get_field("uuid").unwrap(),
        }
    }
}

pub struct JsonIndexer;

impl JsonIndexer {
    #[instrument(name = "json::index_resource", skip_all)]
    pub fn index_resource(
        &self,
        output_dir: &Path,
        resource: &nidx_protos::Resource,
    ) -> anyhow::Result<Option<TantivySegmentMetadata>> {
        if resource.skip_json {
            return Ok(None);
        }
        let field_schema = JsonSchema::new();
        let mut indexer = TantivyIndexer::new(output_dir.to_path_buf(), field_schema.schema.clone())?;

        index_json_fields(&mut indexer, resource, field_schema)?;
        indexer.finalize()
    }

    pub fn deletions_for_resource(&self, resource: &nidx_protos::Resource) -> Vec<String> {
        resource.json_fields_to_delete.clone()
    }

    #[instrument(name = "json::merge", skip_all)]
    pub fn merge(
        &self,
        work_dir: &Path,
        open_index: impl OpenIndexMetadata<TantivyMeta>,
    ) -> anyhow::Result<TantivySegmentMetadata> {
        let field_schema = JsonSchema::new();
        let deletion_query = JsonDeletionQueryBuilder::new(&field_schema);
        let index = open_index_with_deletions(field_schema.schema, open_index, deletion_query)?;

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

pub struct JsonSearcher {
    pub reader: JsonReaderService,
}

impl JsonSearcher {
    #[instrument(name = "json::open", skip_all)]
    pub fn open(open_index: impl OpenIndexMetadata<TantivyMeta>) -> anyhow::Result<Self> {
        let schema = JsonSchema::new();
        let index = open_index_with_deletions(
            schema.schema.clone(),
            open_index,
            JsonDeletionQueryBuilder::new(&schema),
        )?;

        Ok(Self {
            reader: JsonReaderService {
                schema: JsonSchema::new(),
                reader: index
                    .reader_builder()
                    .reload_policy(tantivy::ReloadPolicy::Manual)
                    .try_into()?,
                index,
            },
        })
    }

    #[instrument(name = "json::search", skip_all)]
    pub fn search(&self, request: &JsonSearchRequest) -> anyhow::Result<HashSet<Uuid>> {
        let query = build_tantivy_query(&request.filter, self.reader.schema.json);
        self.reader.search(&*query)
    }

    pub fn space_usage(&self) -> usize {
        self.reader.space_usage()
    }
}
