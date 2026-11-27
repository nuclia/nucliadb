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
    query::{BooleanQuery, Query, TermSetQuery},
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

    #[instrument(name = "json::index_resources", skip_all)]
    pub fn index_resources(
        &self,
        output_dir: &Path,
        resources: &[nidx_protos::Resource],
    ) -> anyhow::Result<Option<TantivySegmentMetadata>> {
        let schema = JsonSchema::new();
        let mut indexer = TantivyIndexer::new(output_dir.to_path_buf(), schema.schema.clone())?;
        for resource in resources.iter().filter(|resource| !resource.skip_json) {
            index_json_fields(&mut indexer, resource, schema.clone())?;
        }
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

    #[instrument(name = "json::search_resource_ids", skip_all)]
    pub fn search_resource_ids(&self, request: &JsonSearchRequest, ids: &[Uuid]) -> anyhow::Result<HashSet<Uuid>> {
        if ids.is_empty() {
            return Ok(HashSet::new());
        }
        let uuid_field = self.reader.schema.schema.get_field("uuid")?;
        let resources = TermSetQuery::new(
            ids.iter()
                .map(|id| Term::from_field_bytes(uuid_field, id.simple().to_string().as_bytes())),
        );
        let filter = build_tantivy_query(&request.filter, self.reader.schema.json);
        self.reader
            .search(&BooleanQuery::intersection(vec![Box::new(resources), filter]))
    }

    #[instrument(name = "json::search_page", skip_all)]
    pub fn search_page(
        &self,
        request: &JsonSearchRequest,
        after: Option<Uuid>,
        limit: usize,
    ) -> anyhow::Result<Vec<Uuid>> {
        let query = build_tantivy_query(&request.filter, self.reader.schema.json);
        self.reader.search_page(&*query, after, limit)
    }

    pub fn space_usage(&self) -> usize {
        self.reader.space_usage()
    }
}

#[cfg(test)]
mod lpg_tests {
    use super::*;
    use crate::search::{JsonFilterExpression, JsonPathFilter, JsonPredicate};
    use tantivy::{Index, TantivyDocument, schema::OwnedValue};

    #[test]
    fn neighbor_filter_is_intersected_with_resource_ids() -> anyhow::Result<()> {
        let schema = JsonSchema::new();
        let index = Index::create_in_ram(schema.schema.clone());
        let mut writer = index.writer(15_000_000)?;
        for (number, enabled) in [(1, true), (2, false), (3, true)] {
            let id = Uuid::from_u128(number);
            let mut doc = TantivyDocument::default();
            doc.add_bytes(schema.rid, id.simple().to_string().as_bytes());
            for word in schema::encode_rid(id) {
                doc.add_u64(schema.encoded_rid, word);
            }
            doc.add_field_value(
                schema.json,
                &OwnedValue::from(serde_json::json!({
                    "a/lpg_record_v1": {"enabled": enabled}
                })),
            );
            writer.add_document(doc)?;
        }
        writer.commit()?;
        let reader = index.reader()?;
        let searcher = JsonSearcher {
            reader: JsonReaderService { schema, index, reader },
        };
        let query = JsonSearchRequest {
            filter: JsonFilterExpression::Path(JsonPathFilter {
                field_id: "a/lpg_record_v1".into(),
                json_path: "enabled".into(),
                predicate: JsonPredicate::Boolean(true),
            }),
        };
        assert_eq!(
            searcher.search_resource_ids(&query, &[Uuid::from_u128(1), Uuid::from_u128(2)])?,
            HashSet::from([Uuid::from_u128(1)])
        );
        assert!(searcher.search_resource_ids(&query, &[])?.is_empty());
        Ok(())
    }
}
