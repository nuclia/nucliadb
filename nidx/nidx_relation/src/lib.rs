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

mod io_maps;
mod reader;
mod resource_indexer;
mod schema;

use nidx_protos::{resource::ResourceStatus, RelationSearchRequest, RelationSearchResponse};
use nidx_tantivy::{
    index_reader::{open_index_with_deletions, DeletionQueryBuilder},
    TantivyIndexer, TantivyMeta, TantivySegmentMetadata,
};
use nidx_types::OpenIndexMetadata;
use reader::RelationsReaderService;
use resource_indexer::index_relations;
pub use schema::Schema as RelationSchema;
use std::{collections::HashSet, path::Path};
use tantivy::{
    directory::MmapDirectory,
    indexer::merge_indices,
    query::{Query, TermSetQuery},
    schema::Field,
    Term,
};

pub struct RelationIndexer;

pub struct RelationDeletionQueryBuilder(Field);
impl DeletionQueryBuilder for RelationDeletionQueryBuilder {
    fn query<'a>(&self, keys: impl Iterator<Item = &'a String>) -> Box<dyn Query> {
        Box::new(TermSetQuery::new(keys.map(|k| Term::from_field_bytes(self.0, k.as_bytes()))))
    }
}
impl RelationDeletionQueryBuilder {
    fn new(schema: &RelationSchema) -> Self {
        RelationDeletionQueryBuilder(schema.resource_id)
    }
}

impl RelationIndexer {
    pub fn index_resource(
        &self,
        output_dir: &Path,
        resource: &nidx_protos::Resource,
    ) -> anyhow::Result<Option<TantivySegmentMetadata>> {
        let field_schema = RelationSchema::new();
        let mut indexer = TantivyIndexer::new(output_dir.to_path_buf(), field_schema.schema.clone())?;

        if resource.status == ResourceStatus::Delete as i32 {
            return Err(anyhow::anyhow!("This is a deletion, not a set resource"));
        }

        index_relations(&mut indexer, resource, field_schema)?;
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
        let schema = RelationSchema::new();
        let deletions_query = RelationDeletionQueryBuilder::new(&schema);
        let index = open_index_with_deletions(schema.schema, open_index, deletions_query)?;

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

pub struct RelationSearcher {
    pub reader: RelationsReaderService,
}

impl RelationSearcher {
    pub fn open(open_index: impl OpenIndexMetadata<TantivyMeta>) -> anyhow::Result<Self> {
        let schema = RelationSchema::new();
        let deletions_query = RelationDeletionQueryBuilder::new(&schema);
        let index = open_index_with_deletions(schema.schema, open_index, deletions_query)?;

        Ok(Self {
            reader: RelationsReaderService {
                index: index.clone(),
                schema: RelationSchema::new(),
                reader: index.reader_builder().reload_policy(tantivy::ReloadPolicy::Manual).try_into()?,
            },
        })
    }

    pub fn search(&self, request: &RelationSearchRequest) -> anyhow::Result<RelationSearchResponse> {
        self.reader.search(request)
    }
}
