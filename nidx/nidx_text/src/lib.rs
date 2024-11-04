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
mod schema;
mod search_query;

use std::collections::HashSet;
use std::path::Path;

use anyhow::anyhow;
use nidx_protos::resource::ResourceStatus;
use nidx_protos::{DocumentSearchRequest, DocumentSearchResponse, StreamRequest};
use nidx_tantivy::index_reader::{open_index_with_deletions, DeletionQueryBuilder};
use nidx_tantivy::{TantivyIndexer, TantivyMeta, TantivySegmentMetadata};
use nidx_types::OpenIndexMetadata;
use prefilter::{PreFilterRequest, PreFilterResponse};
use reader::{DocumentIterator, TextReaderService};
use schema::{timestamp_to_datetime_utc, TextSchema};

pub use search_query::TextContext;
use tantivy::indexer::merge_indices;
use tantivy::schema::Field;
use tantivy::{
    directory::MmapDirectory,
    doc,
    query::{Query, TermSetQuery},
    schema::Facet,
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

        if resource.status == ResourceStatus::Delete as i32 {
            return Err(anyhow::anyhow!("This is a deletion, not a set resource"));
        }

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
            tags: HashSet::new(),
            index_metadata: TantivyMeta {
                segment_id: segment.id().uuid_string(),
            },
        })
    }
}

fn index_document(
    writer: &mut TantivyIndexer,
    resource: &nidx_protos::Resource,
    schema: TextSchema,
) -> anyhow::Result<usize> {
    let Some(resource_id) = resource.resource.as_ref().map(|r| r.uuid.as_str()) else {
        return Err(anyhow!("Missing resource ID"));
    };
    let Some(metadata) = resource.metadata.as_ref() else {
        return Err(anyhow!("Missing resource metadata"));
    };
    let Some(modified) = metadata.modified.as_ref() else {
        return Err(anyhow!("Missing resource modified date in metadata"));
    };
    let Some(created) = metadata.created.as_ref() else {
        return Err(anyhow!("Missing resource created date in metadata"));
    };

    let mut base_doc = doc!(
        schema.uuid => resource_id.as_bytes(),
        schema.modified => timestamp_to_datetime_utc(modified),
        schema.created => timestamp_to_datetime_utc(created),
        schema.status => resource.status as u64,
    );

    let resource_security = resource.security.as_ref();
    if let Some(security_groups) = resource_security.filter(|i| !i.access_groups.is_empty()) {
        base_doc.add_u64(schema.groups_public, 0_u64);
        for group_id in security_groups.access_groups.iter() {
            let mut group_id_key = group_id.clone();
            if !group_id.starts_with('/') {
                // Slash needs to be added to be compatible with tantivy facet fields
                group_id_key = "/".to_string() + group_id;
            }
            let facet = Facet::from(group_id_key.as_str());
            base_doc.add_facet(schema.groups_with_access, facet)
        }
    } else {
        base_doc.add_u64(schema.groups_public, 1_u64);
    }

    for label in resource.labels.iter() {
        let facet = Facet::from(label.as_str());
        base_doc.add_facet(schema.facets, facet);
    }

    for (field, text_info) in &resource.texts {
        let mut field_doc = base_doc.clone();
        let mut facet_key: String = "/".to_owned();
        facet_key.push_str(field.as_str());
        let facet_field = Facet::from(facet_key.as_str());
        field_doc.add_facet(schema.field, facet_field);
        field_doc.add_text(schema.text, &text_info.text);

        for label in text_info.labels.iter() {
            let facet = Facet::from(label.as_str());
            field_doc.add_facet(schema.facets, facet);
        }
        writer.add_document(field_doc).unwrap();
    }

    Ok(resource.texts.len())
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
