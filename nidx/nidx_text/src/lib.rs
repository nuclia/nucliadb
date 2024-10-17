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

mod query_io;
pub mod reader;
mod schema;
mod search_query;
pub mod writer;

use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

use nidx_types::Seq;
use nucliadb_core::protos::DocumentSearchRequest;
use nucliadb_core::texts::FieldReader;
use nucliadb_core::{
    node_error,
    protos::{resource::ResourceStatus, Resource},
    NodeResult,
};
use reader::TextReaderService;
use schema::{timestamp_to_datetime_utc, TextSchema};
use serde_json::Value;
use tantivy::fastfield::write_alive_bitset;
use tantivy::SegmentReader;
use tantivy::{
    directory::MmapDirectory,
    doc,
    indexer::merge_filtered_segments,
    query::{Query, TermSetQuery},
    schema::Facet,
    Index, SegmentId, SingleSegmentIndexWriter, Term,
};
use tantivy_common::{ReadOnlyBitSet, TerminatingWrite};
use tempfile::{tempdir, TempDir};

pub struct TextIndexer;

impl TextIndexer {
    pub fn new() -> Self {
        TextIndexer
    }

    pub fn index_resource<'a>(&self, output_dir: &Path, resource: &Resource) -> NodeResult<(i64, Vec<String>)> {
        // Index resource
        let field_schema = TextSchema::new();
        let index_builder = Index::builder().schema(field_schema.schema.clone());
        let mut writer = index_builder.single_segment_index_writer(MmapDirectory::open(output_dir)?, 15_000_000)?;
        let Some(resource_id) = resource.resource.as_ref() else {
            return Err(node_error!("Missing resource ID"));
        };

        // writer.delete_term(uuid_term);
        let count = if resource.status != ResourceStatus::Delete as i32 {
            index_document(&mut writer, &resource, field_schema)?
        } else {
            0
        };
        writer.finalize()?;

        return Ok((count as i64, vec![resource_id.uuid.clone()]));
    }

    pub fn merge(
        &self,
        work_dir: &Path,
        segments: &[(PathBuf, Seq, i64)],
        deletions: &Vec<(Seq, &Vec<String>)>,
    ) -> NodeResult<(String, usize)> {
        // Move all segment data into the same directory. TODO: Do this while downloading/unpacking
        let mut segment_uuids = Vec::new();
        for (segment_dir, _, num_docs) in segments {
            let mut uuid = None;
            for segment_file in std::fs::read_dir(&segment_dir)? {
                let name = segment_file?.file_name();
                std::fs::rename(segment_dir.join(&name), work_dir.join(&name))?;
                if uuid.is_none() {
                    uuid = Some(name.into_string().unwrap().split('.').next().unwrap().to_string());
                }
            }
            segment_uuids.push((uuid.unwrap(), num_docs));
        }

        // Open the index (which we use just to get Segment objects)
        let index = Index::open_in_dir(work_dir)?;

        let tantivy_segments = segment_uuids
            .iter()
            .map(|(suid, num_docs)| {
                // TODO: Do not rely on segments.records. Save the SegmentMeta to the segments table (JSON field for random stuff)
                let meta = index.new_segment_meta(SegmentId::from_uuid_string(&suid).unwrap(), **num_docs as u32);
                index.segment(meta)
            })
            .collect::<Vec<_>>();

        // Calculate deletions bitset. So ugly!
        let deletions = segments
            .iter()
            .zip(tantivy_segments.iter())
            .map(|((_, segment_seq, _), segment)| {
                let uuid = index.schema().get_field("uuid").unwrap();
                let query = Box::new(TermSetQuery::new(deletions.iter().flat_map(|(deletion_seq, keys)| {
                    if segment_seq < deletion_seq {
                        keys.iter().map(|k| Term::from_field_bytes(uuid, k.as_bytes())).collect()
                    } else {
                        vec![]
                    }
                })));
                let segment_reader = SegmentReader::open(segment).unwrap();
                let mut bitset = tantivy_common::BitSet::with_max_value_and_full(segment.meta().max_doc());
                let mut deleted = Vec::new();
                query
                    .weight(tantivy::query::EnableScoring::Disabled {
                        schema: &index.schema(),
                        searcher_opt: None,
                    })
                    .unwrap()
                    .for_each_no_score(&segment_reader, &mut |doc_set| {
                        for doc in doc_set {
                            bitset.remove(*doc);
                            deleted.push(*doc);
                        }
                    })
                    .unwrap();
                Some(ReadOnlyBitSet::from(&bitset).into())
            })
            .collect::<Vec<_>>();

        // Do the merge
        let output_dir = work_dir.join("output");
        std::fs::create_dir(output_dir)?;
        let output_index = merge_filtered_segments(
            &tantivy_segments,
            index.settings().clone(),
            deletions,
            MmapDirectory::open(work_dir.join("output"))?,
        )?;
        let docs = output_index.searchable_segment_metas()?[0].num_docs();

        Ok((String::from("output"), docs as usize))
    }
}

fn index_document(writer: &mut SingleSegmentIndexWriter, resource: &Resource, schema: TextSchema) -> NodeResult<usize> {
    let Some(resource_id) = resource.resource.as_ref().map(|r| r.uuid.as_str()) else {
        return Err(node_error!("Missing resource ID"));
    };
    let Some(metadata) = resource.metadata.as_ref() else {
        return Err(node_error!("Missing resource metadata"));
    };
    let Some(modified) = metadata.modified.as_ref() else {
        return Err(node_error!("Missing resource modified date in metadata"));
    };
    let Some(created) = metadata.created.as_ref() else {
        return Err(node_error!("Missing resource created date in metadata"));
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
    _index_dir: TempDir,
    reader: TextReaderService,
}

impl TextSearcher {
    pub fn new(
        segments_dir: &Path,
        index_id: i64,
        segments: Vec<(i64, i64)>,
        deletions: Vec<(i64, String)>,
    ) -> NodeResult<Self> {
        let index_dir = tempdir()?;
        let index_path = index_dir.path();

        let mut segment_uuids = Vec::new();
        for (s, _) in &segments {
            let segment_dir = segments_dir.join(format!("{index_id}/{s}"));
            let mut uuid = None;
            for segment_file in std::fs::read_dir(&segment_dir)? {
                let name = segment_file?.file_name();
                std::fs::copy(segment_dir.join(&name), index_path.join(&name))?;
                if uuid.is_none() {
                    uuid = Some(name.into_string().unwrap().split('.').next().unwrap().to_string());
                }
            }
            let segment_meta: Value = serde_json::from_reader(File::open(segment_dir.join("meta.json"))?)?;
            let num_docs = segment_meta.as_object().unwrap().get("segments").unwrap().as_array().unwrap()[0]
                .as_object()
                .unwrap()
                .get("max_doc")
                .unwrap()
                .as_i64()
                .unwrap();
            segment_uuids.push((uuid.unwrap(), num_docs));
        }

        let index = Index::open_in_dir(index_path)?;
        let mut tantivy_segments = segment_uuids
            .iter()
            .map(|(suid, num_docs)| {
                // TODO: Do not rely on segments.records. Save the SegmentMeta to the segments table (JSON field for random stuff)
                let meta = index.new_segment_meta(SegmentId::from_uuid_string(&suid).unwrap(), *num_docs as u32);
                index.segment(meta)
            })
            .collect::<Vec<_>>();

        // Write deletions bitset to disk. So ugly!
        let num_deletions: Vec<_> = segments
            .iter()
            .zip(tantivy_segments.iter_mut())
            .map(|((sidd, segment_seq), segment)| {
                let uuid = index.schema().get_field("uuid").unwrap();
                let query = Box::new(TermSetQuery::new(deletions.iter().filter_map(|(deletion_seq, key)| {
                    if segment_seq < deletion_seq {
                        Some(Term::from_field_bytes(uuid, key.as_bytes()))
                    } else {
                        None
                    }
                })));
                let segment_reader = SegmentReader::open(segment).unwrap();
                let mut bitset = tantivy_common::BitSet::with_max_value_and_full(segment.meta().max_doc());
                let mut deleted = Vec::new();
                query
                    .weight(tantivy::query::EnableScoring::Disabled {
                        schema: &index.schema(),
                        searcher_opt: None,
                    })
                    .unwrap()
                    .for_each_no_score(&segment_reader, &mut |doc_set| {
                        for doc in doc_set {
                            bitset.remove(*doc);
                            deleted.push(*doc);
                        }
                    })
                    .unwrap();

                if !deleted.is_empty() {
                    let mut deletes = segment.open_write(tantivy::SegmentComponent::Delete).unwrap();
                    write_alive_bitset(&bitset, &mut deletes).unwrap();
                    deletes.terminate().unwrap();
                }

                println!(
                    "MMMM segment {} with seq {} and deletions {:?}, deleted = {:?}",
                    sidd, segment_seq, deletions, deleted
                );

                (segment.meta(), deleted.len())
            })
            .collect();

        let mut index_meta: serde_json::Value = serde_json::from_reader(File::open(index_path.join("meta.json"))?)?;
        let segments = index_meta.as_object_mut().unwrap().get_mut("segments").unwrap().as_array_mut().unwrap();
        segments.clear();

        // Write list of segments to state
        for (meta, num_deletions) in num_deletions {
            let meta = meta.clone().with_delete_meta(num_deletions as u32, 0);
            let meta_json = serde_json::to_value(meta)?;
            segments.push(meta_json);
        }
        serde_json::to_writer(
            OpenOptions::new().write(true).truncate(true).open(index_path.join("meta.json"))?,
            &index_meta,
        )?;

        let reader = index.reader()?;
        Ok(Self {
            _index_dir: index_dir,
            reader: TextReaderService {
                index,
                schema: TextSchema::new(),
                reader,
            },
        })
    }

    pub fn dummy_search(&self) -> NodeResult<(usize, usize)> {
        let results = self
            .reader
            .search(&DocumentSearchRequest {
                body: "Pinocho".to_string(),
                result_per_page: 20,
                ..Default::default()
            })?
            .results
            .len();
        let total = self.reader.count()?;
        Ok((results, total))
    }
}
