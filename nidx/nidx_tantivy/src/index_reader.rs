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

use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, Mutex},
};

use nidx_types::Seq;
use tantivy::{
    directory::{error::OpenReadError, MmapDirectory, RamDirectory},
    fastfield::write_alive_bitset,
    query::Query,
    schema::Schema,
    Directory, Index, IndexMeta, IndexSettings, SegmentId, SegmentReader,
};
use tantivy_common::{BitSet, TerminatingWrite as _};

use crate::TantivySegmentMetadata;

pub trait DeletionQueryBuilder<'a> {
    fn query(&self, keys: impl Iterator<Item = &'a String>) -> Box<dyn Query>;
}

pub fn open_index_with_deletions<'a>(
    schema: Schema,
    segments: Vec<(TantivySegmentMetadata, Seq)>,
    deletions: &'a [(Seq, &Vec<String>)],
    query_builder: impl DeletionQueryBuilder<'a>,
) -> anyhow::Result<Index> {
    let index = Index::open(NidxDirectory::new(schema.clone(), &segments)?)?;

    // Apply deletions
    for segment in index.searchable_segments()? {
        let segment_deletions = deletions
            .iter()
            .filter_map(|(deletion_seq, keys)| {
                let del: i64 = deletion_seq.into();
                if segment.meta().delete_opstamp().unwrap() < del as u64 {
                    Some(keys.iter())
                } else {
                    None
                }
            })
            .flatten();
        let segment_reader = SegmentReader::open(&segment).unwrap();
        let mut bitset = BitSet::with_max_value_and_full(segment.meta().max_doc());
        let mut deleted = Vec::new();

        let query = query_builder.query(segment_deletions);
        query
            .weight(tantivy::query::EnableScoring::Disabled {
                schema: &index.schema(),
                searcher_opt: None,
            })?
            .for_each_no_score(&segment_reader, &mut |doc_set| {
                for doc in doc_set {
                    bitset.remove(*doc);
                    deleted.push(*doc);
                }
            })
            .unwrap();

        let mut writer = index.directory().open_write(Path::new(&format!(
            "{}.{}.del",
            segment.id().uuid_string(),
            segment.meta().max_doc() - bitset.len() as u32
        )))?;
        write_alive_bitset(&bitset, &mut writer)?;
        writer.terminate()?;
    }

    Ok(index)
}

/// A Tantivy directory implementation that reads data from a set of directories (one per segment)
///
/// We need to be able to apply deletions, but we cannot calculate them until after the index and directory is opened
/// since we need to search in the segments to know the deleted documents. So we open it with a metadata that indicates
/// no deletions, calculate deletions, and then write the deletions through this directory. The code that handles
/// writes detects a deletion file being writter and adjusts the index metadata in order to reflect it.
///
/// An alternative to this would be to open the index twice: first without deletions, then a second time passing the
/// deletions on the constructor so we can have the correct metadata from the start. However, this is hard without copying
/// tantivy code into this project becaus deletions files have a footer with a checksum and we don't have access to it outside
/// of the dictionary implementation.
///
/// In order to alleviate this, this Directory is kept private and only meant to be used from `TantivyIndexWithDeletions`
#[derive(Debug, Clone)]
struct NidxDirectory {
    index_meta: Arc<Mutex<IndexMeta>>,
    segment_dirs: HashMap<String, MmapDirectory>,
    ram: RamDirectory,
}

impl NidxDirectory {
    fn new(schema: Schema, segments: &Vec<(TantivySegmentMetadata, Seq)>) -> anyhow::Result<Self> {
        let mut index_meta = IndexMeta::with_schema(schema.clone());

        // Temporary index, just to create SegmentMetas
        let temp_index = Index::create(RamDirectory::create(), schema, IndexSettings::default())?;

        let mut segment_dirs = HashMap::new();
        for (segment, seq) in segments {
            let mmap = MmapDirectory::open(&segment.path)?;
            segment_dirs.insert(segment.index_metadata.segment_id.clone(), mmap);

            let opstamp: i64 = seq.into();

            // Adds segments to meta.json
            index_meta.segments.push(
                temp_index
                    .new_segment_meta(
                        SegmentId::from_uuid_string(&segment.index_metadata.segment_id)?,
                        segment.records as u32,
                    )
                    .with_delete_meta(0, opstamp as u64),
            );
        }
        Ok(Self {
            index_meta: Arc::new(Mutex::new(index_meta)),
            segment_dirs,
            ram: RamDirectory::create(),
        })
    }
}

impl Directory for NidxDirectory {
    fn get_file_handle(
        &self,
        path: &std::path::Path,
    ) -> Result<std::sync::Arc<dyn tantivy::directory::FileHandle>, tantivy::directory::error::OpenReadError> {
        if path.extension().is_some_and(|ext| ext == "del") {
            self.ram.get_file_handle(path)
        } else {
            let prefix = path.file_stem().unwrap().to_str().unwrap();
            let subdir = &self.segment_dirs[prefix];
            subdir.get_file_handle(path)
        }
    }

    fn delete(&self, path: &std::path::Path) -> Result<(), tantivy::directory::error::DeleteError> {
        if path.ends_with(".tantivy-meta.lock") {
            return Ok(());
        }
        unimplemented!()
    }

    fn exists(&self, _path: &std::path::Path) -> Result<bool, tantivy::directory::error::OpenReadError> {
        unimplemented!()
    }

    fn open_write(
        &self,
        path: &std::path::Path,
    ) -> Result<tantivy::directory::WritePtr, tantivy::directory::error::OpenWriteError> {
        let name_parts: Vec<_> = path.file_name().unwrap().to_str().unwrap().split('.').collect();
        if name_parts.last().is_some_and(|x| *x == "del") {
            // Setting a deletion file: we need to set the metadata (in meta.json) for tantivy to be aware of this
            // Since we cannot pass extra parameters here, we use the fact that the deletions are named like
            // <segment>.<opstamp>.del in order to pass information in the opstamp field, in this case, the number
            // of deletions. This is not stricly needed, but it's nice in order to get accurate counts, which we
            // currently don't use anywhere.
            let mut locked_meta = self.index_meta.lock().unwrap();
            let found = locked_meta
                .segments
                .iter()
                .enumerate()
                .find(|(_, segment)| segment.id().uuid_string() == name_parts[0]);
            if let Some((position, _)) = found {
                let opstamp = name_parts[1].parse().unwrap();
                let segment = locked_meta.segments.swap_remove(position);
                locked_meta.segments.push(segment.with_delete_meta(opstamp, opstamp as u64));
            }
        }
        self.ram.open_write(path)
    }

    fn atomic_read(&self, path: &std::path::Path) -> Result<Vec<u8>, tantivy::directory::error::OpenReadError> {
        if path.ends_with(".managed.json") {
            return Err(OpenReadError::FileDoesNotExist(path.to_path_buf()));
        } else if path.ends_with("meta.json") {
            return Ok(serde_json::to_vec(&*self.index_meta.lock().unwrap()).unwrap());
        }
        unimplemented!();
    }

    fn atomic_write(&self, _path: &std::path::Path, _data: &[u8]) -> std::io::Result<()> {
        Ok(())
    }

    fn sync_directory(&self) -> std::io::Result<()> {
        Ok(())
    }

    fn watch(
        &self,
        _watch_callback: tantivy::directory::WatchCallback,
    ) -> tantivy::Result<tantivy::directory::WatchHandle> {
        unimplemented!()
    }
}
