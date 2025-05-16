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

use nidx_types::{OpenIndexMetadata, Seq};
use tantivy::index::SegmentId;
use tantivy::{
    Directory, Index, IndexMeta, IndexSettings, SegmentReader,
    directory::{MmapDirectory, RamDirectory, error::OpenReadError},
    fastfield::write_alive_bitset,
    query::Query,
    schema::Schema,
};
use tantivy_common::{BitSet, TerminatingWrite as _};

use crate::{TantivyMeta, TantivySegmentMetadata};

pub trait DeletionQueryBuilder {
    fn query<'a>(&self, keys: impl Iterator<Item = &'a String>) -> Box<dyn Query>;
}

pub fn open_index_with_deletions(
    schema: Schema,
    open_index: impl OpenIndexMetadata<TantivyMeta>,
    query_builder: impl DeletionQueryBuilder,
) -> anyhow::Result<Index> {
    let index = Index::open(UnionDirectory::new(schema.clone(), open_index.segments())?)?;

    // Apply deletions
    for segment in index.searchable_segments()? {
        let segment_deletions = open_index
            .deletions()
            .filter(|&(_, del_seq)| (Seq::from(segment.meta().delete_opstamp().unwrap() as i64) < del_seq))
            .map(|(key, _)| key);
        let segment_reader = SegmentReader::open(&segment)?;
        let mut bitset = BitSet::with_max_value_and_full(segment.meta().max_doc());

        let query = query_builder.query(segment_deletions);
        query
            .weight(tantivy::query::EnableScoring::disabled_from_schema(&index.schema()))?
            .for_each_no_score(&segment_reader, &mut |doc_set| {
                for doc in doc_set {
                    bitset.remove(*doc);
                }
            })?;

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
/// We need to be able to apply deletions, but we cannot calculate them until the index and directory are opened
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
struct UnionDirectory {
    index_meta: Arc<Mutex<IndexMeta>>,
    segment_dirs: HashMap<String, MmapDirectory>,
    ram: RamDirectory,
}

impl UnionDirectory {
    fn new(schema: Schema, segments: impl Iterator<Item = (TantivySegmentMetadata, Seq)>) -> anyhow::Result<Self> {
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

impl Directory for UnionDirectory {
    fn get_file_handle(
        &self,
        path: &std::path::Path,
    ) -> Result<std::sync::Arc<dyn tantivy::directory::FileHandle>, tantivy::directory::error::OpenReadError> {
        if path.extension().is_some_and(|ext| ext == "del") {
            self.ram.get_file_handle(path)
        } else {
            let prefix = path.file_stem().unwrap().to_str().unwrap();
            let subdir = &self.segment_dirs.get(prefix);
            if let Some(subdir) = subdir {
                subdir.get_file_handle(path)
            } else {
                Err(OpenReadError::FileDoesNotExist(path.to_path_buf()))
            }
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
                locked_meta
                    .segments
                    .push(segment.with_delete_meta(opstamp, opstamp as u64));
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

#[cfg(test)]
mod tests {
    use std::{io::Write, path::Path};

    use crate::TantivySegmentMetadata;
    use serde_json::Value;
    use tantivy::index::SegmentId;
    use tantivy::{
        Directory,
        schema::{NumericOptions, Schema},
    };
    use tantivy_common::TerminatingWrite;
    use tempfile::tempdir;

    use super::UnionDirectory;

    #[test]
    fn test_read_union_directory() -> anyhow::Result<()> {
        let dir1 = tempdir()?;
        let dir2 = tempdir()?;

        let uuid1 = SegmentId::generate_random().uuid_string();
        let uuid2 = SegmentId::generate_random().uuid_string();
        let uuid3 = SegmentId::generate_random().uuid_string();

        std::fs::write(dir1.path().join(format!("{uuid1}.store")), "dir1")?;
        std::fs::write(dir2.path().join(format!("{uuid2}.store")), "dir2")?;

        let segments = vec![
            (
                TantivySegmentMetadata {
                    path: dir1.path().to_path_buf(),
                    records: 1,
                    index_metadata: crate::TantivyMeta {
                        segment_id: uuid1.clone(),
                    },
                },
                1i64.into(),
            ),
            (
                TantivySegmentMetadata {
                    path: dir2.path().to_path_buf(),
                    records: 1,
                    index_metadata: crate::TantivyMeta {
                        segment_id: uuid2.clone(),
                    },
                },
                1i64.into(),
            ),
        ];

        let directory = UnionDirectory::new(Schema::builder().build(), segments.into_iter())?;

        // Can read existing files
        assert_eq!(
            directory
                .open_read(Path::new(&format!("{uuid1}.store")))?
                .read_bytes()?,
            "dir1"
        );
        assert_eq!(
            directory
                .open_read(Path::new(&format!("{uuid2}.store")))?
                .read_bytes()?,
            "dir2"
        );

        // Can not read unknown files of existing segments
        assert!(directory.open_read(Path::new(&format!("{uuid1}.fake"))).is_err());

        // Can not read unknown segments
        assert!(directory.open_read(Path::new(&format!("{uuid3}.store"))).is_err());

        // Can not read malformed filenames
        assert!(directory.open_read(Path::new("wadus")).is_err());

        Ok(())
    }

    #[test]
    fn test_union_directory_metadata() -> anyhow::Result<()> {
        let dir1 = tempdir()?;
        let dir2 = tempdir()?;

        let uuid1 = SegmentId::generate_random().uuid_string();
        let uuid2 = SegmentId::generate_random().uuid_string();

        std::fs::write(dir1.path().join(format!("{uuid1}.store")), "dir1")?;
        std::fs::write(dir2.path().join(format!("{uuid2}.store")), "dir2")?;

        let segments = vec![
            (
                TantivySegmentMetadata {
                    path: dir1.path().to_path_buf(),
                    records: 12,
                    index_metadata: crate::TantivyMeta {
                        segment_id: uuid1.clone(),
                    },
                },
                12i64.into(),
            ),
            (
                TantivySegmentMetadata {
                    path: dir2.path().to_path_buf(),
                    records: 1,
                    index_metadata: crate::TantivyMeta {
                        segment_id: uuid2.clone(),
                    },
                },
                13i64.into(),
            ),
        ];

        let mut schema = Schema::builder();
        schema.add_bool_field("mirror", NumericOptions::default());
        let directory = UnionDirectory::new(schema.build(), segments.into_iter())?;

        // Can read metadata
        let meta: Value = serde_json::from_slice(&directory.atomic_read(Path::new("meta.json"))?)?;
        let root = meta.as_object().unwrap();

        // Has schema
        let schema = root["schema"].as_array().unwrap();
        assert_eq!(
            schema[0].as_object().unwrap()["name"],
            Value::String("mirror".to_string())
        );

        // Has list of segments
        let segments = root["segments"].as_array().unwrap();
        let segment = segments[0].as_object().unwrap();
        assert_eq!(
            segment["deletes"].as_object().unwrap()["num_deleted_docs"]
                .as_i64()
                .unwrap(),
            0
        );
        assert_eq!(segment["deletes"].as_object().unwrap()["opstamp"].as_i64().unwrap(), 12);
        assert_eq!(segment["segment_id"].as_str().unwrap().replace('-', ""), uuid1);

        // Adding deletions
        let mut writer = directory.open_write(Path::new(&format!("{uuid1}.7.del")))?;
        writer.write_all(b"garbage")?;
        writer.terminate()?;

        // Metadata was updated
        let meta: Value = serde_json::from_slice(&directory.atomic_read(Path::new("meta.json"))?)?;
        let root = meta.as_object().unwrap();
        let segments = root["segments"].as_array().unwrap();
        let segment = segments
            .iter()
            .find(|s| s.as_object().unwrap()["segment_id"].as_str().unwrap().replace('-', "") == uuid1)
            .unwrap();
        assert_eq!(
            segment["deletes"].as_object().unwrap()["num_deleted_docs"]
                .as_i64()
                .unwrap(),
            7
        );

        Ok(())
    }
}
