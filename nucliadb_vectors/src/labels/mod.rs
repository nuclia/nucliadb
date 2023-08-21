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
// Label Storage based on the FST crate, see https://docs.rs/fst/latest/fst
//
// Labels are stored in two files:
//
// - `labels.idx`: a file storing labels as records, each label is a (key, doc_ids) tuple
// - `labels.fst`: an FST map that stores the label value along with an offset in `label.idx`
//
// Indexing labels is done by calling the `LabelIndex::new` function,
// which returns an instance of `LabelIndex`.
//
// Searching for a label is done with the `get_label` method of the `LabelIndex` class.
//
// For read-only access, you can use `LabelIndex::open`
//
// There's a benchmark in `nucliadb/vectors_benchmark` you can call with `make label-bench`
//
// LIMITATIONS:
//
// FST's MapBuilder takes a sorted list of keys to insert all keys in one pass,
// so they need to be sorted beforehand.
//
// If we do this in memory it's going to cost ~350MiB per 100k labels.
// If we want less memory pressure, we could store labels in several FSTs and merge
// search results.
//
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::RwLock;

use bincode::serialize_into;
use fst::{Map, MapBuilder};
use memmap2::Mmap;
use serde::{Deserialize, Serialize};

use crate::VectorR;

/// Struct that contains the label and a sorted list of doc_ids
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Label {
    pub key: String,
    pub doc_ids: Vec<u64>,
}

pub struct LabelIndex {
    pub fst_file_path: PathBuf,
    pub records_file_path: PathBuf,
    records_file: RwLock<File>,
    fst_map: Map<Mmap>,
}

impl LabelIndex {
    const LABELS_FST: &str = "labels.fst";
    const LABELS_IDX: &str = "labels.idx";

    pub fn open(path: &Path) -> VectorR<Self> {
        let records_file_path = path.join(Self::LABELS_IDX);
        let fst_file_path = path.join(Self::LABELS_FST);

        let records_file = File::open(&records_file_path)?;
        let mmap = unsafe { Mmap::map(&File::open(&fst_file_path)?)? };
        let fst_map = Map::new(mmap)?;

        Ok(Self {
            fst_file_path,
            records_file_path,
            fst_map,
            records_file: RwLock::new(records_file),
        })
    }

    pub fn get_label(&self, key: &str) -> VectorR<Option<Label>> {
        let Some(offset) = self.fst_map.get(key) else {
            return Ok(None);
        };

        // we have a match, let's get the record
        let mut records_file = self.records_file.write().unwrap();
        let _ = records_file.seek(io::SeekFrom::Start(offset))?;

        Ok(bincode::deserialize_from(&*records_file).map(Some)?)
    }

    pub fn new<'a, I>(path: &Path, labels: I) -> VectorR<Self>
    where
        I: Iterator<Item = &'a Label>,
    {
        let records_file_path = path.join(Self::LABELS_IDX);
        let fst_file_path = path.join(Self::LABELS_FST);

        // create the record and fst files
        let mut records_file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(&records_file_path)?;
        let mut fst_file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(&fst_file_path)?;
        let mut records_writer = BufWriter::new(&mut records_file);
        let fst_writer = BufWriter::new(&mut fst_file);
        let mut fst_builder = MapBuilder::new(fst_writer)?;

        // 1. add Label into the records file
        // 2. add the key into the fst file, the value is the offset in the record file
        for label in labels {
            let start_position: u64 = records_writer.stream_position()?;
            serialize_into(&mut records_writer, label)?;
            fst_builder.insert(label.key.clone(), start_position)?;
        }
        records_writer.flush()?;
        fst_builder.finish()?;
        std::mem::drop(records_writer);

        let mmap = unsafe { Mmap::map(&fst_file)? };
        let fst_map = Map::new(mmap)?;
        Ok(Self {
            fst_file_path,
            records_file_path,
            records_file: RwLock::new(records_file),
            fst_map,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init() -> VectorR<()> {
        // let's create the index
        let mut labels: Vec<Label> = vec![
            Label {
                key: "key3".to_owned(),
                doc_ids: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            },
            Label {
                key: "key2".to_owned(),
                doc_ids: vec![1, 2, 3, 4, 5],
            },
            Label {
                key: "key1".to_owned(),
                doc_ids: vec![7, 8],
            },
        ];

        // needs to be sorted by keys
        labels.sort_by_key(|label| label.key.clone());
        let dir = tempfile::tempdir().unwrap();
        let label_index = LabelIndex::new(dir.path(), labels.iter())?;

        // now let's search for key1
        match label_index.get_label("key1") {
            Err(err) => panic!("{err:?}"),
            Ok(None) => panic!("Should be some"),
            Ok(Some(label)) => assert_eq!(label.doc_ids, vec![7, 8]),
        }

        // and key 3
        match label_index.get_label("key3") {
            Err(err) => panic!("{err:?}"),
            Ok(None) => panic!("Should be some"),
            Ok(Some(label)) => assert_eq!(label.doc_ids, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
        }

        // key23 can't be found
        match label_index.get_label("key23") {
            Err(err) => panic!("{err:?}"),
            Ok(Some(label)) => panic!("Should be None, found: {label:?}"),
            Ok(None) => (),
        }
        Ok(())
    }
}
