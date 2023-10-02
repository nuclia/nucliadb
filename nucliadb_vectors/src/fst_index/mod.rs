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
// - `labels.idx`: a file storing labels as records, each label is a (key, node_addresses) tuple
// - `labels.fst`: an FST map that stores the label value along with an offset in `label.idx`
//
// Indexing labels is done by calling the `LabelIndex::new` function,
// which returns an instance of `LabelIndex`.
//
// Searching for nodes matching some labels or key prefixes is done with the `get_nodes` methods
// of the `LabelIndex` and `KeyIndex` classes.
//
// For read-only access, you can use `LabelIndex::open` or `KeyIndex::open` methods.
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
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::RwLock;

use bincode::serialize_into;
use fst::automaton::{Automaton, Str};
use fst::{IntoStreamer, Map, MapBuilder, Streamer};
use memmap2::Mmap;
use serde::{Deserialize, Serialize};

use crate::VectorR;

/// Struct that contains the label and a sorted list of node_addresses
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Label {
    pub key: String,
    pub node_addresses: Vec<u64>,
}

pub struct LabelIndex {
    pub fst_file_path: PathBuf,
    pub records_file_path: PathBuf,
    records_file: RwLock<BufReader<File>>,
    fst_map: Map<Mmap>,
}

/// LabelIndex manages an FST file along with an index file.
/// They are created by iterating on Label objects.
impl LabelIndex {
    const LABELS_FST: &str = "labels.fst";
    const LABELS_IDX: &str = "labels.idx";

    /// Returns true if FST files are present
    pub fn exists(path: &Path) -> bool {
        let records_file_path = path.join(Self::LABELS_IDX);
        let fst_file_path = path.join(Self::LABELS_FST);
        records_file_path.exists() && fst_file_path.exists()
    }

    /// Opens the index for read access.
    /// `path` is the directory where the FST and index files are located.
    pub fn open(path: &Path) -> VectorR<Self> {
        let records_file_path = path.join(Self::LABELS_IDX);
        let fst_file_path = path.join(Self::LABELS_FST);

        let records_file = BufReader::new(File::open(&records_file_path)?);
        let mmap = unsafe { Mmap::map(&File::open(&fst_file_path)?)? };
        let fst_map = Map::new(mmap)?;

        Ok(Self {
            fst_file_path,
            records_file_path,
            fst_map,
            records_file: RwLock::new(records_file),
        })
    }

    /// Given a list of labels, return the intersection of all node addresses
    pub fn get_nodes(&self, labels: &[String]) -> VectorR<HashSet<u64>> {
        let mut records = self.records_file.write().unwrap();
        let mut results: HashSet<u64> = HashSet::new();

        for (index, label) in labels.iter().enumerate() {
            if let Some(offset) = self.fst_map.get(label) {
                let _ = records.seek(io::SeekFrom::Start(offset))?;
                let label_struct: Label = bincode::deserialize_from(&mut *records)?;
                let set: HashSet<u64> = label_struct.node_addresses.iter().cloned().collect();
                if index == 0 {
                    results.extend(&set);
                } else {
                    results = results.intersection(&set).cloned().collect();
                }
            } else {
                // if a label does not exist, we return an empty HashSet immediatly
                return Ok(HashSet::new());
            }
        }
        Ok(results)
    }

    /// Creates an index and returns an instance with read access.
    /// `path` is the directory to store the FST and index file.
    /// `labels` is an iterator of Label objects.
    pub fn new<I>(path: &Path, labels: I) -> VectorR<Self>
    where I: Iterator<Item = Label> {
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

        // TODO: we're sorting here
        let mut sorted_labels: Vec<Label> = labels.collect();
        sorted_labels.sort_by_key(|l| l.key.clone());

        // 1. add Label into the records file
        // 2. add the key into the fst file, the value is the offset in the record file
        for mut label in sorted_labels {
            label.node_addresses.sort();
            let start_position: u64 = records_writer.stream_position()?;
            serialize_into(&mut records_writer, &label)?;
            fst_builder.insert(label.key.clone(), start_position)?;
        }
        records_writer.flush()?;
        fst_builder.finish()?;
        std::mem::drop(records_writer);

        let records_file = RwLock::new(BufReader::new(records_file));
        let mmap = unsafe { Mmap::map(&fst_file)? };
        let fst_map = Map::new(mmap)?;
        Ok(Self {
            fst_file_path,
            records_file_path,
            records_file,
            fst_map,
        })
    }
}

pub struct KeyIndex {
    pub fst_file_path: PathBuf,
    fst_map: Map<Mmap>,
}

/// KeyIndex stores a (key, doc_id) tuple used to speed up lookups.
impl KeyIndex {
    const KEYS_FST: &str = "keys.fst";

    /// Returns true if the FST file is present
    pub fn exists(path: &Path) -> bool {
        let fst_file_path = path.join(Self::KEYS_FST);
        fst_file_path.exists()
    }

    /// Opens the index for read access.
    /// `path` is the directory where the FST file is located.
    pub fn open(path: &Path) -> VectorR<Self> {
        let fst_file_path = path.join(Self::KEYS_FST);
        let mmap = unsafe { Mmap::map(&File::open(&fst_file_path)?)? };
        let fst_map = Map::new(mmap)?;

        Ok(Self {
            fst_file_path,
            fst_map,
        })
    }

    /// Given a list of key prefixes, returns a list of node addresses
    pub fn get_nodes(&self, key_prefixes: &[String]) -> VectorR<HashSet<u64>> {
        if key_prefixes.is_empty() {
            return Ok(HashSet::new());
        }

        let mut results: HashSet<u64> = HashSet::new();

        for prefix in key_prefixes.iter() {
            let matcher = Str::new(prefix).starts_with();
            let mut stream = self.fst_map.search(&matcher).into_stream();

            while let Some((_, node_address)) = stream.next() {
                results.insert(node_address);
            }
        }

        Ok(results)
    }

    /// Creates an index and returns an instance with read access.
    /// `path` is the directory to store the FST and index file.
    /// `labels` is an iterator of Label objects.
    pub fn new<I>(path: &Path, keys: I) -> VectorR<Self>
    where I: Iterator<Item = (String, u64)> {
        let fst_file_path = path.join(Self::KEYS_FST);

        // create the fst file
        let mut fst_file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(&fst_file_path)?;

        let fst_writer = BufWriter::new(&mut fst_file);
        let mut fst_builder = MapBuilder::new(fst_writer)?;

        for (key, doc_id) in keys {
            fst_builder.insert(key.clone(), doc_id)?;
        }
        fst_builder.finish()?;

        let mmap = unsafe { Mmap::map(&fst_file)? };
        let fst_map = Map::new(mmap)?;
        Ok(Self {
            fst_file_path,
            fst_map,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! assert_hashset {
        ($hashset:expr, $vec:expr) => {{
            let hashset: HashSet<_> = $hashset.clone();
            let vec: HashSet<_> = $vec.into_iter().collect();
            assert_eq!(hashset, vec);
        }};
    }

    #[test]
    fn test_init() -> VectorR<()> {
        // let's create the index
        let labels: Vec<Label> = vec![
            Label {
                key: "key3".to_owned(),
                node_addresses: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            },
            Label {
                key: "key2".to_owned(),
                node_addresses: vec![1, 2, 3, 4, 5, 13],
            },
            Label {
                key: "key1".to_owned(),
                node_addresses: vec![7, 8, 11],
            },
        ];

        // needs to be sorted by keys
        let dir = tempfile::tempdir().unwrap();
        let label_index = LabelIndex::new(dir.path(), labels.into_iter())?;

        // now let's search for the intersection of key1 and key3
        match label_index.get_nodes(&["key1".to_string(), "key3".to_string()]) {
            Err(err) => panic!("{err:?}"),
            Ok(node_addresses) => assert_hashset!(node_addresses, vec![7, 8]),
        }

        // if one label does not exist, we return 0
        match label_index.get_nodes(&[
            "key1".to_string(),
            "key3".to_string(),
            "IDONTEXIST".to_string(),
        ]) {
            Err(err) => panic!("{err:?}"),
            Ok(node_addresses) => assert_eq!(node_addresses.len(), 0),
        }

        // key23 can't be found
        match label_index.get_nodes(&["key23".to_string()]) {
            Err(err) => panic!("{err:?}"),
            Ok(node_addresses) => assert_eq!(node_addresses.len(), 0),
        }

        // let's create the id lookup
        let mut keys: Vec<(String, u64)> = vec![
            ("key-for-1".to_string(), 1),
            ("key-for-2".to_string(), 2),
            ("key-for-3".to_string(), 3),
        ];

        // needs to be sorted by keys
        keys.sort();
        let key_index = KeyIndex::new(dir.path(), keys.into_iter())?;

        // now let's search for key-
        match key_index.get_nodes(&["key-".to_string()]) {
            Err(err) => panic!("{err:?}"),
            Ok(node_addresses) => assert_hashset!(node_addresses, vec![1, 2, 3]),
        }

        // now let's search for xxx-
        match key_index.get_nodes(&["xxx-".to_string()]) {
            Err(err) => panic!("{err:?}"),
            Ok(node_addresses) => assert!(node_addresses.is_empty()),
        }

        Ok(())
    }
}
