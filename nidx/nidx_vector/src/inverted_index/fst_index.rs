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

use std::{collections::HashSet, fs::File, io::BufWriter, path::Path, sync::Arc};

use fst::{Automaton, IntoStreamer, Map, Streamer};
use memmap2::Mmap;

use crate::VectorR;

use super::map::{InvertedMapReader, InvertedMapWriter};

/// A full inverted index mapping from keys to set of vector_ids (stored in a map file)
pub struct FstIndexWriter<'a> {
    writer: fst::MapBuilder<BufWriter<File>>,
    map_writer: &'a mut InvertedMapWriter,
}

impl<'a> FstIndexWriter<'a> {
    pub fn new(path: &Path, map_writer: &'a mut InvertedMapWriter) -> VectorR<Self> {
        Ok(Self {
            writer: fst::MapBuilder::new(BufWriter::new(File::create(path)?))?,
            map_writer,
        })
    }

    /// Insert a key to this index. This method must be called with keys sorted
    pub fn write(&mut self, key: &[u8], vector_ids: &[u32]) -> VectorR<()> {
        let map_pos = self.map_writer.write(vector_ids)?;

        Ok(self.writer.insert(key, map_pos)?)
    }

    pub fn finish(self) -> VectorR<()> {
        self.writer.finish()?;
        Ok(())
    }
}

pub struct FstIndexReader {
    fst: Map<Mmap>,
    map_reader: Arc<InvertedMapReader>,
}

impl FstIndexReader {
    pub fn open(path: &Path, map_reader: Arc<InvertedMapReader>) -> VectorR<Self> {
        Ok(Self {
            fst: Map::new(unsafe { Mmap::map(&File::open(path)?)? })?,
            map_reader,
        })
    }

    /// Gets a set of vector ids given the index key
    pub fn get(&self, key: &[u8]) -> Option<Vec<u32>> {
        self.fst.get(key).map(|pos| self.map_reader.get(pos))
    }

    /// Gets a set of vector ids given a key prefix
    pub fn get_prefix(&self, key: &[u8]) -> HashSet<u32> {
        let mut results = HashSet::new();
        let mut fst_results = self.fst.search(StartsWithBytes(key)).into_stream();
        while let Some((_key, pos)) = fst_results.next() {
            results.extend(self.map_reader.get(pos));
        }
        results
    }

    pub fn space_usage(&self) -> usize {
        self.fst.as_fst().as_bytes().len()
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use rand::Rng;
    use tempfile::NamedTempFile;

    use crate::{
        VectorR,
        inverted_index::map::{InvertedMapReader, InvertedMapWriter},
    };

    use super::{FstIndexReader, FstIndexWriter};

    #[test]
    fn test_fst_index_write_read() -> VectorR<()> {
        // Create some random entries for the map
        let mut rng = rand::thread_rng();
        let mut entries: BTreeMap<Vec<u8>, Vec<u32>> = BTreeMap::new();

        for _ in 0..20 {
            let len = rng.gen_range(1..2000);
            let mut ids = Vec::new();
            for _ in 0..len {
                ids.push(rng.r#gen());
            }
            let mut key = [0; 16];
            for k in &mut key {
                *k = rng.r#gen();
            }
            entries.insert(key.to_vec(), ids);
        }

        // Write the map
        let tmp = NamedTempFile::new()?;
        let tmp_map = NamedTempFile::new()?;
        let mut map_writer = InvertedMapWriter::new(tmp_map.path())?;
        let mut writer = FstIndexWriter::new(tmp.path(), &mut map_writer)?;

        for e in &entries {
            writer.write(e.0, e.1)?;
        }
        writer.finish()?;
        map_writer.finish()?;

        // Check the map has the same contents we initialized
        let map_reader = Arc::new(InvertedMapReader::open(tmp_map.path())?);
        let reader = FstIndexReader::open(tmp.path(), map_reader)?;

        for (key, value) in entries {
            let indexed = reader.get(&key);
            assert_eq!(indexed.unwrap(), value);
        }

        // A random key search does not return anything
        let key = "abcdefgh".as_bytes();
        assert!(reader.get(key).is_none());

        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct StartsWithBytes<'a>(&'a [u8]);

impl Automaton for StartsWithBytes<'_> {
    type State = Option<usize>;

    fn start(&self) -> Option<usize> {
        Some(0)
    }

    fn is_match(&self, pos: &Option<usize>) -> bool {
        *pos >= Some(self.0.len())
    }

    fn can_match(&self, pos: &Option<usize>) -> bool {
        pos.is_some()
    }

    fn will_always_match(&self, pos: &Option<usize>) -> bool {
        *pos >= Some(self.0.len())
    }

    fn accept(&self, pos: &Option<usize>, b: u8) -> Option<usize> {
        if let Some(pos) = *pos {
            // Already at the end, match
            if pos >= self.0.len() {
                return Some(pos + 1);
            }
            // If this character matches, advance
            if self.0.get(pos).cloned() == Some(b) {
                return Some(pos + 1);
            }
        }
        None
    }
}
