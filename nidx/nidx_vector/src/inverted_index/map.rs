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

use std::{
    fs::File,
    io::{BufWriter, Write},
    path::Path,
};

use memmap2::Mmap;
use stream_vbyte::{decode::decode, encode::encode, scalar::Scalar};

use crate::VectorR;

/// A structure that maps from an index entry to a list of vector ids
pub struct InvertedMapWriter {
    writer: BufWriter<File>,
    pos: u64,
}

impl InvertedMapWriter {
    pub fn new(path: &Path) -> VectorR<Self> {
        Ok(Self {
            writer: BufWriter::new(File::create(path)?),
            pos: 0,
        })
    }

    /// Write a list of vector ids to the map, returns the map position they
    /// were written at, to be stored at the index.
    pub fn write(&mut self, data: &[u32]) -> VectorR<u64> {
        let mut buf = vec![0; data.len() * 5];
        let current_pos = self.pos;
        let encoded_len = encode::<Scalar>(data, &mut buf);
        self.pos += encoded_len as u64 + 8;

        self.writer.write_all(&data.len().to_le_bytes())?;
        self.writer.write_all(&buf[0..encoded_len])?;

        Ok(current_pos)
    }

    pub fn finish(mut self) -> VectorR<()> {
        self.writer.flush()?;
        Ok(())
    }
}

pub struct InvertedMapReader {
    data: Mmap,
}

impl InvertedMapReader {
    pub fn open(path: &Path) -> VectorR<Self> {
        Ok(Self {
            data: unsafe { Mmap::map(&File::open(path)?)? },
        })
    }

    /// Gets a set of vector ids given the position in this file (as returned by InvertedMapWriter::write)
    pub fn get(&self, pos: u64) -> Vec<u32> {
        let pos = pos as usize;
        let nums_len = usize::from_le_bytes(self.data[pos..pos + 8].try_into().unwrap());
        let mut nums = vec![0; nums_len];

        decode::<Scalar>(&self.data[pos + 8..], nums_len, &mut nums);

        nums
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;
    use tempfile::NamedTempFile;

    use crate::VectorR;

    use super::{InvertedMapReader, InvertedMapWriter};

    #[test]
    fn test_inverted_map_write_read() -> VectorR<()> {
        // Create some random entries for the map
        let mut rng = rand::thread_rng();
        let mut entries: Vec<Vec<u32>> = Vec::new();

        for _ in 0..20 {
            let len = rng.gen_range(1..2000);
            let mut entry = Vec::new();
            for _ in 0..len {
                entry.push(rng.r#gen());
            }
            entries.push(entry);
        }

        // Write the map
        let tmp = NamedTempFile::new()?;
        let mut writer = InvertedMapWriter::new(tmp.path())?;
        let mut indexes = Vec::new();

        for e in &entries {
            indexes.push(writer.write(e)?);
        }
        writer.finish()?;

        // Check the map has the same contents we initialized
        let reader = InvertedMapReader::open(tmp.path())?;
        for i in 0..20 {
            let indexed = reader.get(indexes[i]);
            assert_eq!(indexed, entries[i]);
        }

        Ok(())
    }
}
