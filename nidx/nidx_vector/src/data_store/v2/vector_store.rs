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

use memmap2::Mmap;
use std::{fs::File, io::Write as _, path::Path};

use crate::{config::VectorType, data_types::usize_utils::U32_LEN};

const FILENAME: &str = "vectors.bin";

struct StoreV2Vector<'a>(&'a [u8]);

impl StoreV2Vector<'_> {
    fn vector(&self) -> &[u8] {
        &self.0[0..self.0.len() - U32_LEN]
    }

    // fn paragraph(&self, store: impl DataStore) -> u32 {
    //     0
    // }
}

/// Storage for vectors of fixed size
/// For each vector, we store the vector and a pointer to the paragraph it appears on
/// so we can retrieve the rest of the metadata (paragraph_id, labels, etc.)
/// (encoded_vector: [u8], paragraph_addr: u32)
struct VectorStore {
    data: Mmap,
    vector_len: usize,
}

impl VectorStore {
    pub fn open(path: &Path, vector_type: VectorType) -> std::io::Result<Self> {
        let data = unsafe { Mmap::map(&File::open(path.join(FILENAME))?)? };

        // TODO: Maybe different flags for read (random) and merge (sequential / willneed)
        #[cfg(not(target_os = "windows"))]
        {
            data.advise(memmap2::Advice::Random)?;
        }

        Ok(Self {
            data,
            vector_len: vector_type.dimension().unwrap() + U32_LEN,
        })
    }

    pub fn get_vector(&self, vector_addr: u32) -> StoreV2Vector {
        let start = vector_addr as usize * self.vector_len;
        StoreV2Vector(&self.data[start..start + self.vector_len])
    }
}

pub struct VectorStoreWriter {
    output: File,
    addr: u32,
}

impl VectorStoreWriter {
    pub fn new(path: &Path) -> std::io::Result<Self> {
        Ok(Self {
            output: File::create(path.join(FILENAME))?,
            addr: 0,
        })
    }

    pub fn write(&mut self, paragraph_id: u32, vectors: &[&[u8]]) -> std::io::Result<(u32, u32)> {
        let first_addr = self.addr;
        for v in vectors {
            self.output.write_all(v)?;
            self.output.write_all(paragraph_id.to_le_bytes().as_slice())?;
            // TODO: Alignment
        }
        self.addr += vectors.len() as u32;
        let last_addr = self.addr - 1;
        Ok((first_addr, last_addr))
    }

    pub fn close(self) -> std::io::Result<()> {
        self.output.sync_all()
    }
}
