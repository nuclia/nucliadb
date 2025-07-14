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

use crate::{data_store::ParagraphRef, data_types::usize_utils::U32_LEN};

const FILENAME_DATA: &str = "paragraphs.bin";
const FILENAME_POS: &str = "paragraphs.pos";

#[derive(bincode::Encode, bincode::BorrowDecode, Clone)]
pub struct StoredParagraph<'a> {
    key: &'a str,
    labels: Vec<&'a str>,
    metadata: &'a [u8],
    first_vector_add: u32,
    num_vectors: u32,
}

impl<'a> StoredParagraph<'a> {
    pub fn key(&self) -> &str {
        self.key
    }

    pub fn metadata(&self) -> &[u8] {
        self.metadata
    }

    pub fn labels(&self) -> Vec<String> {
        self.labels.iter().copied().map(str::to_string).collect()
    }
}

/// Storage for paragraphs metadata
/// Since the data is of variable size, we store pointers to the data start in a different file
/// for quick indexing.
pub struct ParagraphStore {
    pos: Mmap,
    data: Mmap,
}

impl ParagraphStore {
    pub fn open(path: &Path) -> std::io::Result<Self> {
        let pos = unsafe { Mmap::map(&File::open(path.join(FILENAME_POS))?)? };
        let data = unsafe { Mmap::map(&File::open(path.join(FILENAME_DATA))?)? };

        // TODO: Maybe different flags for read (random) and merge (sequential / willneed)
        #[cfg(not(target_os = "windows"))]
        {
            pos.advise(memmap2::Advice::WillNeed)?;
            data.advise(memmap2::Advice::Random)?;
        }

        Ok(Self { pos, data })
    }

    pub fn get_paragraph(&self, vector_addr: u32) -> ParagraphRef {
        let start_bytes = &self.pos[vector_addr as usize * 4..vector_addr as usize * 4 + 4];
        let start = u32::from_be_bytes(start_bytes.try_into().unwrap()) as usize;
        let (paragraph, _): (StoredParagraph, _) =
            bincode::borrow_decode_from_slice(&self.data[start..], bincode::config::standard()).unwrap();
        ParagraphRef::V2(paragraph)
    }

    pub fn size_bytes(&self) -> usize {
        self.data.len()
    }
}

pub struct ParagraphStoreWriter {
    output: File,
    addr: u32,
}

impl ParagraphStoreWriter {
    pub fn new(path: &Path) -> std::io::Result<Self> {
        Ok(Self {
            output: File::create(path.join(FILENAME_DATA))?,
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
