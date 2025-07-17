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

use crate::{
    VectorR,
    data_store::{OpenReason, ParagraphAddr, ParagraphRef},
    data_types::usize_utils::U32_LEN,
    segment::Elem,
};

const FILENAME_DATA: &str = "paragraphs.bin";
const FILENAME_POS: &str = "paragraphs.pos";

#[derive(bincode::Encode, bincode::BorrowDecode, Clone)]
pub struct StoredParagraph<'a> {
    key: &'a str,
    labels: Vec<&'a str>,
    metadata: &'a [u8],
    first_vector: u32,
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
        self.labels.iter().map(|s| s.to_string()).collect()
    }

    pub fn from_elem(elem: &'a Elem, first_vector: u32) -> Self {
        StoredParagraph {
            key: &elem.key,
            labels: elem.labels.iter().map(String::as_str).collect(),
            metadata: elem.metadata.as_ref().map_or(&[], |x| x),
            first_vector,
            num_vectors: elem.vectors.len() as u32,
        }
    }

    pub fn vector_first_and_len(&self) -> (u32, u32) {
        (self.first_vector, self.num_vectors)
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
    pub fn open(path: &Path, reason: &OpenReason) -> std::io::Result<Self> {
        let pos = unsafe { Mmap::map(&File::open(path.join(FILENAME_POS))?)? };
        let data = unsafe { Mmap::map(&File::open(path.join(FILENAME_DATA))?)? };

        #[cfg(not(target_os = "windows"))]
        {
            pos.advise(memmap2::Advice::WillNeed)?;
            let advice = match reason {
                OpenReason::Create => memmap2::Advice::Sequential,
                OpenReason::Search => memmap2::Advice::Random,
            };
            data.advise(advice)?;
        }

        Ok(Self { pos, data })
    }

    pub fn get_paragraph(&self, ParagraphAddr(addr): ParagraphAddr) -> ParagraphRef {
        let start_bytes = &self.pos[addr as usize * U32_LEN..addr as usize * U32_LEN + U32_LEN];
        let start = u32::from_le_bytes(start_bytes.try_into().unwrap()) as usize;
        let (paragraph, _) =
            bincode::borrow_decode_from_slice(&self.data[start..], bincode::config::standard()).unwrap();
        ParagraphRef::V2(paragraph)
    }

    pub fn size_bytes(&self) -> usize {
        self.data.len() + self.pos.len()
    }

    pub fn stored_elements(&self) -> usize {
        self.pos.len() / U32_LEN
    }
}

pub struct ParagraphStoreWriter {
    data: File,
    pos: File,
    data_pos: u32,
    addr: u32,
}

impl ParagraphStoreWriter {
    pub fn new(path: &Path) -> std::io::Result<Self> {
        Ok(Self {
            pos: File::create(path.join(FILENAME_POS))?,
            data: File::create(path.join(FILENAME_DATA))?,
            data_pos: 0,
            addr: 0,
        })
    }

    pub fn write(&mut self, paragraph: StoredParagraph) -> VectorR<u32> {
        let written = bincode::encode_into_std_write(paragraph, &mut self.data, bincode::config::standard())?;
        self.pos.write_all(&self.data_pos.to_le_bytes())?;
        self.data_pos += written as u32;
        self.addr += 1;

        Ok(self.addr)
    }

    pub fn write_paragraph_ref(
        &mut self,
        paragraph: ParagraphRef,
        first_vector: u32,
        num_vectors: u32,
    ) -> VectorR<u32> {
        let labels = paragraph.labels();
        let paragraph = StoredParagraph {
            key: paragraph.id(),
            labels: labels.iter().map(|x| x.as_str()).collect(),
            metadata: paragraph.metadata(),
            first_vector,
            num_vectors,
        };
        let written = bincode::encode_into_std_write(paragraph, &mut self.data, bincode::config::standard())?;
        self.pos.write_all(&self.data_pos.to_le_bytes())?;
        self.data_pos += written as u32;
        self.addr += 1;

        Ok(self.addr)
    }

    pub fn close(self) -> std::io::Result<()> {
        self.data.sync_all()?;
        self.pos.sync_all()?;
        Ok(())
    }
}
