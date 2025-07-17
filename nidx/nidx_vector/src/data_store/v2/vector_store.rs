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

use lazy_static::lazy_static;
use memmap2::Mmap;
use std::{
    fs::File,
    io::{Seek, SeekFrom, Write as _},
    path::Path,
};

use crate::{
    config::VectorType,
    data_store::{OpenReason, ParagraphAddr, VectorAddr, VectorRef},
    data_types::usize_utils::U32_LEN,
};

const FILENAME: &str = "vectors.bin";

/// Storage for vectors of fixed size
/// For each vector, we store the vector and a pointer to the paragraph it appears on
/// so we can retrieve the rest of the metadata (paragraph_id, labels, etc.)
/// (encoded_vector: [u8], paragraph_addr: u32)
pub struct VectorStore {
    data: Mmap,
    vector_len_bytes: usize,
    record_len_bytes: usize,
}

fn padding_bytes(vector_type: &VectorType) -> usize {
    if vector_type.vector_alignment() > U32_LEN {
        vector_type.vector_alignment() - U32_LEN
    } else {
        0
    }
}

impl VectorStore {
    pub fn open(path: &Path, vector_type: &VectorType, reason: &OpenReason) -> std::io::Result<Self> {
        let data = unsafe { Mmap::map(&File::open(path.join(FILENAME))?)? };

        #[cfg(not(target_os = "windows"))]
        {
            let advice = match reason {
                OpenReason::Create => memmap2::Advice::Sequential,
                OpenReason::Search => memmap2::Advice::Random,
            };
            data.advise(advice)?;
        }

        let padding_bytes = padding_bytes(vector_type);
        Ok(Self {
            data,
            vector_len_bytes: vector_type.len_bytes(),
            record_len_bytes: vector_type.len_bytes() + U32_LEN + padding_bytes,
        })
    }

    pub fn get_vector(&self, addr: VectorAddr) -> VectorRef {
        let start = self.record_start(addr);
        let vector = &self.data[start..start + self.vector_len_bytes];
        let paragraph_bytes = &self.data[start + self.vector_len_bytes..start + self.vector_len_bytes + U32_LEN];
        let paragraph_addr = ParagraphAddr(u32::from_le_bytes((paragraph_bytes).try_into().unwrap()));
        VectorRef { vector, paragraph_addr }
    }

    pub fn size_bytes(&self) -> usize {
        self.data.len()
    }

    pub fn stored_elements(&self) -> usize {
        self.data.len() / (self.record_len_bytes)
    }

    fn record_start(&self, VectorAddr(addr): VectorAddr) -> usize {
        addr as usize * self.record_len_bytes
    }

    #[cfg(not(target_os = "windows"))]
    pub fn will_need(&self, addr: VectorAddr) {
        lazy_static! {
            static ref PAGE_SIZE: usize = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;
        };

        // Align node pointer to the start page, as required by madvise
        let start = self.data.as_ptr().wrapping_add(self.record_start(addr));
        let offset = start.align_offset(*PAGE_SIZE);
        let (start_page, advise_size) = if offset > 0 {
            (
                start.wrapping_add(offset).wrapping_sub(*PAGE_SIZE),
                self.record_len_bytes + *PAGE_SIZE - offset,
            )
        } else {
            (start, self.record_len_bytes)
        };

        unsafe { libc::madvise(start_page as *mut libc::c_void, advise_size, libc::MADV_WILLNEED) };
    }

    #[cfg(target_os = "windows")]
    pub fn will_need(src: &[u8], id: usize, vector_len: usize) {}
}

pub struct VectorStoreWriter {
    output: File,
    addr: u32,
    padding_bytes: usize,
}

impl VectorStoreWriter {
    pub fn new(path: &Path, vector_type: &VectorType) -> std::io::Result<Self> {
        Ok(Self {
            output: File::create(path.join(FILENAME))?,
            addr: 0,
            padding_bytes: padding_bytes(vector_type),
        })
    }

    pub fn write(
        &mut self,
        paragraph_id: u32,
        vectors: impl Iterator<Item = impl AsRef<[u8]>>,
    ) -> std::io::Result<(u32, u32)> {
        let first_addr = self.addr;
        for v in vectors {
            self.output.write_all(v.as_ref())?;
            self.output.write_all(paragraph_id.to_le_bytes().as_slice())?;
            if self.padding_bytes > 0 {
                self.output.seek(SeekFrom::Current(self.padding_bytes as i64))?;
            }
            self.addr += 1;
        }
        let last_addr = self.addr - 1;
        Ok((first_addr, last_addr))
    }

    pub fn close(self) -> std::io::Result<()> {
        self.output.sync_all()
    }
}
