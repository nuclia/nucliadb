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
use memmap2::{Mmap, MmapOptions};
use std::{
    fs::File,
    io::{BufWriter, Write as _},
    path::Path,
};

use crate::{
    data_store::{OpenReason, VectorAddr},
    vector_types::rabitq,
};

const FILENAME: &str = "vectors.quant";

/// Storage for quantized vectors of fixed size, just the vectors, metadata is with the raw vectors
pub struct QuantVectorStore {
    data: Mmap,
    vector_len_bytes: usize,
}

impl QuantVectorStore {
    pub fn open(path: &Path, vector_len_bytes: usize, reason: &OpenReason) -> std::io::Result<Self> {
        let mut options = MmapOptions::new();
        if matches!(reason, OpenReason::Search { prewarm: true }) {
            options.populate();
        }
        let data = unsafe { options.map(&File::open(path.join(FILENAME))?)? };

        #[cfg(not(target_os = "windows"))]
        {
            let advice = match reason {
                OpenReason::Create => memmap2::Advice::Sequential,
                OpenReason::Search { .. } => memmap2::Advice::Random,
            };
            data.advise(advice)?;
        }

        Ok(Self { data, vector_len_bytes })
    }

    pub fn get_vector(&self, addr: VectorAddr) -> rabitq::EncodedVector<'_> {
        let start = self.record_start(addr);
        rabitq::EncodedVector::from_bytes(&self.data[start..start + self.vector_len_bytes])
    }

    fn record_start(&self, VectorAddr(addr): VectorAddr) -> usize {
        addr as usize * self.vector_len_bytes
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
                self.vector_len_bytes + *PAGE_SIZE - offset,
            )
        } else {
            (start, self.vector_len_bytes)
        };

        unsafe { libc::madvise(start_page as *mut libc::c_void, advise_size, libc::MADV_WILLNEED) };
    }

    #[cfg(target_os = "windows")]
    pub fn will_need(src: &[u8], id: usize, vector_len: usize) {}
}

pub struct QuantVectorStoreWriter {
    output: BufWriter<File>,
}

impl QuantVectorStoreWriter {
    pub fn new(path: &Path) -> std::io::Result<Self> {
        Ok(Self {
            output: BufWriter::new(File::create(path.join(FILENAME))?),
        })
    }

    pub fn write(&mut self, vector: &[u8]) -> std::io::Result<()> {
        self.output.write_all(vector)
    }

    pub fn close(&mut self) -> std::io::Result<()> {
        self.output.flush()
    }
}
