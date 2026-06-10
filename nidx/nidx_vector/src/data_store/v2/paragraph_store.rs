// Copyright 2021 Bosutech XXI S.L.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

use memmap2::Mmap;
use std::{
    fs::File,
    io::{BufWriter, Write as _},
    path::Path,
};

use crate::{
    VectorR,
    data_store::{OpenReason, ParagraphAddr, ParagraphRef},
    data_types::usize_utils::U32_LEN,
    segment::Elem,
    utils::wincode_config,
};

const FILENAME_DATA: &str = "paragraphs.bin";
const FILENAME_POS: &str = "paragraphs.pos";

#[derive(wincode::SchemaWrite, wincode::SchemaRead, Clone)]
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
                OpenReason::Search { .. } => memmap2::Advice::Random,
            };
            data.advise(advice)?;
        }

        Ok(Self { pos, data })
    }

    pub fn get_paragraph(&self, ParagraphAddr(addr): ParagraphAddr) -> ParagraphRef<'_> {
        let start_bytes = &self.pos[addr as usize * U32_LEN..addr as usize * U32_LEN + U32_LEN];
        let start = u32::from_le_bytes(start_bytes.try_into().unwrap()) as usize;
        let paragraph: StoredParagraph<'_> =
            wincode::config::deserialize(&self.data[start..], wincode_config()).unwrap();
        ParagraphRef::V2(paragraph)
    }

    pub fn size_bytes(&self) -> usize {
        self.data.len() + self.pos.len()
    }

    pub fn stored_elements(&self) -> u32 {
        (self.pos.len() / U32_LEN) as u32
    }
}

pub struct ParagraphStoreWriter {
    data: BufWriter<File>,
    pos: BufWriter<File>,
    data_pos: u32,
    addr: u32,
}

impl ParagraphStoreWriter {
    pub fn new(path: &Path) -> std::io::Result<Self> {
        Ok(Self {
            pos: BufWriter::new(File::create(path.join(FILENAME_POS))?),
            data: BufWriter::new(File::create(path.join(FILENAME_DATA))?),
            data_pos: 0,
            addr: 0,
        })
    }

    pub fn write(&mut self, paragraph: StoredParagraph) -> VectorR<u32> {
        let written = wincode::config::serialized_size(&paragraph, wincode_config())?;
        wincode::config::serialize_into(
            wincode::io::std_write::WriteAdapter::new(&mut self.data),
            &paragraph,
            wincode_config(),
        )?;
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
        let written = wincode::config::serialized_size(&paragraph, wincode_config())?;
        wincode::config::serialize_into(
            wincode::io::std_write::WriteAdapter::new(&mut self.data),
            &paragraph,
            wincode_config(),
        )?;
        self.pos.write_all(&self.data_pos.to_le_bytes())?;
        self.data_pos += written as u32;
        self.addr += 1;

        Ok(self.addr)
    }

    pub fn close(&mut self) -> std::io::Result<()> {
        self.data.flush()?;
        self.pos.flush()?;
        Ok(())
    }
}
