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

use std::{path::Path, sync::Arc};

use super::{
    IndexBuilder, OpenOptions, file,
    fst_index::{FstIndexReader, FstIndexWriter},
    map::{InvertedMapReader, InvertedMapWriter},
};
use crate::{
    ParagraphAddr, VectorR,
    data_store::{DataStore, iter_paragraphs},
    utils::FieldKey,
};

pub struct RelationInvertedIndexes {
    field_index: FstIndexReader,
}

impl RelationInvertedIndexes {
    pub fn space_usage(&self) -> usize {
        self.field_index.space_usage()
    }

    pub fn open(work_path: &Path, options: OpenOptions) -> VectorR<Self> {
        let map = Arc::new(InvertedMapReader::open(
            &work_path.join(file::INDEX_MAP),
            options.prewarm,
        )?);
        let field_index = FstIndexReader::open(&work_path.join(file::FIELD_INDEX), map, options.prewarm)?;

        Ok(Self { field_index })
    }

    pub fn ids_for_field_key(&self, field_key: &FieldKey) -> impl Iterator<Item = ParagraphAddr> {
        self.field_index
            .get_prefix(field_key.bytes())
            .into_iter()
            .map(ParagraphAddr)
    }

    pub fn build(work_path: &Path, data_store: &impl DataStore) -> VectorR<()> {
        let mut field_builder = IndexBuilder::new();

        for paragraph_addr in iter_paragraphs(data_store) {
            let paragraph = data_store.get_paragraph(paragraph_addr);
            let field_key = FieldKey::from_bytes(paragraph.metadata());
            field_builder.insert(field_key.bytes().to_vec(), paragraph_addr);
        }

        let mut map = InvertedMapWriter::new(&work_path.join(file::INDEX_MAP))?;

        let mut field_index = FstIndexWriter::new(&work_path.join(file::FIELD_INDEX), &mut map)?;
        field_builder.write(&mut field_index)?;
        field_index.finish()?;

        map.finish()?;

        Ok(())
    }
}
