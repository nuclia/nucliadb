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

use std::{path::Path, sync::Arc};

use super::{
    IndexBuilder, OpenOptions, file,
    fst_index::{FstIndexReader, FstIndexWriter},
    map::{InvertedMapReader, InvertedMapWriter},
};
use crate::{
    ParagraphAddr, VectorR,
    data_store::{DataStore, iter_paragraphs},
    field_list_metadata::decode_field_list_metadata,
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
            let fields = decode_field_list_metadata(paragraph.metadata());

            for field_key in fields {
                field_builder.insert(field_key.bytes().to_vec(), paragraph_addr);
            }
        }

        let mut map = InvertedMapWriter::new(&work_path.join(file::INDEX_MAP))?;

        let mut field_index = FstIndexWriter::new(&work_path.join(file::FIELD_INDEX), &mut map)?;
        field_builder.write(&mut field_index)?;
        field_index.finish()?;

        map.finish()?;

        Ok(())
    }
}
