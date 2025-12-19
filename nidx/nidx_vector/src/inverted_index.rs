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

use std::{collections::BTreeMap, path::Path};

use fst_index::FstIndexWriter;

use crate::{ParagraphAddr, VectorR, config::IndexEntity, data_store::DataStore};
use paragraph::ParagraphInvertedIndexes;
use relation::RelationInvertedIndexes;

pub use paragraph::FilterBitSet;

mod fst_index;
mod map;
mod paragraph;
mod relation;

mod file {
    pub const INDEX_MAP: &str = "index.map";
    pub const FIELD_INDEX: &str = "field.fst";
    pub const LABEL_INDEX: &str = "label.fst";
}

/// Helper to build indexes when the input is not sorted by key
struct IndexBuilder {
    ordered_keys: BTreeMap<Vec<u8>, Vec<u32>>,
}

impl IndexBuilder {
    pub fn new() -> Self {
        Self {
            ordered_keys: Default::default(),
        }
    }

    pub fn insert(&mut self, key: Vec<u8>, ParagraphAddr(id): ParagraphAddr) {
        self.ordered_keys.entry(key).or_default().push(id);
    }

    pub fn write(self, index_writer: &mut FstIndexWriter) -> VectorR<()> {
        for (key, mut values) in self.ordered_keys {
            values.sort();
            index_writer.write(&key, &values)?;
        }

        Ok(())
    }
}

/// Build indexes from a DataStore.
pub fn build_indexes(work_path: &Path, indexes: &IndexEntity, data_store: &impl DataStore) -> VectorR<()> {
    match indexes {
        IndexEntity::Paragraph => ParagraphInvertedIndexes::build(work_path, data_store),
        IndexEntity::Relation => RelationInvertedIndexes::build(work_path, data_store),
    }
}

pub struct OpenOptions {
    pub prewarm: bool,
}

pub enum InvertedIndexes {
    Paragraph(ParagraphInvertedIndexes),
    Relation(RelationInvertedIndexes),
}

impl InvertedIndexes {
    pub fn open(index_type: &IndexEntity, work_path: &Path, records: usize, options: OpenOptions) -> VectorR<Self> {
        match index_type {
            IndexEntity::Paragraph => Ok(InvertedIndexes::Paragraph(ParagraphInvertedIndexes::open(
                work_path, records, options,
            )?)),
            IndexEntity::Relation => Ok(InvertedIndexes::Relation(RelationInvertedIndexes::open(
                work_path, options,
            )?)),
        }
    }

    pub fn space_usage(&self) -> usize {
        match self {
            InvertedIndexes::Paragraph(indexes) => indexes.space_usage(),
            InvertedIndexes::Relation(indexes) => indexes.space_usage(),
        }
    }

    pub fn exists(path: &Path) -> bool {
        path.join(file::INDEX_MAP).exists()
    }
}
