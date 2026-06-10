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

use std::{collections::BTreeMap, path::Path};

use fst_index::FstIndexWriter;

use crate::{ParagraphAddr, VectorR, config::VectorConfig, data_store::DataStore};
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
pub fn build_indexes(work_path: &Path, config: &VectorConfig, data_store: &impl DataStore) -> VectorR<()> {
    if config.uses_relation_inverted_index() {
        RelationInvertedIndexes::build(work_path, data_store)
    } else {
        ParagraphInvertedIndexes::build(work_path, data_store)
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
    pub fn open(config: &VectorConfig, work_path: &Path, records: usize, options: OpenOptions) -> VectorR<Self> {
        if config.uses_relation_inverted_index() {
            Ok(InvertedIndexes::Relation(RelationInvertedIndexes::open(
                work_path, options,
            )?))
        } else {
            Ok(InvertedIndexes::Paragraph(ParagraphInvertedIndexes::open(
                work_path, records, options,
            )?))
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
