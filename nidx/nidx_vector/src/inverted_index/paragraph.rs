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

use bit_set::BitSet;
use bit_vec::BitVec;

use super::{
    IndexBuilder, OpenOptions, file,
    fst_index::{FstIndexReader, FstIndexWriter},
    map::{InvertedMapReader, InvertedMapWriter},
};
use crate::{
    ParagraphAddr, VectorR,
    data_store::{DataStore, iter_paragraphs},
    formula::{BooleanOperator, Clause, Formula},
    utils::FieldKey,
};

pub struct ParagraphInvertedIndexes {
    field_index: FstIndexReader,
    label_index: FstIndexReader,
    records: usize,
}

pub struct FilterBitSet(BitSet);

impl FilterBitSet {
    pub fn new(len: usize, value: bool) -> Self {
        Self(BitSet::from_bit_vec(BitVec::from_elem(len, value)))
    }

    pub fn remove(&mut self, id: ParagraphAddr) {
        self.0.remove(id.0 as usize);
    }

    pub fn iter(&self) -> impl Iterator<Item = ParagraphAddr> {
        self.0.iter().map(|v| ParagraphAddr(v as u32))
    }

    pub fn intersect_with(&mut self, other: &FilterBitSet) {
        self.0.intersect_with(&other.0);
    }

    pub fn contains(&self, ParagraphAddr(id): ParagraphAddr) -> bool {
        self.0.contains(id as usize)
    }
}

/// The key for the labels, ending with a separator to allow for easy prefix search
fn labels_key(label: &str) -> Vec<u8> {
    [&label.as_bytes()[1..], "/".as_bytes()].concat()
}

impl ParagraphInvertedIndexes {
    pub fn space_usage(&self) -> usize {
        self.field_index.space_usage() + self.label_index.space_usage()
    }

    pub fn build(work_path: &Path, data_store: &impl DataStore) -> VectorR<()> {
        let mut field_builder = IndexBuilder::new();
        let mut label_builder = IndexBuilder::new();

        for paragraph_addr in iter_paragraphs(data_store) {
            let paragraph = data_store.get_paragraph(paragraph_addr);
            let key = paragraph.id();
            let labels = paragraph.labels();

            if let Some(key) = FieldKey::from_field_id(key) {
                field_builder.insert(key.bytes().to_vec(), paragraph_addr);
            }
            for l in labels {
                label_builder.insert(labels_key(&l), paragraph_addr);
            }
        }

        let mut map = InvertedMapWriter::new(&work_path.join(file::INDEX_MAP))?;

        let mut field_index = FstIndexWriter::new(&work_path.join(file::FIELD_INDEX), &mut map)?;
        field_builder.write(&mut field_index)?;
        field_index.finish()?;

        let mut label_index = FstIndexWriter::new(&work_path.join(file::LABEL_INDEX), &mut map)?;
        label_builder.write(&mut label_index)?;
        label_index.finish()?;

        map.finish()?;

        Ok(())
    }

    pub fn open(work_path: &Path, records: usize, options: OpenOptions) -> VectorR<Self> {
        let map = Arc::new(InvertedMapReader::open(
            &work_path.join(file::INDEX_MAP),
            options.prewarm,
        )?);
        let field_index = FstIndexReader::open(&work_path.join(file::FIELD_INDEX), map.clone(), options.prewarm)?;
        let label_index = FstIndexReader::open(&work_path.join(file::LABEL_INDEX), map, options.prewarm)?;

        Ok(Self {
            field_index,
            label_index,
            records,
        })
    }

    pub fn ids_for_deletion_key(&self, key: &FieldKey) -> impl Iterator<Item = ParagraphAddr> {
        self.field_index.get_prefix(key.bytes()).into_iter().map(ParagraphAddr)
    }

    pub fn filter(&self, formula: &Formula) -> Option<FilterBitSet> {
        formula
            .clauses
            .iter()
            .map(|f| self.filter_clause(f))
            .reduce(|mut a, b| {
                if formula.operator == BooleanOperator::And {
                    a.intersect_with(&b)
                } else {
                    a.union_with(&b)
                }
                a
            })
            .map(FilterBitSet)
    }

    fn filter_clause(&self, clause: &Clause) -> BitSet {
        match clause {
            Clause::Atom(atom_clause) => {
                let ids: &mut dyn Iterator<Item = u32> = match atom_clause {
                    crate::formula::AtomClause::Label(label) => {
                        &mut self.label_index.get_prefix(&labels_key(label)).into_iter()
                    }
                    crate::formula::AtomClause::KeyPrefixSet(field_ids) => &mut field_ids
                        .iter()
                        .filter_map(|id| FieldKey::from_field_id(id).map(|k| self.field_index.get(k.bytes())))
                        .flatten()
                        .flatten(),
                };
                let mut bitset = BitSet::from_bit_vec(BitVec::from_elem(self.records, false));
                ids.for_each(|id| {
                    bitset.insert(id as usize);
                });
                bitset
            }
            Clause::Compound(expression) => {
                let func: &dyn Fn(BitSet, BitSet) -> BitSet = match expression.operator {
                    crate::formula::BooleanOperator::Not | crate::formula::BooleanOperator::And => &|mut a, b| {
                        a.intersect_with(&b);
                        a
                    },
                    crate::formula::BooleanOperator::Or => &|mut a, b| {
                        a.union_with(&b);
                        a
                    },
                };
                let mut bitset = expression
                    .operands
                    .iter()
                    .map(|f| self.filter_clause(f))
                    .reduce(func)
                    .unwrap();
                if matches!(expression.operator, crate::formula::BooleanOperator::Not) {
                    bitset.get_mut().iter_mut().for_each(|mut f| *f = !*f);
                    bitset
                } else {
                    bitset
                }
            }
        }
    }
}
