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

use std::{collections::BTreeMap, path::Path, sync::Arc};

use bit_set::BitSet;
use bit_vec::BitVec;
use fst_index::{FstIndexReader, FstIndexWriter};
use map::{InvertedMapReader, InvertedMapWriter};
use tracing::warn;

use crate::{
    data_point::node::Node,
    data_types::data_store::{self},
    formula::{BooleanOperator, Clause, Formula},
    VectorR,
};

mod fst_index;
mod map;

mod file {
    pub const INDEX_MAP: &str = "index.map";
    pub const FIELD_INDEX: &str = "field.fst";
    pub const LABEL_INDEX: &str = "label.fst";
}

/// The key for the field index. [uuid_as_bytes, field_type/field_name]
fn field_id_key(paragraph_key: &str) -> Option<Vec<u8>> {
    let mut parts = paragraph_key.split('/');
    if let Some(uuid) = parts.next() {
        if let Some(field_type) = parts.next() {
            if let Some(field_name) = parts.next() {
                return Some(
                    [
                        uuid::Uuid::parse_str(uuid).unwrap().as_bytes(),
                        field_type.as_bytes(),
                        "/".as_bytes(),
                        field_name.as_bytes(),
                    ]
                    .concat(),
                );
            }
        } else {
            return Some(uuid::Uuid::parse_str(uuid).unwrap().as_bytes().to_vec());
        }
    }
    warn!(?paragraph_key, "Unable to parse field id from key");
    None
}

/// The key for the labels, ending with a separator to allow for easy prefix search
fn labels_key(label: &str) -> Vec<u8> {
    [label[1..].as_bytes(), "/".as_bytes()].concat()
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

    pub fn insert(&mut self, key: Vec<u8>, id: u32) {
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

/// Build indexes from a nodes.kv file.
pub fn build_indexes(work_path: &Path, nodes: &[u8]) -> VectorR<()> {
    let mut field_builder = IndexBuilder::new();
    let mut label_builder = IndexBuilder::new();

    for id in 0..data_store::stored_elements(nodes) {
        let node = data_store::get_value(nodes, id);
        let key = Node::key(node);
        let labels = Node::labels(node);

        let id = id as u32;
        if let Some(key) = field_id_key(std::str::from_utf8(key).unwrap()) {
            field_builder.insert(key, id);
        }
        for l in labels {
            label_builder.insert(labels_key(&l), id);
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

pub struct InvertedIndexes {
    field_index: FstIndexReader,
    label_index: FstIndexReader,
    records: usize,
}

impl InvertedIndexes {
    pub fn exists(path: &Path) -> bool {
        path.join(file::INDEX_MAP).exists()
    }

    pub fn open(work_path: &Path, records: usize) -> VectorR<Self> {
        let map = Arc::new(InvertedMapReader::open(&work_path.join(file::INDEX_MAP))?);
        let field_index = FstIndexReader::open(&work_path.join(file::FIELD_INDEX), map.clone())?;
        let label_index = FstIndexReader::open(&work_path.join(file::LABEL_INDEX), map)?;

        Ok(Self {
            field_index,
            label_index,
            records,
        })
    }

    pub fn ids_for_deletion_key(&self, key: &str) -> Option<impl Iterator<Item = u32>> {
        field_id_key(key).map(|key| self.field_index.get_prefix(&key).into_iter())
    }

    pub fn filter(&self, formula: &Formula) -> Option<BitSet> {
        formula.clauses.iter().map(|f| self.filter_clause(f)).reduce(|mut a, b| {
            if formula.operator == BooleanOperator::And {
                a.intersect_with(&b)
            } else {
                a.union_with(&b)
            }
            a
        })
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
                        .filter_map(|id| field_id_key(id).map(|k| self.field_index.get(&k)))
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
                let mut bitset = expression.operands.iter().map(|f| self.filter_clause(f)).reduce(func).unwrap();
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
