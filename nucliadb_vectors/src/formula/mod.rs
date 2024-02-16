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

use crate::data_point::{Address, DataRetriever};

#[derive(Debug, Copy, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub enum AtomKind {
    KeyPrefix,
    Label,
}

/// Is a singleton clause.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct AtomClause {
    kind: AtomKind,
    value: String,
}
impl AtomClause {
    pub fn new(value: String, kind: AtomKind) -> AtomClause {
        AtomClause {
            kind,
            value,
        }
    }
    pub fn label(value: String) -> AtomClause {
        AtomClause::new(value, AtomKind::Label)
    }
    pub fn key_prefix(value: String) -> AtomClause {
        AtomClause::new(value, AtomKind::KeyPrefix)
    }
    fn run<D: DataRetriever>(&self, x: Address, retriever: &D) -> bool {
        match self.kind {
            AtomKind::KeyPrefix => retriever.get_key(x).starts_with(self.value.as_bytes()),
            AtomKind::Label => retriever.has_label(x, self.value.as_bytes()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum BooleanOperator {
    Not,
    Or,
    And,
}

/// Is a clause formed by the conjuction of several LabelClauses. Additionally this
/// clause has a threshold that specifies the minimum number of AtomClauses that have to
/// succeed in order for the overall conjuction to be satisfied.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct CompoundClause {
    operator: BooleanOperator,
    operands: Vec<Clause>,
}
impl CompoundClause {
    pub fn len(&self) -> usize {
        self.operands.len()
    }
    pub fn is_empty(&self) -> bool {
        self.operands.is_empty()
    }
    pub fn new(operator: BooleanOperator, operands: Vec<Clause>) -> CompoundClause {
        CompoundClause {
            operator,
            operands,
        }
    }
    fn run<D: DataRetriever>(&self, x: Address, retriever: &D) -> bool {
        if self.is_empty() {
            return true;
        }

        let mut subquery_iterator = self.operands.iter();
        match self.operator {
            BooleanOperator::And => subquery_iterator.all(|subquery| subquery.run(x, retriever)),
            BooleanOperator::Or => subquery_iterator.any(|subquery| subquery.run(x, retriever)),
            BooleanOperator::Not => !subquery_iterator.all(|subquery| subquery.run(x, retriever)),
        }
    }
}

/// Wrapper that unifies the different types of clauses a formula may have.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub enum Clause {
    Atom(AtomClause),
    Compound(CompoundClause),
}

impl Clause {
    fn run<D: DataRetriever>(&self, x: Address, retriever: &D) -> bool {
        match self {
            Clause::Compound(q) => q.run(x, retriever),
            Clause::Atom(q) => q.run(x, retriever),
        }
    }
}

impl From<AtomClause> for Clause {
    fn from(value: AtomClause) -> Self {
        Clause::Atom(value)
    }
}

impl From<CompoundClause> for Clause {
    fn from(value: CompoundClause) -> Self {
        Clause::Compound(value)
    }
}

#[derive(Default)]
pub struct AtomCollector {
    pub labels: Vec<String>,
    pub key_prefixes: Vec<String>,
}
impl AtomCollector {
    fn add(&mut self, atom: AtomClause) {
        match atom.kind {
            AtomKind::KeyPrefix => self.key_prefixes.push(atom.value),
            AtomKind::Label => self.labels.push(atom.value),
        }
    }
}

/// Once applied to a given address, the formula becomes a boolean
/// expression that evaluates to whether the address is valid or not.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct Formula {
    clauses: Vec<Clause>,
}
impl Formula {
    pub fn new() -> Formula {
        Formula::default()
    }
    pub fn extend<C>(&mut self, clause: C)
    where
        Clause: From<C>,
    {
        self.clauses.push(clause.into())
    }
    pub fn run<D: DataRetriever>(&self, x: Address, retriever: &D) -> bool {
        self.clauses.iter().all(|q| q.run(x, retriever))
    }
    /// Returns the atoms that form a formula
    pub fn get_atoms(&self) -> AtomCollector {
        let mut collector = AtomCollector::default();
        let mut work: Vec<_> = self.clauses.iter().collect();
        while let Some(clause) = work.pop() {
            match clause {
                Clause::Atom(q) => collector.add(q.clone()),
                Clause::Compound(clause) => {
                    for clause in clause.operands.iter() {
                        work.push(clause);
                    }
                }
            }
        }
        collector
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::data_point::Address;
    struct DummyRetriever {
        key: &'static [u8],
        labels: HashSet<&'static [u8]>,
    }
    impl DataRetriever for DummyRetriever {
        fn get_key(&self, _: Address) -> &[u8] {
            self.key
        }
        fn has_label(&self, _: Address, label: &[u8]) -> bool {
            self.labels.contains(label)
        }
        fn is_deleted(&self, _: Address) -> bool {
            panic!("Not meant to be used")
        }
        fn similarity(&self, _: Address, _: Address) -> f32 {
            panic!("Not meant to be used")
        }
        fn get_vector(&self, _: Address) -> &[u8] {
            panic!("Not meant to be used")
        }
        fn min_score(&self) -> f32 {
            -1.0
        }
    }
    #[test]
    fn test_query() {
        const KEY: &str = "/This/is/a/key";
        const L1: &str = "Label1";
        const L2: &str = "Label2";
        const L3: &str = "Label3";
        const ADDRESS: Address = Address::dummy();
        let retriever = DummyRetriever {
            key: KEY.as_bytes(),
            labels: [L1.as_bytes(), L3.as_bytes()].into_iter().collect(),
        };
        let mut formula = Formula::new();
        formula.extend(AtomClause::label(L1.to_string()));
        formula.extend(AtomClause::label(L3.to_string()));
        assert!(formula.run(ADDRESS, &retriever));

        let mut formula = Formula::new();
        formula.extend(AtomClause::label(L1.to_string()));
        formula.extend(AtomClause::label(L2.to_string()));
        assert!(!formula.run(ADDRESS, &retriever));

        #[rustfmt::skip] let inner = vec![
            Clause::Atom(AtomClause::label(L1.to_string())), 
            Clause::Atom(AtomClause::label(L2.to_string()))
        ];
        let mut formula = Formula::new();
        formula.extend(CompoundClause::new(BooleanOperator::Or, inner));
        assert!(formula.run(ADDRESS, &retriever));

        let mut formula = Formula::new();
        let inner = vec![Clause::Atom(AtomClause::key_prefix("/This/is".to_string()))];
        formula.extend(CompoundClause::new(BooleanOperator::Or, inner));
        assert!(formula.run(ADDRESS, &retriever));
        let mut formula = Formula::new();

        let inner = vec![Clause::Atom(AtomClause::key_prefix("/This/is/not".to_string()))];
        formula.extend(CompoundClause::new(BooleanOperator::Or, inner));
        assert!(!formula.run(ADDRESS, &retriever));
    }
}
