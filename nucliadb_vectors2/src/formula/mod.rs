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

/// Is a singleton clause formed by label. It will be satisfied only if the address
/// is applied to has the label.
#[derive(Debug, Clone)]
pub struct LabelClause {
    value: String,
}
impl LabelClause {
    pub fn new(labels: String) -> LabelClause {
        LabelClause { value: labels }
    }
    fn run<D: DataRetriever>(&self, x: Address, retriever: &D) -> bool {
        retriever.has_label(x, self.value.as_bytes())
    }
}

/// Is a clause formed by the conjuction of several LabelClauses. Additionally this
/// clause has a threshold that specifies the minimum number of LabelClauses that have to
/// succeed in order for the overall conjuction to be satisfied.
#[derive(Debug, Clone)]
pub struct CompoundClause {
    threshold: usize,
    labels: Vec<LabelClause>,
}
impl CompoundClause {
    pub fn new(threshold: usize, labels: Vec<LabelClause>) -> CompoundClause {
        CompoundClause { threshold, labels }
    }
    fn run<D: DataRetriever>(&self, x: Address, retriever: &D) -> bool {
        let number_of_subqueries = self.labels.len();
        let mut threshold = self.threshold;
        let mut i = 0;
        while threshold > 0 && i <= number_of_subqueries {
            let is_valid = self.labels[i].run(x, retriever);
            threshold -= is_valid as usize;
            i += 1;
        }
        threshold == 0
    }
}

/// Wrapper that unifies the different types of clauses a formula may have.
#[derive(Debug, Clone)]
pub enum Clause {
    Label(LabelClause),
    Compound(CompoundClause),
}

impl Clause {
    fn run<D: DataRetriever>(&self, x: Address, retriever: &D) -> bool {
        match self {
            Clause::Compound(q) => q.run(x, retriever),
            Clause::Label(q) => q.run(x, retriever),
        }
    }
}

impl From<LabelClause> for Clause {
    fn from(value: LabelClause) -> Self {
        Clause::Label(value)
    }
}

impl From<CompoundClause> for Clause {
    fn from(value: CompoundClause) -> Self {
        Clause::Compound(value)
    }
}

/// Formulas are boolean expressions in conjuctive normal form, but for labels.
/// The clauses in a formula are connected by intersections, and they are formed
/// by strings. Once applied to a given address, the formula becomes a boolean
/// expression that evaluates to whether the address is valid or not.
#[derive(Debug, Clone, Default)]
pub struct Formula {
    clauses: Vec<Clause>,
}
impl Formula {
    pub fn new() -> Formula {
        Formula::default()
    }
    pub fn extend<C>(&mut self, clause: C)
    where Clause: From<C> {
        self.clauses.push(clause.into())
    }
    pub fn run<D: DataRetriever>(&self, x: Address, retriever: &D) -> bool {
        self.clauses.iter().all(|q| q.run(x, retriever))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::data_point::Address;
    struct DummyRetriever {
        labels: HashSet<&'static [u8]>,
    }
    impl DataRetriever for DummyRetriever {
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
    }
    #[test]
    fn test_query() {
        const L1: &str = "Label1";
        const L2: &str = "Label2";
        const L3: &str = "Label3";
        const ADDRESS: Address = Address::dummy();
        let retriever = DummyRetriever {
            labels: [L1.as_bytes(), L3.as_bytes()].into_iter().collect(),
        };
        let mut formula = Formula::new();
        formula.extend(LabelClause::new(L1.to_string()));
        formula.extend(LabelClause::new(L3.to_string()));
        assert!(formula.run(ADDRESS, &retriever));

        let mut formula = Formula::new();
        formula.extend(LabelClause::new(L1.to_string()));
        formula.extend(LabelClause::new(L2.to_string()));
        assert!(!formula.run(ADDRESS, &retriever));

        let mut formula = Formula::new();
        let inner = vec![
            LabelClause::new(L1.to_string()),
            LabelClause::new(L2.to_string()),
        ];
        formula.extend(CompoundClause::new(1, inner));
        assert!(formula.run(ADDRESS, &retriever));
    }
}
