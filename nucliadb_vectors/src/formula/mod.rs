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

use std::collections::HashSet;

use crate::data_point::{Address, DataRetriever};

/// Is a singleton clause.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub enum AtomClause {
    KeyPrefix(String),
    Label(String),
    KeyPrefixSet((HashSet<String>, HashSet<String>)),
    KeyField((String, String)),
}
impl AtomClause {
    pub fn label(value: String) -> AtomClause {
        Self::Label(value)
    }
    pub fn key_prefix(value: String) -> AtomClause {
        Self::KeyPrefix(value)
    }
    pub fn key_set(resource_set: HashSet<String>, field_set: HashSet<String>) -> AtomClause {
        Self::KeyPrefixSet((resource_set, field_set))
    }
    pub fn key_field(field_type: String, field_name: String) -> AtomClause {
        Self::KeyField((field_type, field_name))
    }
    fn run<D: DataRetriever>(&self, x: Address, retriever: &D) -> bool {
        match self {
            Self::KeyPrefix(value) => retriever.get_key(x).starts_with(value.as_bytes()),
            Self::Label(value) => retriever.has_label(x, value.as_bytes()),
            Self::KeyPrefixSet((resource_set, field_set)) => {
                let key = retriever.get_key(x);
                let mut key_parts = key.split(|b| *b == b'/');

                // Matches resource_id
                let resource_id = std::str::from_utf8(key_parts.next().unwrap()).unwrap();
                let matches_resource = resource_set.contains(resource_id);
                if matches_resource {
                    return true;
                }

                // Matches field_id (key up to the third slash)
                let mut slash_count = 0;
                let mut end_pos = 0;
                for char in key.iter() {
                    if *char == b'/' {
                        slash_count += 1;
                        if slash_count == 3 {
                            break;
                        }
                    }
                    end_pos += 1;
                }
                // slash_count = 2 if we reach the end of string with 2 middle slashes
                if slash_count < 2 {
                    return false;
                }

                field_set.contains(std::str::from_utf8(&key[0..end_pos]).unwrap())
            }
            Self::KeyField((field_type, field_name)) => {
                let key = retriever.get_key(x);
                let mut key_parts = key.split(|b| *b == b'/');
                // Skip resource_id, then try to parse field_type and field_name and check if they match
                if key_parts.next().is_some() {
                    if let Some(ftype) = key_parts.next() {
                        if let Some(fname) = key_parts.next() {
                            let ftype_str = std::str::from_utf8(ftype).unwrap();
                            let fname_str = std::str::from_utf8(fname).unwrap();
                            return ftype_str == field_type && fname_str == field_name;
                        }
                    }
                }
                false
            }
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
        fn will_need(&self, _x: Address) {
            // noop
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

    #[test]
    fn test_key_filters() {
        let resource_id = String::from("015163a0629e4f368aa9d54978d2a9ff");
        const FIELD_ID: &str = "015163a0629e4f368aa9d54978d2a9ff/f/file1";
        let field_id = String::from(FIELD_ID);

        let fake_resource_id = String::from("badcafe0badcafe0badcafe0badcafe0");
        let fake_field_id = format!("{resource_id}/f/not_a_field");

        const ADDRESS: Address = Address::dummy();
        let retriever = DummyRetriever {
            key: FIELD_ID.as_bytes(),
            labels: HashSet::new(),
        };

        // Find by resource_id
        let mut formula = Formula::new();
        formula.extend(AtomClause::key_prefix(resource_id.clone()));
        assert!(formula.run(ADDRESS, &retriever));

        let mut formula = Formula::new();
        formula.extend(AtomClause::key_prefix(fake_resource_id.clone()));
        assert!(!formula.run(ADDRESS, &retriever));

        // Find by field_id
        let mut formula = Formula::new();
        formula.extend(AtomClause::key_prefix(field_id.clone()));
        assert!(formula.run(ADDRESS, &retriever));

        let mut formula = Formula::new();
        formula.extend(AtomClause::key_prefix(fake_field_id.clone()));
        assert!(!formula.run(ADDRESS, &retriever));

        // Find by set of resource_ids
        let mut formula = Formula::new();
        formula.extend(AtomClause::key_set([resource_id.clone()].into_iter().collect(), HashSet::new()));
        assert!(formula.run(ADDRESS, &retriever));

        let mut formula = Formula::new();
        formula.extend(AtomClause::key_set([fake_resource_id.clone()].into_iter().collect(), HashSet::new()));
        assert!(!formula.run(ADDRESS, &retriever));

        // Find by set of field_ids
        let mut formula = Formula::new();
        formula.extend(AtomClause::key_set(HashSet::new(), [field_id.clone()].into_iter().collect()));
        assert!(formula.run(ADDRESS, &retriever));

        let mut formula = Formula::new();
        formula.extend(AtomClause::key_set(HashSet::new(), [fake_field_id.clone()].into_iter().collect()));
        assert!(!formula.run(ADDRESS, &retriever));
    }
}
