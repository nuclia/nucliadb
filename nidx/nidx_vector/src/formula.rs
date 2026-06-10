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

use std::collections::HashSet;

/// Is a singleton clause.
#[derive(Debug, Clone, PartialEq)]
pub enum AtomClause {
    Label(String),
    KeyPrefixSet(HashSet<String>),
}
impl AtomClause {
    pub fn label(value: String) -> AtomClause {
        Self::Label(value)
    }
    pub fn key_set(field_set: HashSet<String>) -> AtomClause {
        Self::KeyPrefixSet(field_set)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BooleanOperator {
    Not,
    Or,
    And,
}

/// Is a clause formed by the conjuction of several LabelClauses. Additionally this
/// clause has a threshold that specifies the minimum number of AtomClauses that have to
/// succeed in order for the overall conjuction to be satisfied.
#[derive(Debug, Clone, PartialEq)]
pub struct CompoundClause {
    pub operator: BooleanOperator,
    pub operands: Vec<Clause>,
}
impl CompoundClause {
    pub fn new(operator: BooleanOperator, operands: Vec<Clause>) -> CompoundClause {
        CompoundClause { operator, operands }
    }
}

/// Wrapper that unifies the different types of clauses a formula may have.
#[derive(Debug, Clone, PartialEq)]
pub enum Clause {
    Atom(AtomClause),
    Compound(CompoundClause),
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
#[derive(Debug, Clone, PartialEq)]
pub struct Formula {
    pub clauses: Vec<Clause>,
    pub operator: BooleanOperator,
}
impl Formula {
    pub fn new() -> Formula {
        Formula {
            operator: BooleanOperator::And,
            clauses: Vec::new(),
        }
    }
    pub fn extend<C>(&mut self, clause: C)
    where
        Clause: From<C>,
    {
        self.clauses.push(clause.into())
    }
    pub fn non_empty(&self) -> bool {
        !self.clauses.is_empty()
    }
}

impl Default for Formula {
    fn default() -> Self {
        Self::new()
    }
}
