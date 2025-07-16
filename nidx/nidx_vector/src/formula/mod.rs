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
}

impl Default for Formula {
    fn default() -> Self {
        Self::new()
    }
}
