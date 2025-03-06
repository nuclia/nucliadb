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

use crate::formula::*;
use nidx_types::query_language::*;

fn map_literal(literal: &str) -> Clause {
    Clause::Atom(AtomClause::label(literal.to_string()))
}

fn map_not(inner: &BooleanExpression<String>) -> Clause {
    let operator = BooleanOperator::Not;
    let operand = map_expression(inner);
    let operands = vec![operand];

    Clause::Compound(CompoundClause::new(operator, operands))
}

fn map_operation(operation: &BooleanOperation<String>) -> Clause {
    let operator = match operation.operator {
        Operator::And => BooleanOperator::And,
        Operator::Or => BooleanOperator::Or,
    };
    let operands = operation.operands.iter().map(map_expression).collect();

    Clause::Compound(CompoundClause::new(operator, operands))
}

pub fn map_expression(expression: &BooleanExpression<String>) -> Clause {
    match expression {
        BooleanExpression::Not(inner) => map_not(inner),
        BooleanExpression::Literal(literal) => map_literal(literal),
        BooleanExpression::Operation(operation) => map_operation(operation),
    }
}
