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
