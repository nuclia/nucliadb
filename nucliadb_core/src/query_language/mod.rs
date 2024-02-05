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

#![allow(unused)]

use std::collections::LinkedList;

use serde_json::Value as JsonExpression;

/// Operators allowed in negated normal form.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum NnfOperator {
    And,
    Or,
    Not,
}

/// Used to apply [`NnfOperator`]s to [`NnfExpression`]s.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct NnfClause {
    operator: NnfOperator,
    operands: LinkedList<NnfExpression>,
}

/// A [`NnfExpression`] is one formed by using literals and [`NnfOperator`]s.
/// Every propositional formula must be transformed to this form before it can
/// be written in conjunctive normal form.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum NnfExpression {
    Clause(NnfClause),
    Literal(String),
}

fn transform_literal(negated: bool, literal: String) -> NnfExpression {
    if !negated {
        NnfExpression::Literal(literal)
    } else {
        NnfExpression::Clause(NnfClause {
            operator: NnfOperator::Not,
            operands: LinkedList::from([NnfExpression::Literal(literal)]),
        })
    }
}

fn transform_not(negated: bool, operand: JsonExpression) -> NnfExpression {
    transform(!negated, operand)
}

fn transform_other(negated: bool, operator: NnfOperator, json_operands: Vec<JsonExpression>) -> NnfExpression {
    let mut operands = LinkedList::new();
    let operator = match operator {
        NnfOperator::And if negated => NnfOperator::Or,
        NnfOperator::Or if negated => NnfOperator::And,
        operator => operator,
    };

    for json_operand in json_operands {
        let nnf_expression = transform(negated, json_operand);
        let NnfExpression::Clause(mut clause) = nnf_expression else {
            operands.push_back(nnf_expression);
            continue;
        };
        if clause.operator == operator {
            operands.append(&mut clause.operands);
        } else {
            operands.push_back(NnfExpression::Clause(clause));
        }
    }

    NnfExpression::Clause(NnfClause {
        operator,
        operands,
    })
}

fn transform(negated: bool, expression: JsonExpression) -> NnfExpression {
    let JsonExpression::Object(json_object) = expression else {
        panic!("This function should only be called with json objects");
    };
    let Some((key, expression)) = json_object.into_iter().next() else {
        panic!("Empty objects are not valid expressions");
    };
    match (key.as_str(), expression) {
        ("literal", JsonExpression::String(literal)) => transform_literal(negated, literal),
        ("and", JsonExpression::Array(operands)) => transform_other(negated, NnfOperator::And, operands),
        ("or", JsonExpression::Array(operands)) => transform_other(negated, NnfOperator::Or, operands),
        ("not", operand) => transform_not(negated, operand),
        ill_formed => panic!("Unexpected expression: {ill_formed:?} "),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn literal_to_nnf() {
        let json_expression = serde_json::json!({
            "literal": "var",
        });
        let expected = NnfExpression::Literal("var".to_string());
        let computed = transform(false, json_expression);

        assert_eq!(expected, computed);
    }

    #[test]
    fn negated_literal_to_nnf() {
        let json_expression = serde_json::json!({
            "not": {
                "literal": "var",
            }
        });
        let expected = NnfExpression::Clause(NnfClause {
            operator: NnfOperator::Not,
            operands: LinkedList::from([NnfExpression::Literal("var".to_string())]),
        });
        let computed = transform(false, json_expression);
        assert_eq!(expected, computed);
    }

    #[test]
    fn double_negation_literal_to_nnf() {
        let json_expression = serde_json::json!({
            "not": {
                "not": {
                    "literal": "var",
                }
            }
        });
        let expected = NnfExpression::Literal("var".to_string());
        let computed = transform(false, json_expression);
        assert_eq!(expected, computed);
    }

    #[test]
    fn nested_formulas_to_nnf() {
        let json_expression = serde_json::json!({
            "and": [
                {
                    "not": {
                        "or": [
                            { "literal": "var" },
                            { "not": { "literal": "foo"} }
                        ],
                    }
                },
                {
                    "or": [
                        { "literal": "var" },
                        { "literal": "foo" },
                    ]
                }
            ]
        });

        // Expected formula is: !var AND foo AND (var OR foo)
        let var = NnfExpression::Literal("var".to_string());
        let foo = NnfExpression::Literal("foo".to_string());
        let not_var = NnfExpression::Clause(NnfClause {
            operator: NnfOperator::Not,
            operands: LinkedList::from([var.clone()]),
        });
        let var_or_foo = NnfExpression::Clause(NnfClause {
            operator: NnfOperator::Or,
            operands: LinkedList::from([var, foo.clone()]),
        });
        let expected = NnfExpression::Clause(NnfClause {
            operator: NnfOperator::And,
            operands: LinkedList::from([not_var, foo, var_or_foo]),
        });
        let computed = transform(false, json_expression);
        assert_eq!(expected, computed);
    }
}
