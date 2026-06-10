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

use nidx_protos::{
    FilterExpression,
    filter_expression::{Expr, FacetFilter, FilterExpressionList},
};
use nidx_types::query_language::*;

/// Extract an expression only involving some labels if it's an AND subset of the total expression
pub fn extract_label_filters(expression: &FilterExpression, labels: &[&str]) -> Option<BooleanExpression<String>> {
    match expression.expr.as_ref().unwrap() {
        Expr::Facet(FacetFilter { facet }) if labels.contains(&facet.as_str()) => {
            Some(BooleanExpression::Literal(facet.clone()))
        }
        Expr::BoolNot(not_expr) => {
            extract_label_filters(not_expr.as_ref(), labels).map(|e| BooleanExpression::Not(Box::new(e)))
        }
        Expr::BoolAnd(FilterExpressionList { operands }) => {
            let relevant: Vec<_> = operands
                .iter()
                .filter_map(|e| extract_label_filters(e, labels))
                .collect();
            match &relevant[..] {
                [] => None,
                [expression] => Some(expression.clone()),
                _ => Some(BooleanExpression::Operation(BooleanOperation {
                    operator: Operator::And,
                    operands: relevant,
                })),
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_label_filters() {
        const LABELS: &[&str] = &["/v", "/w"];

        let a = FilterExpression {
            expr: Some(Expr::Facet(FacetFilter { facet: "/a".into() })),
        };
        let v = FilterExpression {
            expr: Some(Expr::Facet(FacetFilter { facet: "/v".into() })),
        };
        let w = FilterExpression {
            expr: Some(Expr::Facet(FacetFilter { facet: "/w".into() })),
        };

        let not = |e| FilterExpression {
            expr: Some(Expr::BoolNot(Box::new(e))),
        };

        // Literal
        assert_eq!(extract_label_filters(&a, LABELS), None);
        assert_eq!(
            extract_label_filters(&v, LABELS),
            Some(BooleanExpression::Literal("/v".into()))
        );

        // Not literal
        assert_eq!(extract_label_filters(&not(a.clone()), LABELS), None);

        assert_eq!(
            extract_label_filters(&not(v.clone()), LABELS),
            Some(BooleanExpression::Not(Box::new(BooleanExpression::Literal(
                "/v".into()
            ))))
        );

        // Or (not supported)
        let or_expr = FilterExpression {
            expr: Some(Expr::BoolOr(FilterExpressionList {
                operands: vec![a.clone(), v.clone(), not(w.clone())],
            })),
        };
        assert_eq!(extract_label_filters(&or_expr, LABELS), None);

        // And
        let expr = FilterExpression {
            expr: Some(Expr::BoolAnd(FilterExpressionList {
                operands: vec![a.clone(), v.clone(), not(w.clone())],
            })),
        };
        let expected = BooleanExpression::Operation(BooleanOperation {
            operator: Operator::And,
            operands: vec![
                BooleanExpression::Literal("/v".into()),
                BooleanExpression::Not(Box::new(BooleanExpression::Literal("/w".into()))),
            ],
        });
        assert_eq!(extract_label_filters(&expr, LABELS), Some(expected));

        // Nested
        let expr = FilterExpression {
            expr: Some(Expr::BoolAnd(FilterExpressionList {
                operands: vec![a, v, or_expr],
            })),
        };
        assert_eq!(
            extract_label_filters(&expr, LABELS),
            Some(BooleanExpression::Literal("/v".into()))
        );
    }
}
