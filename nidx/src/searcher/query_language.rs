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

use anyhow::anyhow;
use nidx_protos::{
    filter_expression::{Expr, FacetFilter, FilterExpressionList},
    FilterExpression,
};
use nidx_types::query_language::*;
use serde_json::Value as Json;

#[derive(Debug, Clone)]
struct Translated {
    has_field_labels: bool,
    has_paragraph_labels: bool,
    expression: BooleanExpression,
}

fn translate_literal(literal: String, context: &QueryContext) -> Translated {
    Translated {
        has_field_labels: context.field_labels.contains(&literal),
        has_paragraph_labels: context.paragraph_labels.contains(&literal),
        expression: BooleanExpression::Literal(literal),
    }
}

fn translate_not(inner: Json, context: &QueryContext) -> anyhow::Result<Translated> {
    let inner = translate_expression(inner, context)?;
    if matches!(&inner.expression, BooleanExpression::Not(_)) {
        return Ok(inner);
    }

    Ok(Translated {
        expression: BooleanExpression::Not(Box::new(inner.expression)),
        ..inner
    })
}

fn translate_operation(operator: Operator, operands: Vec<Json>, context: &QueryContext) -> anyhow::Result<Translated> {
    let mut has_field_labels = false;
    let mut has_paragraph_labels = false;
    let mut boolean_operation = BooleanOperation {
        operator,
        operands: Vec::with_capacity(operands.len()),
    };
    for json_operand in operands {
        let operand = translate_expression(json_operand, context)?;
        has_field_labels = has_field_labels || operand.has_field_labels;
        has_paragraph_labels = has_paragraph_labels || operand.has_paragraph_labels;
        boolean_operation.operands.push(operand.expression);
    }
    Ok(Translated {
        has_field_labels,
        has_paragraph_labels,
        expression: BooleanExpression::Operation(boolean_operation),
    })
}

fn translate_expression(query: Json, context: &QueryContext) -> anyhow::Result<Translated> {
    let Json::Object(json_object) = query else {
        #[rustfmt::skip] return Err(anyhow!(
            "Only json objects are valid roots for expressions: {query}"
        ));
    };
    let Some((root_id, inner_expression)) = json_object.into_iter().next() else {
        #[rustfmt::skip] return Err(anyhow!(
            "Empty objects are not supported by the schema"
        ));
    };
    match (root_id.as_str(), inner_expression) {
        ("literal", Json::String(literal)) => Ok(translate_literal(literal, context)),
        ("and", Json::Array(operands)) => translate_operation(Operator::And, operands, context),
        ("or", Json::Array(operands)) => translate_operation(Operator::Or, operands, context),
        ("not", operand) => translate_not(operand, context),
        (root_id, _) => Err(anyhow!("{root_id} is not a valid expression")),
    }
}

struct ExpressionSplit {
    fields_only: BooleanExpression,
    paragraphs_only: BooleanExpression,
}

fn split_mixed(expression: BooleanExpression, context: &QueryContext) -> anyhow::Result<ExpressionSplit> {
    let BooleanExpression::Operation(expression) = expression else {
        #[rustfmt::skip] return Err(anyhow!(
            "This function can not operate with literals"
        ));
    };
    if !matches!(expression.operator, Operator::And) {
        #[rustfmt::skip] return Err(anyhow!(
            "Only mixed ANDs can be splitted"
        ));
    }

    let operands = expression.operands;
    let mut fields_only = Vec::new();
    let mut paragraphs_only = Vec::new();

    for operand in operands {
        let literal = match &operand {
            BooleanExpression::Literal(literal) => literal,
            BooleanExpression::Not(subexpression) => match subexpression.as_ref() {
                BooleanExpression::Literal(literal) => literal,
                #[rustfmt::skip] _ => return Err(anyhow!(
                    "Nested expressions can not be splitted"
                )),
            },
            #[rustfmt::skip] _ => return Err(anyhow!(
                "Nested expressions can not be splitted"
            )),
        };
        if context.field_labels.contains(literal) {
            fields_only.push(operand);
        } else {
            paragraphs_only.push(operand)
        }
    }

    Ok(ExpressionSplit {
        fields_only: if fields_only.len() == 1 {
            fields_only.pop().unwrap()
        } else {
            BooleanExpression::Operation(BooleanOperation {
                operator: Operator::And,
                operands: fields_only,
            })
        },
        paragraphs_only: if paragraphs_only.len() == 1 {
            paragraphs_only.pop().unwrap()
        } else {
            BooleanExpression::Operation(BooleanOperation {
                operator: Operator::And,
                operands: paragraphs_only,
            })
        },
    })
}

#[derive(Default)]
pub struct QueryAnalysis {
    pub labels_prefilter_query: Option<BooleanExpression>,
    pub keywords_prefilter_query: Option<BooleanExpression>,
    pub search_query: Option<BooleanExpression>,
}

pub fn translate(
    labels_query: Option<&str>,
    keywords_query: Option<&str>,
    context: &QueryContext,
) -> anyhow::Result<QueryAnalysis> {
    let mut labels_prefilter_queries = Vec::new();
    let mut keywords_prefilter_queries = Vec::new();
    let mut search_queries = Vec::new();

    // Translate labels query. Some of the labels are paragraph-type labels that need to be passed a search query.
    if let Some(labels_query) = labels_query {
        if !labels_query.is_empty() {
            let Json::Object(labels_subexpressions) = serde_json::from_str(labels_query)? else {
                #[rustfmt::skip] return Err(anyhow!(
                    "Unexpected labels query format {labels_query}"
                ));
            };

            for (root_id, expression) in labels_subexpressions {
                let as_json_expression = serde_json::json!({ root_id: expression });
                let translation_report = translate_expression(as_json_expression, context)?;
                if translation_report.has_field_labels && translation_report.has_paragraph_labels {
                    let splitted = split_mixed(translation_report.expression, context)?;
                    labels_prefilter_queries.push(splitted.fields_only);
                    search_queries.push(splitted.paragraphs_only);
                } else if translation_report.has_field_labels {
                    labels_prefilter_queries.push(translation_report.expression);
                } else if translation_report.has_paragraph_labels {
                    search_queries.push(translation_report.expression);
                }
            }
        }
    }

    // Translate keywords query. All the labels are field-type labels that need to be passed a prefilter query.
    if let Some(keywords_query) = keywords_query {
        if !keywords_query.is_empty() {
            let Json::Object(keywords_subexpressions) = serde_json::from_str(keywords_query)? else {
                #[rustfmt::skip] return Err(anyhow!(
                    "Unexpected keywords query format {keywords_query}"
                ));
            };
            for (root_id, expression) in keywords_subexpressions {
                let as_json_expression = serde_json::json!({ root_id: expression });
                let translation_report = translate_expression(as_json_expression, context)?;
                keywords_prefilter_queries.push(translation_report.expression);
            }
        }
    }

    Ok(QueryAnalysis {
        labels_prefilter_query: if labels_prefilter_queries.len() <= 1 {
            labels_prefilter_queries.pop()
        } else {
            Some(BooleanExpression::Operation(BooleanOperation {
                operator: Operator::And,
                operands: labels_prefilter_queries,
            }))
        },

        keywords_prefilter_query: if keywords_prefilter_queries.len() <= 1 {
            keywords_prefilter_queries.pop()
        } else {
            Some(BooleanExpression::Operation(BooleanOperation {
                operator: Operator::And,
                operands: keywords_prefilter_queries,
            }))
        },

        search_query: if search_queries.len() <= 1 {
            search_queries.pop()
        } else {
            Some(BooleanExpression::Operation(BooleanOperation {
                operator: Operator::And,
                operands: search_queries,
            }))
        },
    })
}

/// Extract an expression only involving some labels if it's an AND subset of the total expression
pub fn extract_label_filters(expression: &FilterExpression, labels: &[&str]) -> Option<BooleanExpression> {
    match expression.expr.as_ref().unwrap() {
        Expr::Facet(FacetFilter {
            facet,
        }) if labels.contains(&facet.as_str()) => Some(BooleanExpression::Literal(facet.clone())),
        Expr::BoolNot(not_expr) => {
            extract_label_filters(not_expr.as_ref(), labels).map(|e| BooleanExpression::Not(Box::new(e)))
        }
        Expr::BoolAnd(FilterExpressionList {
            operands,
        }) => {
            let relevant: Vec<_> = operands.iter().filter_map(|e| extract_label_filters(e, labels)).collect();
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
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn do_proper_analysis() {
        #[rustfmt::skip] let context = QueryContext {
            paragraph_labels: HashSet::from([
                "foo".to_string(),
            ]),
            field_labels: HashSet::from([
                "var".to_string(),
            ]),
        };
        let labels_query = serde_json::json!({
            "and": [
                { "not": {"literal": "var"}},
                {"literal": "foo"},
            ],
            "literal": "var",
        });

        let not_var = BooleanExpression::Not(Box::new(BooleanExpression::Literal("var".to_string())));
        let expected_prefilter = BooleanExpression::Operation(BooleanOperation {
            operator: Operator::And,
            operands: vec![not_var, BooleanExpression::Literal("var".to_string())],
        });
        let expected_search = BooleanExpression::Literal("foo".to_string());

        let analysis = translate(Some(&labels_query.to_string()), None, &context).unwrap();
        let Some(labels_prefilter_query) = analysis.labels_prefilter_query else {
            panic!("The json used should produce a prefilter")
        };
        let Some(search_query) = analysis.search_query else {
            panic!("The json used should produce a search query")
        };
        assert_eq!(labels_prefilter_query, expected_prefilter);
        assert_eq!(search_query, expected_search);
        assert!(analysis.keywords_prefilter_query.is_none());
    }

    #[test]
    fn translate_expression_and_nested() {
        #[rustfmt::skip] let context = QueryContext {
            paragraph_labels: HashSet::from([
                "foo".to_string(),
            ]),
            field_labels: HashSet::from([
                "var".to_string(),
            ]),
        };
        let query = serde_json::json!({
            "and": [
                { "not": {"literal": "var"}},
                {
                    "and": [
                        {"literal": "var"},
                        {"literal": "foo"},
                    ]
                }
            ]
        });

        let inner_and = BooleanExpression::Operation(BooleanOperation {
            operator: Operator::And,
            operands: vec![
                BooleanExpression::Literal("var".to_string()),
                BooleanExpression::Literal("foo".to_string()),
            ],
        });
        let not_var = BooleanExpression::Not(Box::new(BooleanExpression::Literal("var".to_string())));
        let expected = BooleanExpression::Operation(BooleanOperation {
            operator: Operator::And,
            operands: vec![not_var, inner_and],
        });

        let translation = translate_expression(query, &context).unwrap();
        assert!(translation.has_paragraph_labels);
        assert!(translation.has_field_labels);
        assert_eq!(translation.expression, expected);
    }

    #[test]
    fn translate_expression_and() {
        #[rustfmt::skip] let context = QueryContext {
            paragraph_labels: HashSet::from([
                "foo".to_string(),
            ]),
            field_labels: HashSet::from([
                "var".to_string(),
            ]),
        };
        let query = serde_json::json!({
            "and": [
                {"literal": "var"},
                {"literal": "foo"},
            ]
        });
        let expected = BooleanExpression::Operation(BooleanOperation {
            operator: Operator::And,
            operands: vec![
                BooleanExpression::Literal("var".to_string()),
                BooleanExpression::Literal("foo".to_string()),
            ],
        });

        let translation = translate_expression(query, &context).unwrap();
        assert!(translation.has_paragraph_labels);
        assert!(translation.has_field_labels);
        assert_eq!(translation.expression, expected);
    }

    #[test]
    fn translate_expression_json_or() {
        #[rustfmt::skip] let context = QueryContext {
            paragraph_labels: HashSet::with_capacity(0),
            field_labels: HashSet::from([
                "var".to_string(),
                "foo".to_string(),
            ]),
        };
        let query = serde_json::json!({
            "or": [
                {"literal": "var"},
                {"literal": "foo"},
            ]
        });
        let expected = BooleanExpression::Operation(BooleanOperation {
            operator: Operator::Or,
            operands: vec![
                BooleanExpression::Literal("var".to_string()),
                BooleanExpression::Literal("foo".to_string()),
            ],
        });

        let translation = translate_expression(query, &context).unwrap();
        assert!(!translation.has_paragraph_labels);
        assert!(translation.has_field_labels);
        assert_eq!(translation.expression, expected);
    }

    #[test]
    fn translate_expression_not() {
        #[rustfmt::skip] let context = QueryContext {
            paragraph_labels: HashSet::with_capacity(0),
            field_labels: HashSet::from([
                "var".to_string()
            ]),
        };
        let query = serde_json::json!({
            "not": {
                "literal": "var",
            }
        });
        let expected = BooleanExpression::Not(Box::new(BooleanExpression::Literal("var".to_string())));

        let translation = translate_expression(query, &context).unwrap();
        assert!(!translation.has_paragraph_labels);
        assert!(translation.has_field_labels);
        assert_eq!(translation.expression, expected);
    }

    #[test]
    fn translate_expression_literal() {
        let expected = BooleanExpression::Literal("var".to_string());

        #[rustfmt::skip] let context = QueryContext {
            paragraph_labels: HashSet::with_capacity(0),
            field_labels: HashSet::from([
                "var".to_string()
            ]),
        };
        let query = serde_json::json!({
            "literal": "var",
        });

        let translation = translate_expression(query, &context).unwrap();
        assert!(!translation.has_paragraph_labels);
        assert!(translation.has_field_labels);
        assert_eq!(translation.expression, expected);

        #[rustfmt::skip] let context = QueryContext {
            field_labels: HashSet::with_capacity(0),
            paragraph_labels: HashSet::from([
                "var".to_string()
            ]),
        };
        let query = serde_json::json!({
            "literal": "var",
        });
        let translation = translate_expression(query, &context).unwrap();
        assert!(!translation.has_field_labels);
        assert!(translation.has_paragraph_labels);
        assert_eq!(translation.expression, expected);
    }

    #[test]
    fn test_split() {
        #[rustfmt::skip] let context = QueryContext {
            paragraph_labels: HashSet::from([
                "foo_paragraph".to_string(),
                "var_paragraph".to_string()
            ]),
            field_labels: HashSet::from([
                "foo_field".to_string(),
                "var_field".to_string()
            ]),
        };
        let mixed_and = BooleanExpression::Operation(BooleanOperation {
            operator: Operator::And,
            operands: vec![
                BooleanExpression::Literal("foo_field".to_string()),
                BooleanExpression::Literal("foo_paragraph".to_string()),
                BooleanExpression::Literal("var_field".to_string()),
                BooleanExpression::Literal("var_paragraph".to_string()),
            ],
        });

        let splitted_expression = split_mixed(mixed_and, &context).unwrap();
        let BooleanExpression::Operation(fields_only) = splitted_expression.fields_only else {
            panic!("Unexpected variant");
        };
        assert!(matches!(fields_only.operator, Operator::And));
        assert_eq!(fields_only.operands.len(), context.field_labels.len());
        for operand in fields_only.operands {
            if let BooleanExpression::Literal(literal) = operand {
                assert!(context.field_labels.contains(&literal));
            } else {
                panic!("Ill formed split");
            }
        }

        let BooleanExpression::Operation(paragraphs_only) = splitted_expression.paragraphs_only else {
            panic!("Unexpected variant");
        };
        assert!(matches!(paragraphs_only.operator, Operator::And));
        assert_eq!(paragraphs_only.operands.len(), context.field_labels.len());
        for operand in paragraphs_only.operands {
            if let BooleanExpression::Literal(literal) = operand {
                assert!(context.paragraph_labels.contains(&literal));
            } else {
                panic!("Ill formed split");
            }
        }
    }

    #[test]
    fn test_extract_label_filters() {
        const LABELS: &[&str] = &["/v", "/w"];

        let a = FilterExpression {
            expr: Some(Expr::Facet(FacetFilter {
                facet: "/a".into(),
            })),
        };
        let v = FilterExpression {
            expr: Some(Expr::Facet(FacetFilter {
                facet: "/v".into(),
            })),
        };
        let w = FilterExpression {
            expr: Some(Expr::Facet(FacetFilter {
                facet: "/w".into(),
            })),
        };

        let not = |e| FilterExpression {
            expr: Some(Expr::BoolNot(Box::new(e))),
        };

        // Literal
        assert_eq!(extract_label_filters(&a, LABELS), None);
        assert_eq!(extract_label_filters(&v, LABELS), Some(BooleanExpression::Literal("/v".into())));

        // Not literal
        assert_eq!(extract_label_filters(&not(a.clone()), LABELS), None);

        assert_eq!(
            extract_label_filters(&not(v.clone()), LABELS),
            Some(BooleanExpression::Not(Box::new(BooleanExpression::Literal("/v".into()))))
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
        assert_eq!(extract_label_filters(&expr, LABELS), Some(BooleanExpression::Literal("/v".into())));
    }
}
