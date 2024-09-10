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

use crate::node_error;
use crate::NodeResult;
use serde_json::Value as Json;
use std::collections::HashSet;

pub struct QueryContext {
    pub paragraph_labels: HashSet<String>,
    pub field_labels: HashSet<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Operator {
    And,
    Or,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct BooleanOperation {
    pub operator: Operator,
    pub operands: Vec<BooleanExpression>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum BooleanExpression {
    Literal(String),
    Not(Box<BooleanExpression>),
    Operation(BooleanOperation),
}

#[derive(Debug, Clone)]
struct Translated {
    is_nested: bool,
    has_field_labels: bool,
    has_paragraph_labels: bool,
    expression: BooleanExpression,
}

fn translate_literal(literal: String, context: &QueryContext) -> Translated {
    Translated {
        is_nested: false,
        has_field_labels: context.field_labels.contains(&literal),
        has_paragraph_labels: context.paragraph_labels.contains(&literal),
        expression: BooleanExpression::Literal(literal),
    }
}

fn translate_not(inner: Json, context: &QueryContext) -> NodeResult<Translated> {
    let inner = translate_expression(inner, context)?;
    if matches!(&inner.expression, BooleanExpression::Not(_)) {
        return Ok(inner);
    }

    Ok(Translated {
        is_nested: matches!(&inner.expression, BooleanExpression::Not(_)),
        expression: BooleanExpression::Not(Box::new(inner.expression)),
        ..inner
    })
}

fn translate_operation(operator: Operator, operands: Vec<Json>, context: &QueryContext) -> NodeResult<Translated> {
    let mut is_nested = false;
    let mut has_field_labels = false;
    let mut has_paragraph_labels = false;
    let mut boolean_operation = BooleanOperation {
        operator,
        operands: Vec::with_capacity(operands.len()),
    };
    for json_operand in operands {
        let operand = translate_expression(json_operand, context)?;
        if let BooleanExpression::Operation(_) = &operand.expression {
            is_nested = true;
        }
        has_field_labels = has_field_labels || operand.has_field_labels;
        has_paragraph_labels = has_paragraph_labels || operand.has_paragraph_labels;
        boolean_operation.operands.push(operand.expression);
    }
    Ok(Translated {
        is_nested,
        has_field_labels,
        has_paragraph_labels,
        expression: BooleanExpression::Operation(boolean_operation),
    })
}

fn translate_expression(query: Json, context: &QueryContext) -> NodeResult<Translated> {
    let Json::Object(json_object) = query else {
        #[rustfmt::skip] return Err(node_error!(
            "Only json objects are valid roots for expressions: {query}"
        ));
    };
    let Some((root_id, inner_expression)) = json_object.into_iter().next() else {
        #[rustfmt::skip] return Err(node_error!(
            "Empty objects are not supported by the schema"
        ));
    };
    match (root_id.as_str(), inner_expression) {
        ("literal", Json::String(literal)) => Ok(translate_literal(literal, context)),
        ("and", Json::Array(operands)) => translate_operation(Operator::And, operands, context),
        ("or", Json::Array(operands)) => translate_operation(Operator::Or, operands, context),
        ("not", operand) => translate_not(operand, context),
        (root_id, _) => Err(node_error!("{root_id} is not a valid expression")),
    }
}

struct ExpressionSplit {
    fields_only: BooleanExpression,
    paragraphs_only: BooleanExpression,
}

fn split_mixed(expression: BooleanExpression, context: &QueryContext) -> NodeResult<ExpressionSplit> {
    let BooleanExpression::Operation(expression) = expression else {
        #[rustfmt::skip] return Err(node_error!(
            "This function can not operate with literals"
        ));
    };
    if !matches!(expression.operator, Operator::And) {
        #[rustfmt::skip] return Err(node_error!(
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
                #[rustfmt::skip] _ => return Err(node_error!(
                    "Nested expressions can not be splitted"
                )),
            },
            #[rustfmt::skip] _ => return Err(node_error!(
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
) -> NodeResult<QueryAnalysis> {
    let mut labels_prefilter_queries = Vec::new();
    let mut keywords_prefilter_queries = Vec::new();
    let mut search_queries = Vec::new();

    // Translate labels query. Some of the labels are paragraph-type labels that need to be passed a search query.
    if let Some(labels_query) = labels_query {
        if !labels_query.is_empty() {
            let Json::Object(labels_subexpressions) = serde_json::from_str(labels_query)? else {
                #[rustfmt::skip] return Err(node_error!(
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
                #[rustfmt::skip] return Err(node_error!(
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

#[cfg(test)]
mod tests {
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
        assert!(matches!(labels_prefilter_query, expected_prefilter));
        assert!(matches!(expected_search, expected_search));
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
        assert!(translation.is_nested);
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
        assert!(!translation.is_nested);
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
        assert!(!translation.is_nested);
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
        assert!(!translation.is_nested);
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
        assert!(!translation.is_nested);
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
        assert!(!translation.is_nested);
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
}
