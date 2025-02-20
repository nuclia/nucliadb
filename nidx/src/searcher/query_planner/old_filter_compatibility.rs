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

use nidx_protos::filter_expression::date_range_filter::DateField;
use nidx_protos::filter_expression::{
    DateRangeFilter, Expr, FacetFilter, FieldFilter, FilterExpressionList, KeywordFilter, ResourceFilter,
};
use nidx_protos::FilterExpression;
use nidx_protos::SearchRequest;

use nidx_types::query_language::*;

use crate::searcher::query_language;

#[allow(deprecated)]
fn analyze_filter(search_request: &SearchRequest) -> anyhow::Result<query_language::QueryAnalysis> {
    let Some(filter) = &search_request.filter else {
        return Ok(Default::default());
    };
    let context = QueryContext {
        field_labels: filter.field_labels.iter().cloned().collect(),
        paragraph_labels: filter.paragraph_labels.iter().cloned().collect(),
    };
    query_language::translate(Some(&filter.labels_expression), Some(&filter.keywords_expression), &context)
}

/// Builds a filter expression from the older filter fields
#[allow(deprecated)]
pub fn filter_from_request(
    request: &SearchRequest,
) -> anyhow::Result<(Option<FilterExpression>, Option<FilterExpression>)> {
    let mut field_filters = vec![];

    // Fields
    if !request.fields.is_empty() {
        let field_from_string = |field: &str| {
            let mut parts = field.split('/');
            FieldFilter {
                field_type: parts.next().unwrap().into(),
                field_id: parts.next().map(str::to_string),
            }
        };
        let fields = request
            .fields
            .iter()
            .map(|f| FilterExpression {
                expr: Some(Expr::Field(field_from_string(f))),
            })
            .collect();
        field_filters.push(FilterExpression {
            expr: Some(Expr::Or(FilterExpressionList {
                expr: fields,
            })),
        });
    }

    // Key filters
    if !request.key_filters.is_empty() {
        let filter_from_string = |key: &String| {
            let mut parts = key.split('/');
            let resource_id = parts.next().unwrap().into();
            let field = parts.next().map(|field_type| FieldFilter {
                field_type: field_type.into(),
                field_id: parts.next().map(str::to_string),
            });
            if let Some(field) = field {
                FilterExpression {
                    expr: Some(Expr::And(FilterExpressionList {
                        expr: vec![
                            FilterExpression {
                                expr: Some(Expr::Resource(ResourceFilter {
                                    resource_id,
                                })),
                            },
                            FilterExpression {
                                expr: Some(Expr::Field(field)),
                            },
                        ],
                    })),
                }
            } else {
                FilterExpression {
                    expr: Some(Expr::Resource(ResourceFilter {
                        resource_id,
                    })),
                }
            }
        };
        let resources = request.key_filters.iter().map(filter_from_string).collect();
        field_filters.push(FilterExpression {
            expr: Some(Expr::Or(FilterExpressionList {
                expr: resources,
            })),
        });
    }

    // Timestamps
    if let Some(ts) = request.timestamps {
        if ts.from_created.is_some() || ts.to_created.is_some() {
            field_filters.push(FilterExpression {
                expr: Some(Expr::Date(DateRangeFilter {
                    field: DateField::Created.into(),
                    from: ts.from_created,
                    to: ts.to_created,
                })),
            });
        }
        if ts.from_modified.is_some() || ts.to_modified.is_some() {
            field_filters.push(FilterExpression {
                expr: Some(Expr::Date(DateRangeFilter {
                    field: DateField::Modified.into(),
                    from: ts.from_modified,
                    to: ts.to_modified,
                })),
            });
        }
    }

    // Filters (keyword and label, for field and paragraph)
    let query_analysis = analyze_filter(request)?;
    if let Some(facets_expression) = query_analysis.labels_prefilter_query {
        field_filters.push(bool_to_filter(facets_expression, |facet| FilterExpression {
            expr: Some(Expr::Facet(FacetFilter {
                facet,
            })),
        }));
    }
    if let Some(keywords_expression) = query_analysis.keywords_prefilter_query {
        field_filters.push(bool_to_filter(keywords_expression, |keyword| FilterExpression {
            expr: Some(Expr::Keyword(KeywordFilter {
                keyword,
            })),
        }));
    }
    let paragraph_expression = query_analysis.search_query.map(|paragraph_expression| {
        bool_to_filter(paragraph_expression, |facet| FilterExpression {
            expr: Some(Expr::Facet(FacetFilter {
                facet,
            })),
        })
    });

    let field_expression = if field_filters.is_empty() {
        None
    } else if field_filters.len() == 1 {
        Some(field_filters.pop().unwrap())
    } else {
        Some(FilterExpression {
            expr: Some(Expr::And(FilterExpressionList {
                expr: field_filters,
            })),
        })
    };

    Ok((field_expression, paragraph_expression))
}

pub fn bool_to_filter(bool: BooleanExpression, map_literal: fn(String) -> FilterExpression) -> FilterExpression {
    match bool {
        BooleanExpression::Literal(l) => map_literal(l),
        BooleanExpression::Not(not) => FilterExpression {
            expr: Some(Expr::Not(Box::new(bool_to_filter(*not, map_literal)))),
        },
        BooleanExpression::Operation(BooleanOperation {
            operator: Operator::And,
            operands,
        }) => FilterExpression {
            expr: Some(Expr::And(FilterExpressionList {
                expr: operands.into_iter().map(|a| bool_to_filter(a, map_literal)).collect(),
            })),
        },
        BooleanExpression::Operation(BooleanOperation {
            operator: Operator::Or,
            operands,
        }) => FilterExpression {
            expr: Some(Expr::Or(FilterExpressionList {
                expr: operands.into_iter().map(|a| bool_to_filter(a, map_literal)).collect(),
            })),
        },
    }
}
