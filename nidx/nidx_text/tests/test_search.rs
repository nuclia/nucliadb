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

mod common;

use std::time::SystemTime;

use nidx_protos::filter_expression::date_range_filter::DateField;
use nidx_protos::filter_expression::{
    DateRangeFilter, Expr, FacetFilter, FieldFilter, FilterExpressionList, KeywordFilter, ResourceFilter,
};
use nidx_protos::prost_types::Timestamp;
use nidx_protos::{Faceted, FilterExpression};
use nidx_protos::{OrderBy, order_by::OrderField, order_by::OrderType};
use nidx_text::TextSearcher;
use nidx_text::{DocumentSearchRequest, prefilter::*};
use nidx_types::prefilter::PrefilterResult;

#[test]
fn test_search_queries() {
    fn query(reader: &TextSearcher, query: impl Into<String>, expected: i32) {
        let query = query.into();
        let request = DocumentSearchRequest {
            id: "shard".to_string(),
            body: query.clone(),
            result_per_page: 20,
            ..Default::default()
        };

        let response = reader.search(&request).unwrap();
        assert_eq!(response.total, expected, "Failed query: '{}'", query);

        assert_eq!(response.total, response.results.len() as i32);
        assert!(!response.next_page);
    }

    let reader = common::test_reader();

    // empty query matches all
    query(&reader, "", 2);

    // exact text
    query(&reader, "enough to test", 1);

    // quoted - exact text
    query(&reader, "\"enough to test\"", 1);

    // exact words
    query(&reader, "enough test", 1);

    // quoted - exact words
    query(&reader, "\"enough test\"", 0);

    // unclosed quote
    query(&reader, "\"enough test", 0);

    // additional (non existent) words
    query(&reader, "enough mischievous test", 0);

    // additional (non existent) symbols
    query(&reader, "enough - test", 0);
}

#[test]
fn test_prefilter_all_search() {
    let reader = common::test_reader();
    let request = PreFilterRequest {
        security: None,
        filter_expression: None,
    };
    let response = reader.prefilter(&request).unwrap();
    assert!(matches!(response, PrefilterResult::All));
}

#[test]
fn test_prefilter_not_search() {
    let reader = common::test_reader();

    let request = PreFilterRequest {
        security: None,
        filter_expression: Some(FilterExpression {
            expr: Some(nidx_protos::filter_expression::Expr::BoolNot(Box::new(
                FilterExpression {
                    expr: Some(nidx_protos::filter_expression::Expr::Facet(FacetFilter {
                        facet: "/l/mylabel".into(),
                    })),
                },
            ))),
        }),
    };
    let response = reader.prefilter(&request).unwrap();
    let valid_fields = &response;
    let PrefilterResult::Some(fields) = valid_fields else {
        panic!("Response is not on the right variant {valid_fields:?}");
    };
    assert_eq!(fields.len(), 1);
}

#[test]
fn test_labels_prefilter_search() {
    let reader = common::test_reader();

    let request = PreFilterRequest {
        security: None,
        filter_expression: Some(FilterExpression {
            expr: Some(nidx_protos::filter_expression::Expr::Facet(FacetFilter {
                facet: "/l/mylabel".into(),
            })),
        }),
    };
    let response = reader.prefilter(&request).unwrap();
    let PrefilterResult::Some(fields) = response else {
        panic!("Response is not on the right variant");
    };
    assert_eq!(fields.len(), 1);
}

#[test]
fn test_keywords_prefilter_search() {
    let reader = common::test_reader();
    let request = PreFilterRequest {
        security: None,
        filter_expression: Some(FilterExpression {
            expr: Some(nidx_protos::filter_expression::Expr::BoolAnd(FilterExpressionList {
                operands: vec![
                    FilterExpression {
                        expr: Some(nidx_protos::filter_expression::Expr::Facet(FacetFilter {
                            facet: "/l/mylabel".into(),
                        })),
                    },
                    FilterExpression {
                        expr: Some(nidx_protos::filter_expression::Expr::Keyword(KeywordFilter {
                            keyword: "foobar".into(),
                        })),
                    },
                ],
            })),
        }),
    };
    let response = reader.prefilter(&request).unwrap();
    let PrefilterResult::None = response else {
        panic!("Response is not on the right variant");
    };

    let request = PreFilterRequest {
        security: None,
        filter_expression: Some(FilterExpression {
            expr: Some(nidx_protos::filter_expression::Expr::Facet(FacetFilter {
                facet: "/l/mylabel".into(),
            })),
        }),
    };
    let response = reader.prefilter(&request).unwrap();
    let PrefilterResult::Some(fields) = response else {
        panic!("Response is not on the right variant");
    };
    assert_eq!(fields.len(), 1);

    let request = PreFilterRequest {
        security: None,
        filter_expression: Some(FilterExpression {
            expr: Some(nidx_protos::filter_expression::Expr::Keyword(KeywordFilter {
                keyword: "%".into(),
            })),
        }),
    };
    let response = reader.prefilter(&request).unwrap();
    let PrefilterResult::None = response else {
        panic!("Response is not on the right variant");
    };
}

#[test]
fn test_filtered_search() {
    fn query(reader: &TextSearcher, query: impl Into<String>, expression: FilterExpression, expected: i32) {
        let query = query.into();
        let request = DocumentSearchRequest {
            id: "shard".to_string(),
            body: query.clone(),
            result_per_page: 20,
            filter_expression: Some(expression),
            ..Default::default()
        };

        let response = reader.search(&request).unwrap();
        assert_eq!(response.total, expected, "Failed query: '{}'", query);

        assert_eq!(response.total, response.results.len() as i32);
        assert!(!response.next_page);
    }

    let reader = common::test_reader();

    query(
        &reader,
        "",
        FilterExpression {
            expr: Some(nidx_protos::filter_expression::Expr::Facet(FacetFilter {
                facet: "/l/mylabel".to_string(),
            })),
        },
        1,
    );
    query(
        &reader,
        "",
        FilterExpression {
            expr: Some(nidx_protos::filter_expression::Expr::Facet(FacetFilter {
                facet: "/e/myentity".to_string(),
            })),
        },
        1,
    );
    query(
        &reader,
        "",
        FilterExpression {
            expr: Some(nidx_protos::filter_expression::Expr::Facet(FacetFilter {
                facet: "/l/fakelabel".to_string(),
            })),
        },
        0,
    );
}

#[test]
fn test_search_by_field() {
    let reader = common::test_reader();

    let request = DocumentSearchRequest {
        id: "shard".to_string(),
        body: "".to_string(),
        result_per_page: 20,
        filter_expression: Some(FilterExpression {
            expr: Some(nidx_protos::filter_expression::Expr::Field(FieldFilter {
                field_type: "a".into(),
                field_id: Some("title".into()),
            })),
        }),
        ..Default::default()
    };

    let response = reader.search(&request).unwrap();
    assert_eq!(response.total, response.results.len() as i32);
    assert_eq!(response.total, 1);
    assert!(!response.next_page);
}

#[test]
fn test_faceted_search() {
    fn query(reader: &TextSearcher, query: impl Into<String>, facets: Faceted, expected: i32) {
        let query = query.into();
        let request = DocumentSearchRequest {
            id: "shard".to_string(),
            body: "".to_string(),
            result_per_page: 20,
            faceted: Some(facets.clone()),
            ..Default::default()
        };
        let response = reader.search(&request).unwrap();
        println!("Response: {response:#?}");
        assert_eq!(
            response.total, expected,
            "Failed faceted query: '{}'. With facets: {:?}",
            query, facets
        );

        assert_eq!(response.total, response.results.len() as i32);
        assert!(!response.next_page);
    }

    let reader = common::test_reader();

    query(
        &reader,
        "",
        Faceted {
            labels: vec!["/l".to_string(), "/t".to_string()],
        },
        2,
    );
}

#[test]
fn test_quote_fixing() {
    fn query(reader: &TextSearcher, query: impl Into<String>) {
        let request = DocumentSearchRequest {
            id: "shard".to_string(),
            body: query.into(),
            result_per_page: 20,
            ..Default::default()
        };

        let response = reader.search(&request).unwrap();
        assert_eq!(response.query, "\"enough test\"");
    }

    let reader = common::test_reader();

    query(&reader, "\"enough test\"");
    query(&reader, "enough test\"");
    query(&reader, "\"enough test");
}

#[test]
fn test_search_with_min_score() {
    fn query(reader: &TextSearcher, query: impl Into<String>, min_score: f32, expected: i32) {
        let query = query.into();
        let request = DocumentSearchRequest {
            id: "shard".to_string(),
            body: query.clone(),
            result_per_page: 20,
            min_score,
            ..Default::default()
        };

        let response = reader.search(&request).unwrap();
        assert_eq!(response.results.len() as i32, expected, "Failed query: '{}'", query);
        assert!(!response.next_page);
    }

    let reader = common::test_reader();

    // Check that the min score is being used to filter the results
    query(&reader, "should", 0.0, 1);
    query(&reader, "should", 100.0, 0);
}

#[test]
fn test_int_order_pagination() {
    let reader = common::test_reader();

    let request = DocumentSearchRequest {
        id: "shard".to_string(),
        body: "".to_string(),
        result_per_page: 1,
        order: Some(OrderBy {
            r#type: OrderType::Desc.into(),
            sort_by: OrderField::Created.into(),
            ..Default::default()
        }),
        min_score: f32::MIN,
        ..Default::default()
    };

    let response = reader.search(&request).unwrap();
    assert_eq!(response.results.len(), 1);
    assert!(response.next_page);
}

#[test]
fn test_timestamp_filtering() {
    let reader = common::test_reader();

    let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap();
    let before = Timestamp {
        seconds: now.as_secs() as i64 - 100,
        nanos: 0,
    };
    let after = Timestamp {
        seconds: now.as_secs() as i64 + 100,
        nanos: 0,
    };

    let search = |date_range| {
        let request = DocumentSearchRequest {
            id: "shard".to_string(),
            body: "".to_string(),
            result_per_page: 10,
            min_score: f32::MIN,
            filter_expression: Some(FilterExpression {
                expr: Some(Expr::Date(date_range)),
            }),
            ..Default::default()
        };
        let response = reader.search(&request).unwrap();
        response.results.len()
    };

    for field in &[DateField::Created.into(), DateField::Modified.into()] {
        assert_eq!(
            search(DateRangeFilter {
                field: *field,
                since: Some(before),
                until: Some(after)
            }),
            2
        );

        assert_eq!(
            search(DateRangeFilter {
                field: *field,
                since: Some(after),
                until: None
            }),
            0
        );
    }
}

#[test]
fn test_key_filtering() {
    let reader = common::test_reader();

    let request = DocumentSearchRequest {
        id: "shard".to_string(),
        body: "".to_string(),
        result_per_page: 1,
        min_score: f32::MIN,
        ..Default::default()
    };
    let response = reader.search(&request).unwrap();
    let resource_id = &response.results[0].uuid;

    let search = |resource| {
        let request = DocumentSearchRequest {
            id: "shard".to_string(),
            body: "".to_string(),
            result_per_page: 10,
            min_score: f32::MIN,
            filter_expression: Some(FilterExpression {
                expr: Some(Expr::Resource(resource)),
            }),
            ..Default::default()
        };
        let response = reader.search(&request).unwrap();
        response.results.len()
    };

    // By resource
    assert_eq!(
        search(ResourceFilter {
            resource_id: resource_id.clone(),
        }),
        2
    );
    assert_eq!(
        search(ResourceFilter {
            resource_id: "fake".into(),
        }),
        0
    );
}

// TODO: order, only_faceted, with_status
