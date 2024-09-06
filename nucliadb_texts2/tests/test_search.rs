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
use nucliadb_core::protos::{order_by::OrderField, order_by::OrderType, OrderBy};
use nucliadb_core::protos::{DocumentSearchRequest, Faceted, Filter};
use nucliadb_core::query_planner::{PreFilterRequest, ValidFieldCollector};
use nucliadb_core::texts::*;
use nucliadb_texts2::reader::TextReaderService;
use std::collections::HashSet;

#[test]
fn test_search_queries() {
    fn query(reader: &TextReaderService, query: impl Into<String>, expected: i32) {
        let query = query.into();
        let request = DocumentSearchRequest {
            id: "shard".to_string(),
            body: query.clone(),
            page_number: 0,
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

    // partial words
    // TODO: uncomment after fixing sc-5626
    // query(&reader, "shou", 1);
}

#[test]
fn test_prefilter_all_search() {
    let reader = common::test_reader();
    let request = PreFilterRequest {
        security: None,
        labels_formula: None,
        timestamp_filters: vec![],
        keywords_formula: None,
    };
    let response = reader.prefilter(&request).unwrap();
    assert!(matches!(response.valid_fields, ValidFieldCollector::All));
}

#[test]
fn test_prefilter_not_search() {
    let reader = common::test_reader();

    let context = nucliadb_core::query_language::QueryContext {
        field_labels: HashSet::from(["/l/mylabel".to_string()]),
        paragraph_labels: HashSet::with_capacity(0),
    };
    let query = "{ \"not\": { \"literal\": \"/l/mylabel\" } }";
    let expression = nucliadb_core::query_language::translate(Some(query), None, &context).unwrap();
    let request = PreFilterRequest {
        security: None,
        timestamp_filters: vec![],
        labels_formula: expression.labels_prefilter_query,
        keywords_formula: expression.keywords_prefilter_query,
    };
    println!("expression: {:?}", request.labels_formula);
    let response = reader.prefilter(&request).unwrap();
    let valid_fields = &response.valid_fields;
    let ValidFieldCollector::Some(fields) = valid_fields else {
        panic!("Response is not on the right variant {valid_fields:?}");
    };
    assert_eq!(fields.len(), 1);
}

#[test]
fn test_labels_prefilter_search() {
    let reader = common::test_reader();

    let context = nucliadb_core::query_language::QueryContext {
        field_labels: HashSet::from(["/l/mylabel".to_string()]),
        paragraph_labels: HashSet::with_capacity(0),
    };
    let query = "{ \"literal\": \"/l/mylabel\" }";
    let expression = nucliadb_core::query_language::translate(Some(query), None, &context).unwrap();
    let request = PreFilterRequest {
        security: None,
        labels_formula: expression.labels_prefilter_query,
        timestamp_filters: vec![],
        keywords_formula: expression.keywords_prefilter_query,
    };
    let response = reader.prefilter(&request).unwrap();
    let ValidFieldCollector::Some(fields) = response.valid_fields else {
        panic!("Response is not on the right variant");
    };
    assert_eq!(fields.len(), 1);
}

#[test]
fn test_keywords_prefilter_search() {
    let reader = common::test_reader();
    let context = nucliadb_core::query_language::QueryContext {
        field_labels: HashSet::from(["/l/mylabel".to_string()]),
        paragraph_labels: HashSet::with_capacity(0),
    };
    let labels = "{ \"literal\": \"/l/mylabel\" }";
    let keywords = "{ \"literal\": \"foobar\" }";
    let expression = nucliadb_core::query_language::translate(Some(labels), Some(keywords), &context).unwrap();
    let request = PreFilterRequest {
        security: None,
        labels_formula: expression.labels_prefilter_query,
        timestamp_filters: vec![],
        keywords_formula: expression.keywords_prefilter_query,
    };
    let response = reader.prefilter(&request).unwrap();
    let ValidFieldCollector::None = response.valid_fields else {
        panic!("Response is not on the right variant");
    };

    let expression = nucliadb_core::query_language::translate(Some(labels), None, &context).unwrap();
    let request = PreFilterRequest {
        security: None,
        labels_formula: expression.labels_prefilter_query,
        timestamp_filters: vec![],
        keywords_formula: expression.keywords_prefilter_query,
    };
    let response = reader.prefilter(&request).unwrap();
    let ValidFieldCollector::Some(fields) = response.valid_fields else {
        panic!("Response is not on the right variant");
    };
    assert_eq!(fields.len(), 1);
}

#[test]
fn test_filtered_search() {
    fn query(reader: &TextReaderService, query: impl Into<String>, filter: Filter, expected: i32) {
        let query = query.into();
        let request = DocumentSearchRequest {
            id: "shard".to_string(),
            body: query.clone(),
            filter: Some(filter),
            page_number: 0,
            result_per_page: 20,
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
        Filter {
            paragraph_labels: vec![],
            field_labels: vec!["/l/mylabel".to_string()],
            labels_expression: "{ \"literal\": \"/l/mylabel\" }".to_string(),
            ..Default::default()
        },
        1,
    );
    query(
        &reader,
        "",
        Filter {
            paragraph_labels: vec![],
            field_labels: vec!["/e/myentity".to_string()],
            labels_expression: "{ \"literal\": \"/e/myentity\" }".to_string(),
            ..Default::default()
        },
        1,
    );
    query(
        &reader,
        "",
        Filter {
            paragraph_labels: vec![],
            field_labels: vec!["/l/fakelabel".to_string()],
            labels_expression: "{ \"literal\": \"/l/fakelabel\" }".to_string(),
            ..Default::default()
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
        page_number: 0,
        result_per_page: 20,
        fields: vec!["title".to_string()],
        ..Default::default()
    };

    let response = reader.search(&request).unwrap();
    assert_eq!(response.total, response.results.len() as i32);
    assert_eq!(response.total, 1);
    assert!(!response.next_page);
}

#[test]
fn test_faceted_search() {
    fn query(reader: &TextReaderService, query: impl Into<String>, facets: Faceted, expected: i32) {
        let query = query.into();
        let request = DocumentSearchRequest {
            id: "shard".to_string(),
            body: "".to_string(),
            page_number: 0,
            result_per_page: 20,
            faceted: Some(facets.clone()),
            ..Default::default()
        };
        let response = reader.search(&request).unwrap();
        println!("Response: {response:#?}");
        assert_eq!(response.total, expected, "Failed faceted query: '{}'. With facets: {:?}", query, facets);

        assert_eq!(response.total, response.results.len() as i32);
        assert!(!response.next_page);
    }

    let reader = common::test_reader();

    query(
        &reader,
        "",
        Faceted {
            labels: vec!["/".to_string(), "/l".to_string(), "/t".to_string()],
        },
        2,
    );
}

#[test]
fn test_quote_fixing() {
    fn query(reader: &TextReaderService, query: impl Into<String>) {
        let request = DocumentSearchRequest {
            id: "shard".to_string(),
            body: query.into(),
            page_number: 0,
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
    fn query(reader: &TextReaderService, query: impl Into<String>, min_score: f32, expected: i32) {
        let query = query.into();
        let request = DocumentSearchRequest {
            id: "shard".to_string(),
            body: query.clone(),
            page_number: 0,
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
        page_number: 0,
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

// TODO: order, timestamp filter, only_faceted, with_status
