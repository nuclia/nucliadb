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

use nucliadb_core::prelude::*;
use nucliadb_core::protos::{DocumentSearchRequest, Faceted, Filter};
use nucliadb_texts::reader::TextReaderService;

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
            field_labels: vec!["/l/mylabel".to_string()],
            ..Default::default()
        },
        1,
    );
    query(
        &reader,
        "",
        Filter {
            field_labels: vec!["/e/myentity".to_string()],
            ..Default::default()
        },
        1,
    );
    query(
        &reader,
        "",
        Filter {
            field_labels: vec!["/l/fakelabel".to_string()],
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

// TODO: order, timestamp filter, only_faceted, with_status
