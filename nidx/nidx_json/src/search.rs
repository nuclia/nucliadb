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

use std::ops::Bound;

use tantivy::Term;
use tantivy::query::{AllQuery, BooleanQuery, FastFieldRangeQuery, Occur, Query, TermQuery};
use tantivy::schema::{Field, IndexRecordOption};

#[derive(Clone)]
pub struct JsonPathFilter {
    pub field_id: String,
    pub json_path: String,
    pub predicate: JsonPredicate,
}

#[derive(Clone)]
pub enum JsonPredicate {
    Text(String),

    IntRange {
        lower: Option<i64>,
        upper: Option<i64>,
    },
    FloatRange {
        lower: Option<f64>,
        upper: Option<f64>,
    },
    DateRange {
        lower: Option<tantivy::DateTime>,
        upper: Option<tantivy::DateTime>,
    },
    Boolean(bool),
}

#[derive(Clone)]
pub enum JsonFilterExpression {
    And(Vec<JsonFilterExpression>),
    Or(Vec<JsonFilterExpression>),
    Not(Box<JsonFilterExpression>),
    Path(JsonPathFilter),
}

#[derive(Clone)]
pub struct JsonSearchRequest {
    pub filter: JsonFilterExpression,
}

fn tantivy_path(filter: &JsonPathFilter) -> String {
    format!("{}.{}", filter.field_id, filter.json_path)
}

/// Converts a single `JsonPathFilter` into a Tantivy `Query`.
fn build_leaf_query(filter: &JsonPathFilter, json_field: Field) -> Box<dyn Query> {
    let path = tantivy_path(filter);

    match &filter.predicate {
        JsonPredicate::Text(val) => {
            // Use the fast field to do exact match
            let mut term = Term::from_field_json_path(json_field, &path, true);
            term.append_type_and_str(val);
            Box::new(FastFieldRangeQuery::new(
                Bound::Included(term.clone()),
                Bound::Included(term),
            ))
        }

        JsonPredicate::IntRange { lower, upper } => {
            let build_bound = |opt: &Option<i64>| -> Bound<Term> {
                match opt {
                    None => Bound::Unbounded,
                    Some(v) => {
                        let mut term = Term::from_field_json_path(json_field, &path, false);
                        term.append_type_and_fast_value(*v);
                        Bound::Included(term)
                    }
                }
            };
            Box::new(FastFieldRangeQuery::new(build_bound(lower), build_bound(upper)))
        }

        JsonPredicate::FloatRange { lower, upper } => {
            let build_bound = |opt: &Option<f64>| -> Bound<Term> {
                match opt {
                    None => Bound::Unbounded,
                    Some(v) => {
                        let mut term = Term::from_field_json_path(json_field, &path, false);
                        term.append_type_and_fast_value(*v);
                        Bound::Included(term)
                    }
                }
            };
            Box::new(FastFieldRangeQuery::new(build_bound(lower), build_bound(upper)))
        }

        JsonPredicate::Boolean(val) => {
            let mut term = Term::from_field_json_path(json_field, &path, false);
            term.append_type_and_fast_value(*val);
            Box::new(TermQuery::new(term, IndexRecordOption::Basic))
        }

        JsonPredicate::DateRange { lower, upper } => {
            let build_bound = |opt: &Option<tantivy::DateTime>| -> Bound<Term> {
                match opt {
                    None => Bound::Unbounded,
                    Some(v) => {
                        let mut term = Term::from_field_json_path(json_field, &path, false);
                        term.append_type_and_fast_value(*v);
                        Bound::Included(term)
                    }
                }
            };
            Box::new(FastFieldRangeQuery::new(build_bound(lower), build_bound(upper)))
        }
    }
}
pub(crate) fn build_tantivy_query(expr: &JsonFilterExpression, json_field: Field) -> Box<dyn Query> {
    match expr {
        JsonFilterExpression::Path(filter) => build_leaf_query(filter, json_field),

        JsonFilterExpression::And(children) => {
            let clauses: Vec<(Occur, Box<dyn Query>)> = children
                .iter()
                .map(|child| (Occur::Must, build_tantivy_query(child, json_field)))
                .collect();
            Box::new(BooleanQuery::new(clauses))
        }

        JsonFilterExpression::Or(children) => {
            let clauses: Vec<(Occur, Box<dyn Query>)> = children
                .iter()
                .map(|child| (Occur::Should, build_tantivy_query(child, json_field)))
                .collect();
            Box::new(BooleanQuery::new(clauses))
        }

        JsonFilterExpression::Not(inner) => {
            // Tantivy requires at least one positive clause in a boolean query.
            // Wrap the negated sub-query with an AllQuery positive clause.
            let clauses: Vec<(Occur, Box<dyn Query>)> = vec![
                (Occur::Must, Box::new(AllQuery)),
                (Occur::MustNot, build_tantivy_query(inner, json_field)),
            ];
            Box::new(BooleanQuery::new(clauses))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use tantivy::schema::OwnedValue;
    use tantivy::{Index, IndexWriter, TantivyDocument};
    use uuid::Uuid;

    use super::*;
    use crate::reader::JsonReaderService;
    use crate::schema::{JsonSchema, encode_rid};

    fn add_doc(
        writer: &mut IndexWriter,
        schema: &JsonSchema,
        uuid: Uuid,
        field_id: &str,
        json_obj: Vec<(String, OwnedValue)>,
    ) {
        let encoded = encode_rid(uuid);
        let mut doc = TantivyDocument::default();
        doc.add_bytes(schema.rid, uuid.to_string().as_bytes());
        for word in encoded.iter().copied() {
            doc.add_u64(schema.encoded_rid, word);
        }
        // Nest the JSON object under the field_id key, matching resource_indexer.rs.
        doc.add_field_value(
            schema.json,
            &OwnedValue::Object(vec![(field_id.to_string(), OwnedValue::Object(json_obj))]),
        );
        writer.add_document(doc).expect("add_document failed");
    }

    fn build_test_index() -> (JsonReaderService, Uuid, Uuid, Uuid) {
        let schema = JsonSchema::new();
        let index = Index::create_in_ram(schema.schema.clone());
        let mut writer: IndexWriter = index.writer(15_000_000).expect("writer failed");

        let apple_id = Uuid::parse_str("00000000000000000000000000000001").unwrap();
        let banana_id = Uuid::parse_str("00000000000000000000000000000002").unwrap();
        let cherry_id = Uuid::parse_str("00000000000000000000000000000003").unwrap();

        // apple: name="red apple", price=150, score=4.5, available=true
        add_doc(
            &mut writer,
            &schema,
            apple_id,
            "t/product",
            vec![
                ("name".to_string(), OwnedValue::Str("red apple".to_string())),
                ("price".to_string(), OwnedValue::I64(150)),
                ("score".to_string(), OwnedValue::F64(4.5)),
                ("available".to_string(), OwnedValue::Bool(true)),
            ],
        );

        // banana: name="green banana", price=80, score=3.2, available=false
        add_doc(
            &mut writer,
            &schema,
            banana_id,
            "t/product",
            vec![
                ("name".to_string(), OwnedValue::Str("green banana".to_string())),
                ("price".to_string(), OwnedValue::I64(80)),
                ("score".to_string(), OwnedValue::F64(3.2)),
                ("available".to_string(), OwnedValue::Bool(false)),
            ],
        );

        // cherry: name="red cherry", price=200, score=4.8, available=true
        add_doc(
            &mut writer,
            &schema,
            cherry_id,
            "t/product",
            vec![
                ("name".to_string(), OwnedValue::Str("red cherry".to_string())),
                ("price".to_string(), OwnedValue::I64(200)),
                ("score".to_string(), OwnedValue::F64(4.8)),
                ("available".to_string(), OwnedValue::Bool(true)),
            ],
        );

        writer.commit().expect("commit failed");

        let reader = index
            .reader_builder()
            .reload_policy(tantivy::ReloadPolicy::Manual)
            .try_into()
            .expect("reader failed");

        let svc = JsonReaderService { index, schema, reader };
        (svc, apple_id, banana_id, cherry_id)
    }

    fn search(svc: &JsonReaderService, expr: JsonFilterExpression) -> HashSet<Uuid> {
        let query = build_tantivy_query(&expr, svc.schema.json);
        svc.search(&*query).expect("search failed").into_iter().collect()
    }

    fn path(field_id: &str, json_path: &str, predicate: JsonPredicate) -> JsonFilterExpression {
        JsonFilterExpression::Path(JsonPathFilter {
            field_id: field_id.to_string(),
            json_path: json_path.to_string(),
            predicate,
        })
    }

    #[test]
    fn test_exact_match() {
        let (svc, apple, _banana, _cherry) = build_test_index();
        // Full stored value matches.
        let results = search(
            &svc,
            path("t/product", "name", JsonPredicate::Text("red apple".to_string())),
        );
        assert!(results.contains(&apple));
        // Partial token no longer matches (fast-field exact match, not token lookup).
        let results = search(
            &svc,
            path("t/product", "name", JsonPredicate::Text("apple".to_string())),
        );
        assert!(!results.contains(&apple));
    }

    #[test]
    fn test_int_range() {
        let (svc, apple, banana, cherry) = build_test_index();
        let results = search(
            &svc,
            path(
                "t/product",
                "price",
                JsonPredicate::IntRange {
                    lower: Some(80),
                    upper: Some(150),
                },
            ),
        );
        assert!(results.contains(&apple));
        assert!(results.contains(&banana));
        assert!(!results.contains(&cherry));
    }

    #[test]
    fn test_int_range_unbounded_upper() {
        let (svc, apple, _banana, cherry) = build_test_index();
        let results = search(
            &svc,
            path(
                "t/product",
                "price",
                JsonPredicate::IntRange {
                    lower: Some(150),
                    upper: None,
                },
            ),
        );
        assert!(results.contains(&apple));
        assert!(results.contains(&cherry));
    }

    #[test]
    fn test_float_range() {
        let (svc, apple, _banana, cherry) = build_test_index();
        let results = search(
            &svc,
            path(
                "t/product",
                "score",
                JsonPredicate::FloatRange {
                    lower: Some(4.0),
                    upper: Some(5.0),
                },
            ),
        );
        assert!(results.contains(&apple));
        assert!(results.contains(&cherry));
    }

    #[test]
    fn test_bool_match_true() {
        let (svc, apple, _banana, cherry) = build_test_index();
        let results = search(&svc, path("t/product", "available", JsonPredicate::Boolean(true)));
        assert!(results.contains(&apple));
        assert!(results.contains(&cherry));
    }

    #[test]
    fn test_bool_match_false() {
        let (svc, _apple, banana, _cherry) = build_test_index();
        let results = search(&svc, path("t/product", "available", JsonPredicate::Boolean(false)));
        assert_eq!(results, HashSet::from([banana]));
    }

    #[test]
    fn test_and_combination() {
        let (svc, apple, _banana, _cherry) = build_test_index();
        let expr = JsonFilterExpression::And(vec![
            path("t/product", "available", JsonPredicate::Boolean(true)),
            path(
                "t/product",
                "price",
                JsonPredicate::IntRange {
                    lower: None,
                    upper: Some(150),
                },
            ),
        ]);
        let results = search(&svc, expr);
        assert_eq!(results, HashSet::from([apple]));
    }

    #[test]
    fn test_or_combination() {
        let (svc, _apple, banana, _cherry) = build_test_index();
        let expr = JsonFilterExpression::Or(vec![path("t/product", "available", JsonPredicate::Boolean(false))]);
        let results = search(&svc, expr);
        assert!(results.contains(&banana));
    }

    #[test]
    fn test_not_combination() {
        let (svc, apple, _banana, cherry) = build_test_index();
        let expr = JsonFilterExpression::Not(Box::new(path("t/product", "available", JsonPredicate::Boolean(false))));
        let results = search(&svc, expr);
        assert!(results.contains(&apple));
        assert!(results.contains(&cherry));
        assert!(!results.contains(&_banana));
    }

    #[test]
    fn test_nested_and_or() {
        let (svc, apple, banana, cherry) = build_test_index();
        let expr = JsonFilterExpression::Or(vec![
            JsonFilterExpression::And(vec![
                path("t/product", "available", JsonPredicate::Boolean(true)),
                path(
                    "t/product",
                    "price",
                    JsonPredicate::IntRange {
                        lower: None,
                        upper: Some(150),
                    },
                ),
            ]),
            path("t/product", "available", JsonPredicate::Boolean(false)),
        ]);
        let results = search(&svc, expr);
        assert!(results.contains(&apple));
        assert!(results.contains(&banana));
        assert!(!results.contains(&cherry));
    }

    fn build_date_index() -> (JsonReaderService, Uuid, Uuid, Uuid) {
        let schema = JsonSchema::new();
        let index = Index::create_in_ram(schema.schema.clone());
        let mut writer: IndexWriter = index.writer(15_000_000).expect("writer failed");

        let old_id = Uuid::parse_str("00000000000000000000000000000011").unwrap();
        let mid_id = Uuid::parse_str("00000000000000000000000000000012").unwrap();
        let new_id = Uuid::parse_str("00000000000000000000000000000013").unwrap();

        let dt = |secs: i64| OwnedValue::Date(tantivy::DateTime::from_timestamp_secs(secs));

        // old: 2020-01-01 00:00:00 UTC  (1577836800)
        add_doc(
            &mut writer,
            &schema,
            old_id,
            "t/event",
            vec![("ts".to_string(), dt(1577836800))],
        );
        // mid: 2022-06-15 00:00:00 UTC  (1655251200)
        add_doc(
            &mut writer,
            &schema,
            mid_id,
            "t/event",
            vec![("ts".to_string(), dt(1655251200))],
        );
        // new: 2024-01-01 00:00:00 UTC  (1704067200)
        add_doc(
            &mut writer,
            &schema,
            new_id,
            "t/event",
            vec![("ts".to_string(), dt(1704067200))],
        );

        writer.commit().expect("commit failed");
        let reader = index
            .reader_builder()
            .reload_policy(tantivy::ReloadPolicy::Manual)
            .try_into()
            .expect("reader failed");
        (JsonReaderService { index, schema, reader }, old_id, mid_id, new_id)
    }

    #[test]
    fn test_exact_match_text_field() {
        // Verifies that JsonPredicate::Text does a true exact match against the
        // fast-field (columnar) value — no tokenization, case-sensitive.
        let schema = JsonSchema::new();
        let index = Index::create_in_ram(schema.schema.clone());
        let mut writer: IndexWriter = index.writer(15_000_000).expect("writer failed");

        let id = Uuid::parse_str("00000000000000000000000000000099").unwrap();
        add_doc(
            &mut writer,
            &schema,
            id,
            "k/product",
            vec![("color".to_string(), OwnedValue::Str("Red Apple".to_string()))],
        );
        writer.commit().expect("commit failed");
        let reader = index
            .reader_builder()
            .reload_policy(tantivy::ReloadPolicy::Manual)
            .try_into()
            .expect("reader failed");
        let svc = JsonReaderService { index, schema, reader };

        // Exact full value matches.
        let results = search(
            &svc,
            path("k/product", "color", JsonPredicate::Text("Red Apple".to_string())),
        );
        assert!(results.contains(&id), "exact full value should match");

        // Partial token does NOT match.
        let results = search(&svc, path("k/product", "color", JsonPredicate::Text("red".to_string())));
        assert!(!results.contains(&id), "partial/lowercased token should not match");

        // Wrong case does NOT match (fast-field match is case-sensitive).
        let results = search(
            &svc,
            path("k/product", "color", JsonPredicate::Text("red apple".to_string())),
        );
        assert!(!results.contains(&id), "wrong case should not match");
    }

    #[test]
    fn test_date_range_bounded() {
        let (svc, _old, mid, _new) = build_date_index();
        // [2021-01-01 .. 2023-01-01]
        let results = search(
            &svc,
            path(
                "t/event",
                "ts",
                JsonPredicate::DateRange {
                    lower: Some(tantivy::DateTime::from_timestamp_secs(1609459200)), // 2021
                    upper: Some(tantivy::DateTime::from_timestamp_secs(1672531200)), // 2023
                },
            ),
        );
        assert_eq!(results, HashSet::from([mid]));
    }

    #[test]
    fn test_date_range_unbounded_upper() {
        let (svc, _old, mid, new) = build_date_index();
        // [2022-01-01 .. ]
        let results = search(
            &svc,
            path(
                "t/event",
                "ts",
                JsonPredicate::DateRange {
                    lower: Some(tantivy::DateTime::from_timestamp_secs(1640995200)), // 2022
                    upper: None,
                },
            ),
        );
        assert!(results.contains(&mid));
        assert!(results.contains(&new));
        assert!(!results.contains(&_old));
    }

    #[test]
    fn test_date_range_unbounded_lower() {
        let (svc, old, _mid, _new) = build_date_index();
        // [ .. 2021-01-01]
        let results = search(
            &svc,
            path(
                "t/event",
                "ts",
                JsonPredicate::DateRange {
                    lower: None,
                    upper: Some(tantivy::DateTime::from_timestamp_secs(1609459200)), // 2021
                },
            ),
        );
        assert_eq!(results, HashSet::from([old]));
    }
}
