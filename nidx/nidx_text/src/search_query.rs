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

use crate::DocumentSearchRequest;
use crate::query_io::translate_keyword_to_text_query;
use nidx_protos::filter_expression::FieldFilter;
use nidx_protos::filter_expression::date_range_filter::DateField;
use nidx_protos::prost_types::Timestamp as ProstTimestamp;
use nidx_protos::stream_filter::Conjunction;
use nidx_protos::{FilterExpression, StreamFilter, StreamRequest};
use std::ops::Bound;
use tantivy::Term;
use tantivy::query::*;
use tantivy::schema::{Facet, Field, IndexRecordOption};

use crate::schema::{self, TextSchema};

pub fn produce_date_range_query(
    field: Field,
    from: Option<&ProstTimestamp>,
    to: Option<&ProstTimestamp>,
) -> Option<RangeQuery> {
    if from.is_none() && to.is_none() {
        return None;
    }

    let left_date_time = from.map(schema::timestamp_to_datetime_utc);
    let right_date_time = to.map(schema::timestamp_to_datetime_utc);
    let left_bound = left_date_time
        .map(|d| Bound::Included(Term::from_field_date(field, d)))
        .unwrap_or(Bound::Unbounded);
    let right_bound = right_date_time
        .map(|d| Bound::Included(Term::from_field_date(field, d)))
        .unwrap_or(Bound::Unbounded);
    let query = RangeQuery::new(left_bound, right_bound);
    Some(query)
}

pub fn create_streaming_query(schema: &TextSchema, request: &StreamRequest) -> Box<dyn Query> {
    let mut queries: Vec<(Occur, Box<dyn Query>)> = vec![];
    queries.push((Occur::Must, Box::new(AllQuery)));

    if let Some(ref filter) = request.filter {
        queries.extend(create_stream_filter_queries(schema, filter))
    } else if let Some(ref filter_expression) = request.filter_expression {
        queries.push((Occur::Must, filter_to_query(schema, filter_expression)))
    }
    Box::new(BooleanQuery::new(queries))
}

pub fn create_query(
    parser: &QueryParser,
    search: &DocumentSearchRequest,
    schema: &TextSchema,
    text: &str,
    with_advance: Option<Box<dyn Query>>,
) -> anyhow::Result<Box<dyn Query>> {
    let mut queries = vec![];
    let main_q = if text.is_empty() {
        Box::new(AllQuery)
    } else {
        parser.parse_query(text).unwrap_or_else(|_| Box::new(AllQuery))
    };
    queries.push((Occur::Must, main_q));

    if let Some(filter_expression) = &search.filter_expression {
        queries.push((Occur::Must, filter_to_query(schema, filter_expression)));
    }

    // Advance query
    if let Some(query) = with_advance {
        queries.push((Occur::Must, query));
    }

    if queries.len() == 1 && queries[0].1.is::<AllQuery>() {
        Ok(queries.pop().unwrap().1)
    } else {
        Ok(Box::new(BooleanQuery::new(queries)))
    }
}

fn create_stream_filter_queries(schema: &TextSchema, filter: &StreamFilter) -> Vec<(Occur, Box<dyn Query>)> {
    let mut queries = vec![];

    let conjunction = Conjunction::try_from(filter.conjunction)
        .unwrap_or(Conjunction::And)
        .into_occur();

    filter
        .labels
        .iter()
        .flat_map(|facet_key| Facet::from_text(facet_key).ok().into_iter())
        .for_each(|facet| {
            let facet_term = Term::from_facet(schema.facets, &facet);
            let facet_term_query: Box<dyn Query> = Box::new(TermQuery::new(facet_term, IndexRecordOption::Basic));
            queries.push((conjunction, facet_term_query))
        });

    queries
}

fn field_key(field: &FieldFilter) -> String {
    if let Some(field_name) = &field.field_id {
        format!("/{}/{}", field.field_type, field_name)
    } else {
        format!("/{}", field.field_type.clone())
    }
}

pub fn filter_to_query(schema: &TextSchema, expr: &FilterExpression) -> Box<dyn Query> {
    let filter_to_query = |expr| filter_to_query(schema, expr);
    match expr.expr.as_ref().unwrap() {
        nidx_protos::filter_expression::Expr::BoolAnd(e) => Box::new(BooleanQuery::intersection(
            e.operands.iter().map(filter_to_query).collect(),
        )),
        nidx_protos::filter_expression::Expr::BoolOr(e) => {
            Box::new(BooleanQuery::union(e.operands.iter().map(filter_to_query).collect()))
        }
        nidx_protos::filter_expression::Expr::BoolNot(e) => Box::new(BooleanQuery::new(vec![
            (Occur::Must, Box::new(AllQuery)),
            (Occur::MustNot, filter_to_query(e)),
        ])),
        nidx_protos::filter_expression::Expr::Resource(resource_filter) => {
            let key = resource_filter.resource_id.clone();
            Box::new(TermQuery::new(
                Term::from_field_bytes(schema.uuid, key.as_bytes()),
                IndexRecordOption::Basic,
            ))
        }
        nidx_protos::filter_expression::Expr::Field(field_filter) => Box::new(TermQuery::new(
            Term::from_facet(schema.field, &Facet::from(&field_key(field_filter))),
            IndexRecordOption::Basic,
        )),
        nidx_protos::filter_expression::Expr::Keyword(keyword_filter) => {
            translate_keyword_to_text_query(&keyword_filter.keyword, schema)
        }
        nidx_protos::filter_expression::Expr::Date(range) => {
            let field = match range.field() {
                DateField::Created => schema.created,
                DateField::Modified => schema.modified,
            };
            let maybe_query = produce_date_range_query(field, range.since.as_ref(), range.until.as_ref());
            if let Some(query) = maybe_query {
                Box::new(query)
            } else {
                Box::new(AllQuery)
            }
        }
        nidx_protos::filter_expression::Expr::Facet(facet_filter) => Box::new(TermQuery::new(
            Term::from_facet(schema.facets, &Facet::from(&facet_filter.facet)),
            IndexRecordOption::Basic,
        )),
    }
}

trait IntoOccur {
    fn into_occur(self) -> Occur;
}

impl IntoOccur for Conjunction {
    fn into_occur(self) -> Occur {
        match self {
            Conjunction::And => Occur::Must,
            Conjunction::Or => Occur::Should,
            Conjunction::Not => Occur::MustNot,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stream_filter_query_per_tag() {
        let schema = TextSchema::new(3);

        let filter = StreamFilter::default();
        let queries = create_stream_filter_queries(&schema, &filter);
        assert!(queries.is_empty());

        let filter = StreamFilter {
            labels: vec!["/A".to_string(); 10],
            ..Default::default()
        };
        let queries = create_stream_filter_queries(&schema, &filter);
        assert_eq!(queries.len(), 10);
    }

    #[test]
    fn test_default_stream_filter_queries_creation() {
        let schema = TextSchema::new(3);
        let filter = StreamFilter {
            labels: vec!["/A".to_string(), "/B".to_string()],
            ..Default::default()
        };

        let queries = create_stream_filter_queries(&schema, &filter);
        assert_eq!(queries.len(), 2);
        for (occur, _query) in queries.iter() {
            assert_eq!(*occur, Occur::Must);
        }
    }

    #[test]
    fn test_and_stream_filter_queries_creation() {
        let schema = TextSchema::new(3);
        let filter = StreamFilter {
            labels: vec!["/A".to_string(), "/B".to_string()],
            conjunction: Conjunction::And.into(),
        };

        let queries = create_stream_filter_queries(&schema, &filter);
        assert_eq!(queries.len(), 2);
        for (occur, _query) in queries.iter() {
            assert_eq!(*occur, Occur::Must);
        }
    }

    #[test]
    fn test_or_stream_filter_queries_creation() {
        let schema = TextSchema::new(3);
        let filter = StreamFilter {
            labels: vec!["/A".to_string(), "/B".to_string()],
            conjunction: Conjunction::Or.into(),
        };

        let queries = create_stream_filter_queries(&schema, &filter);
        assert_eq!(queries.len(), 2);
        for (occur, _query) in queries.iter() {
            assert_eq!(*occur, Occur::Should);
        }
    }

    #[test]
    fn test_not_stream_filter_queries_creation() {
        let schema = TextSchema::new(3);
        let filter = StreamFilter {
            labels: vec!["/A".to_string(), "/B".to_string()],
            conjunction: Conjunction::Not.into(),
        };

        let queries = create_stream_filter_queries(&schema, &filter);
        assert_eq!(queries.len(), 2);
        for (occur, _query) in queries.iter() {
            assert_eq!(*occur, Occur::MustNot);
        }
    }
}
