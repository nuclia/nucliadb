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
// use std::convert::TryFrom;
// use std::time::SystemTime;

use nucliadb_core::protos::stream_filter::Conjunction;
use nucliadb_core::protos::{DocumentSearchRequest, StreamFilter, StreamRequest};
use tantivy::query::*;
use tantivy::schema::{Facet, IndexRecordOption};
use tantivy::Term;

use crate::schema::TextSchema;

pub fn create_streaming_query(schema: &TextSchema, request: &StreamRequest) -> Box<dyn Query> {
    let mut queries: Vec<(Occur, Box<dyn Query>)> = vec![];
    queries.push((Occur::Must, Box::new(AllQuery)));

    if let Some(ref filter) = request.filter {
        queries.extend(create_stream_filter_queries(schema, filter))
    }

    Box::new(BooleanQuery::new(queries))
}

pub fn create_query(
    parser: &QueryParser,
    search: &DocumentSearchRequest,
    schema: &TextSchema,
    text: &str,
    with_advance: Option<Box<dyn Query>>,
) -> Box<dyn Query> {
    let mut queries = vec![];
    let main_q = if text.is_empty() {
        Box::new(AllQuery)
    } else {
        parser
            .parse_query(text)
            .unwrap_or_else(|_| Box::new(AllQuery))
    };
    queries.push((Occur::Must, main_q));

    // Fields
    search
        .fields
        .iter()
        .map(|value| format!("/{}", value))
        .flat_map(|facet_key| Facet::from_text(facet_key.as_str()).ok().into_iter())
        .for_each(|facet| {
            let facet_term = Term::from_facet(schema.field, &facet);
            let facet_term_query = TermQuery::new(facet_term, IndexRecordOption::Basic);
            queries.push((Occur::Must, Box::new(facet_term_query)));
        });

    // Add filter
    search
        .filter
        .iter()
        .flat_map(|f| f.tags.iter())
        .flat_map(|facet_key| Facet::from_text(facet_key).ok().into_iter())
        .for_each(|facet| {
            let facet_term = Term::from_facet(schema.facets, &facet);
            let facet_term_query = TermQuery::new(facet_term, IndexRecordOption::Basic);
            queries.push((Occur::Must, Box::new(facet_term_query)));
        });

    // Status filters
    if let Some(status) = search.with_status.map(|status| status as u64) {
        let term = Term::from_field_u64(schema.status, status);
        let term_query = TermQuery::new(term, IndexRecordOption::Basic);
        queries.push((Occur::Must, Box::new(term_query)));
    };

    // Advance query
    if let Some(query) = with_advance {
        queries.push((Occur::Must, query));
    }

    if queries.len() == 1 && queries[0].1.is::<AllQuery>() {
        queries.pop().unwrap().1
    } else {
        Box::new(BooleanQuery::new(queries))
    }
}

fn create_stream_filter_queries(
    schema: &TextSchema,
    filter: &StreamFilter,
) -> Vec<(Occur, Box<dyn Query>)> {
    let mut queries = vec![];

    let conjunction = Conjunction::from_i32(filter.conjunction)
        .unwrap_or(Conjunction::And)
        .into_occur();

    filter
        .tags
        .iter()
        .flat_map(|facet_key| Facet::from_text(facet_key).ok().into_iter())
        .for_each(|facet| {
            let facet_term = Term::from_facet(schema.facets, &facet);
            let facet_term_query: Box<dyn Query> =
                Box::new(TermQuery::new(facet_term, IndexRecordOption::Basic));
            queries.push((conjunction, facet_term_query))
        });

    queries
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
#[allow(non_snake_case)]
mod tests {
    use super::*;

    #[test]
    fn test_stream_filter_query_per_tag() {
        let schema = TextSchema::new();

        let filter = StreamFilter::default();
        let queries = create_stream_filter_queries(&schema, &filter);
        assert!(queries.is_empty());

        let filter = StreamFilter {
            tags: vec!["/A".to_string(); 10],
            ..Default::default()
        };
        let queries = create_stream_filter_queries(&schema, &filter);
        assert_eq!(queries.len(), 10);
    }

    #[test]
    fn test_default_stream_filter_queries_creation() {
        let schema = TextSchema::new();
        let filter = StreamFilter {
            tags: vec!["/A".to_string(), "/B".to_string()],
            ..Default::default()
        };

        let queries = create_stream_filter_queries(&schema, &filter);
        assert_eq!(queries.len(), 2);
        for (occur, _query) in queries.iter() {
            assert_eq!(*occur, Occur::Must);
        }
    }

    #[test]
    fn test_AND_stream_filter_queries_creation() {
        let schema = TextSchema::new();
        let filter = StreamFilter {
            tags: vec!["/A".to_string(), "/B".to_string()],
            conjunction: Conjunction::And.into(),
        };

        let queries = create_stream_filter_queries(&schema, &filter);
        assert_eq!(queries.len(), 2);
        for (occur, _query) in queries.iter() {
            assert_eq!(*occur, Occur::Must);
        }
    }

    #[test]
    fn test_OR_stream_filter_queries_creation() {
        let schema = TextSchema::new();
        let filter = StreamFilter {
            tags: vec!["/A".to_string(), "/B".to_string()],
            conjunction: Conjunction::Or.into(),
        };

        let queries = create_stream_filter_queries(&schema, &filter);
        assert_eq!(queries.len(), 2);
        for (occur, _query) in queries.iter() {
            assert_eq!(*occur, Occur::Should);
        }
    }

    #[test]
    fn test_NOT_stream_filter_queries_creation() {
        let schema = TextSchema::new();
        let filter = StreamFilter {
            tags: vec!["/A".to_string(), "/B".to_string()],
            conjunction: Conjunction::Not.into(),
        };

        let queries = create_stream_filter_queries(&schema, &filter);
        assert_eq!(queries.len(), 2);
        for (occur, _query) in queries.iter() {
            assert_eq!(*occur, Occur::MustNot);
        }
    }
}
