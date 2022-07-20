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
use nucliadb_protos::ParagraphSearchRequest;
use nucliadb_service_interface::prelude::*;
use tantivy::query::*;
use tantivy::schema::{Facet, IndexRecordOption};
use tantivy::Term;

use crate::schema::ParagraphSchema;

type QueryP = (Occur, Box<dyn Query>);
type NewFuzz = fn(Term, u8, bool) -> FuzzyTermQuery;

fn term_query_to_fuzzy(query: Box<dyn Query>, distance: u8, with: NewFuzz) -> Box<dyn Query> {
    let term_query: &TermQuery = query.downcast_ref().unwrap();
    let term = term_query.term().clone();
    Box::new(with(term, distance, true))
}

fn queryp_map(queries: Vec<QueryP>, distance: u8, as_prefix: usize) -> Vec<QueryP> {
    queries
        .into_iter()
        .enumerate()
        .map(|(id, (_, query))| {
            let query = if query.is::<TermQuery>() && id == as_prefix {
                term_query_to_fuzzy(query, distance, FuzzyTermQuery::new_prefix)
            } else if query.is::<TermQuery>() {
                term_query_to_fuzzy(query, distance, FuzzyTermQuery::new)
            } else {
                query
            };
            (Occur::Must, query)
        })
        .collect()
}

fn flat_bool_query(query: BooleanQuery, collector: (usize, Vec<QueryP>)) -> (usize, Vec<QueryP>) {
    query
        .clauses()
        .iter()
        .map(|(occur, subq)| (*occur, subq.box_clone()))
        .fold(collector, |(mut id, mut c), (occur, subq)| {
            if subq.is::<BooleanQuery>() {
                let subq: Box<BooleanQuery> = subq.downcast().unwrap();
                flat_bool_query(*subq, (id, c))
            } else if subq.is::<TermQuery>() {
                id = c.len();
                c.push((occur, subq));
                (id, c)
            } else {
                c.push((occur, subq));
                (id, c)
            }
        })
}

fn flat_and_adapt(query: Box<dyn Query>, distance: u8) -> Vec<QueryP> {
    let (queries, as_prefix) = if query.is::<BooleanQuery>() {
        let query: Box<BooleanQuery> = query.downcast().unwrap();
        let (as_prefix, queries) = flat_bool_query(*query, (usize::MAX, vec![]));
        (queries, as_prefix)
    } else if query.is::<TermQuery>() {
        let queries = vec![(Occur::Must, query)];
        let as_prefix = 0;
        (queries, as_prefix)
    } else {
        let queries = vec![(Occur::Must, query)];
        let as_prefix = 1;
        (queries, as_prefix)
    };
    queryp_map(queries, distance, as_prefix)
}

fn parse_query(parser: &QueryParser, text: &str, distance: u8) -> Vec<QueryP> {
    if text.is_empty() {
        vec![(Occur::Should, Box::new(AllQuery) as Box<dyn Query>)]
    } else {
        let query = parser.parse_query(text).unwrap();
        flat_and_adapt(query, distance)
    }
}

pub fn create_query(
    parser: &QueryParser,
    text: &str,
    search: &ParagraphSearchRequest,
    schema: &ParagraphSchema,
    distance: u8,
) -> Vec<QueryP> {
    let mut queries = parse_query(parser, text, distance);
    if !search.uuid.is_empty() {
        let term = Term::from_field_text(schema.uuid, &search.uuid);
        let term_query = TermQuery::new(term, IndexRecordOption::Basic);
        queries.push((Occur::Must, Box::new(term_query)))
    }

    // Fields
    search.fields.iter().for_each(|value| {
        let facet_key: String = format!("/{}", value);
        let facet = Facet::from(facet_key.as_str());
        let facet_term = Term::from_facet(schema.field, &facet);
        let facet_term_query = TermQuery::new(facet_term, IndexRecordOption::Basic);
        queries.push((Occur::Should, Box::new(facet_term_query)));
    });

    // Add filter
    search
        .filter
        .iter()
        .flat_map(|f| f.tags.iter())
        .for_each(|value| {
            let facet = Facet::from(value.as_str());
            let facet_term = Term::from_facet(schema.facets, &facet);
            let facet_term_query = TermQuery::new(facet_term, IndexRecordOption::Basic);
            queries.push((Occur::Must, Box::new(facet_term_query)));
        });
    queries
}

#[cfg(test)]
mod tests {
    use tantivy::schema::Field;

    use super::*;
    fn dummy_term_query() -> Box<dyn Query> {
        let field = Field::from_field_id(0);
        let term = Term::from_field_u64(field, 0);
        Box::new(TermQuery::new(term, IndexRecordOption::Basic))
    }
    #[test]
    fn test() {
        let subqueries0: Vec<_> = vec![dummy_term_query; 12]
            .into_iter()
            .map(|f| (Occur::Must, f()))
            .collect();
        let subqueries1: Vec<_> = vec![dummy_term_query; 12]
            .into_iter()
            .map(|f| (Occur::Must, f()))
            .collect();
        let boolean0: Box<dyn Query> = Box::new(BooleanQuery::new(subqueries0));
        let boolean1: Box<dyn Query> = Box::new(BooleanQuery::new(subqueries1));
        let nested = BooleanQuery::new(vec![(Occur::Should, boolean0), (Occur::Should, boolean1)]);
        let adapted = flat_and_adapt(Box::new(nested), 2);
        assert_eq!(adapted.len(), 24);
        assert!(adapted.iter().all(|(occur, _)| *occur == Occur::Must));
        assert!(adapted
            .iter()
            .all(|(_, query)| query.is::<FuzzyTermQuery>()));
    }
}
