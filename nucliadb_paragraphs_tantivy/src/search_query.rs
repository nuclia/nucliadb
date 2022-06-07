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
use tantivy::query::{AllQuery, FuzzyTermQuery, Occur, Query, QueryParser, TermQuery};
use tantivy::schema::{Facet, IndexRecordOption, Type};
use tantivy::Term;

use crate::schema::ParagraphSchema;

type QueryParams = (Occur, Box<dyn Query>);

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Distance {
    Low,
    High,
}

impl From<Distance> for u8 {
    fn from(elem: Distance) -> u8 {
        match elem {
            Distance::Low => 1,
            Distance::High => 2,
        }
    }
}

fn parse_query(
    parser: &QueryParser,
    text: &str,
    distance: Distance,
) -> Vec<(Occur, Box<dyn Query>)> {
    use std::collections::BTreeMap;

    let distance = distance.into();
    let mut collector = BTreeMap::new();
    let query = parser.parse_query(text).unwrap();
    query.query_terms(&mut collector);
    let mut fuzzy_terms: Vec<_> = collector
        .into_iter()
        .filter(|(term, _)| term.typ() == Type::Str)
        .map(|(term, _)| term)
        .collect();
    let last = fuzzy_terms
        .pop()
        .into_iter()
        .map(|term| Box::new(FuzzyTermQuery::new_prefix(term, distance, true)) as Box<dyn Query>)
        .map(|query| (Occur::Must, query));
    fuzzy_terms
        .into_iter()
        .map(|term| Box::new(FuzzyTermQuery::new(term, distance, true)) as Box<dyn Query>)
        .map(|query| (Occur::Must, query))
        .chain(last)
        .collect()
}

pub fn process(
    parser: &QueryParser,
    search: &ParagraphSearchRequest,
    schema: &ParagraphSchema,
    distance: Distance,
) -> Result<Vec<QueryParams>, String> {
    // Parse basic search by tokens
    let mut boolean_vec = if !search.body.is_empty() {
        parse_query(parser, &search.body.to_string(), distance)
    } else {
        vec![(Occur::Should, Box::new(AllQuery) as Box<dyn Query>)]
    };

    if !search.uuid.is_empty() {
        let term = Term::from_field_text(schema.uuid, &search.uuid);
        let term_query = TermQuery::new(term, IndexRecordOption::Basic);
        boolean_vec.push((Occur::Must, Box::new(term_query)))
    }

    // Fields
    for value in &search.fields {
        let facet_key: String = format!("/{}", value);
        let facet = Facet::from(facet_key.as_str());
        let facet_term = Term::from_facet(schema.field, &facet);
        let facet_term_query = TermQuery::new(facet_term, IndexRecordOption::Basic);
        boolean_vec.push((Occur::Should, Box::new(facet_term_query)));
    }

    // Add filter
    match search.filter.as_ref() {
        Some(filter) if !filter.tags.is_empty() => {
            for value in &filter.tags {
                let facet = Facet::from(value.as_str());
                let facet_term = Term::from_facet(schema.facets, &facet);
                let facet_term_query = TermQuery::new(facet_term, IndexRecordOption::Basic);
                boolean_vec.push((Occur::Must, Box::new(facet_term_query)));
            }
        }
        _ => (),
    }

    Ok(boolean_vec)
}
