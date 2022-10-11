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

use nucliadb_protos::DocumentSearchRequest;
use nucliadb_service_interface::dependencies::*;
use tantivy::query::*;
use tantivy::schema::{Facet, IndexRecordOption};
use tantivy::Term;

use crate::schema::FieldSchema;

pub fn create_query(
    parser: &QueryParser,
    search: &DocumentSearchRequest,
    schema: &FieldSchema,
    text: &str,
) -> Box<dyn Query> {
    let mut queries = vec![];
    let main_q = if text.is_empty() {
        Box::new(AllQuery)
    } else {
        parser.parse_query(text).unwrap()
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
            queries.push((Occur::Should, Box::new(facet_term_query)));
        });
    Box::new(BooleanQuery::new(queries))
}
