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

use nucliadb_protos::ParagraphSearchRequest;
// use prost_wkt_types::Timestamp;
// use tantivy::chrono::{DateTime, Utc};
use tantivy::query::{AllQuery, FuzzyTermQuery, Occur, Query, TermQuery};
use tantivy::schema::{Facet, Field, IndexRecordOption};
use tantivy::Term;

use crate::services::paragraph::schema::ParagraphSchema;

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

pub struct SearchQuery {
    pub query: String,
}

// fn timestamp_to_string(timestamp: Timestamp) -> Result<String, String> {
//     match SystemTime::try_from(timestamp) {
//         Ok(sytem_time) => {
//             let datetime: DateTime<Utc> = sytem_time.into();
//             Ok(datetime.to_rfc3339())
//         }
//         Err(e) => Err(format!("Can not convert timestamp to SystemTime: {}", e)),
//     }
// }

impl SearchQuery {
    fn parse_query(text: &str, field: Field, distance: Distance) -> Vec<(Occur, Box<dyn Query>)> {
        let distance = distance.into();
        let mut words: Vec<&str> = text.split(' ').collect();
        let last = words.pop();
        let mut terms: Vec<(Occur, Box<dyn Query>)> = Vec::with_capacity(words.len());
        for word in &words {
            let term = Term::from_field_text(field, word);
            let fuzzy_term = FuzzyTermQuery::new(term, distance, true);
            terms.push((Occur::Must, Box::new(fuzzy_term)));
        }
        if let Some(word) = last {
            let term = Term::from_field_text(field, word);
            let fuzzy_term = FuzzyTermQuery::new_prefix(term, distance, true);
            terms.push((Occur::Must, Box::new(fuzzy_term)));
        }
        terms
    }

    // fn add_date_filters(timestamps: &Timestamps, query: &mut String) -> tantivy::Result<()> {
    //     if let Some(from_created) = timestamps.from_created.clone() {
    //         match timestamp_to_string(from_created) {
    //             Ok(timestamp) => {
    //                 let t = format!(" AND created:[{} TO *}}", timestamp);
    //                 query.push_str(&t);
    //             }
    //             Err(e) => {
    //                 return Err(TantivyError::InvalidArgument(format!(
    //                     "Invalid timestamp in from_created: {}",
    //                     e
    //                 )))
    //             }
    //         }
    //     }

    //     if let Some(to_created) = timestamps.to_created.clone() {
    //         match timestamp_to_string(to_created) {
    //             Ok(timestamp) => {
    //                 let t = format!(" AND created:{{* TO {}]", timestamp);
    //                 query.push_str(&t);
    //             }
    //             Err(e) => {
    //                 return Err(TantivyError::InvalidArgument(format!(
    //                     "Invalid timestamp in to_created: {}",
    //                     e
    //                 )))
    //             }
    //         }
    //     }

    //     if let Some(to_modified) = timestamps.to_modified.clone() {
    //         match timestamp_to_string(to_modified) {
    //             Ok(timestamp) => {
    //                 let t = format!(" AND modified:{{* TO {}]", timestamp);
    //                 query.push_str(&t);
    //             }
    //             Err(e) => {
    //                 return Err(TantivyError::InvalidArgument(format!(
    //                     "Invalid timestamp in to_modified: {}",
    //                     e
    //                 )))
    //             }
    //         }
    //     }

    //     if let Some(from_modified) = timestamps.from_modified.clone() {
    //         match timestamp_to_string(from_modified) {
    //             Ok(timestamp) => {
    //                 let t = format!(" AND modified:[{} TO *}}", timestamp);
    //                 query.push_str(&t);
    //             }
    //             Err(e) => {
    //                 return Err(TantivyError::InvalidArgument(format!(
    //                     "Invalid timestamp in from_modified: {}",
    //                     e
    //                 )))
    //             }
    //         }
    //     }

    //     Ok(())
    // }

    pub fn process(
        search: &ParagraphSearchRequest,
        schema: &ParagraphSchema,
        distance: Distance,
    ) -> Result<Vec<QueryParams>, String> {
        // Parse basic search by tokens
        let mut boolean_vec = if !search.body.is_empty() {
            SearchQuery::parse_query(&search.body.to_string(), schema.text, distance)
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
}
