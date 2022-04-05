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
use std::convert::TryFrom;
use std::time::SystemTime;

use nucliadb_protos::{DocumentSearchRequest, Filter, ParagraphSearchRequest, Timestamps};
use prost_wkt_types::Timestamp;
use tantivy::chrono::{DateTime, Utc};
use tantivy::TantivyError;

pub struct SearchQuery {
    pub query: String,
}

fn timestamp_to_string(timestamp: Timestamp) -> Result<String, String> {
    match SystemTime::try_from(timestamp) {
        Ok(sytem_time) => {
            let datetime: DateTime<Utc> = sytem_time.into();
            Ok(datetime.to_rfc3339())
        }
        Err(e) => Err(format!("Can not convert timestamp to SystemTime: {}", e)),
    }
}

impl SearchQuery {
    fn text_query(text: &str) -> String {
        let mut result = String::new();

        let words: Vec<&str> = text.split(' ').collect();
        for word in &words[0..words.len() - 1] {
            result.push_str(format!("text:\"{}\" AND ", word).as_str());
        }
        result.push_str(format!("text:\"{}\"", words[words.len() - 1]).as_str());

        result
    }

    fn create_for_paragraph(search: &ParagraphSearchRequest) -> String {
        let fields = &search.fields;
        let body = &search.body;
        let mut query = String::from("facets:/paragraph AND");

        if !body.is_empty() {
            query.push_str(&SearchQuery::text_query(body));
        }

        if !fields.is_empty() && !body.is_empty() {
            query.push_str(" AND ")
        }

        for (i, (field, text)) in fields.iter().enumerate() {
            let t = format!(
                "(document_field:\"/f/{}\" AND {})",
                field,
                SearchQuery::text_query(text)
            );
            query.push_str(&t);
            if i < fields.len() - 1 {
                query.push_str(" AND ")
            }
        }
        query
    }

    fn create_for_document(body: &str) -> String {
        format!("facets:/document AND {}", SearchQuery::text_query(body))
    }

    fn add_date_filters(timestamps: &Timestamps, query: &mut String) -> tantivy::Result<()> {
        if let Some(from_created) = timestamps.from_created.clone() {
            match timestamp_to_string(from_created) {
                Ok(timestamp) => {
                    let t = format!(" AND created:[{} TO *}}", timestamp);
                    query.push_str(&t);
                }
                Err(e) => {
                    return Err(TantivyError::InvalidArgument(format!(
                        "Invalid timestamp in from_created: {}",
                        e
                    )))
                }
            }
        }

        if let Some(to_created) = timestamps.to_created.clone() {
            match timestamp_to_string(to_created) {
                Ok(timestamp) => {
                    let t = format!(" AND created:{{* TO {}]", timestamp);
                    query.push_str(&t);
                }
                Err(e) => {
                    return Err(TantivyError::InvalidArgument(format!(
                        "Invalid timestamp in to_created: {}",
                        e
                    )))
                }
            }
        }

        if let Some(to_modified) = timestamps.to_modified.clone() {
            match timestamp_to_string(to_modified) {
                Ok(timestamp) => {
                    let t = format!(" AND modified:{{* TO {}]", timestamp);
                    query.push_str(&t);
                }
                Err(e) => {
                    return Err(TantivyError::InvalidArgument(format!(
                        "Invalid timestamp in to_modified: {}",
                        e
                    )))
                }
            }
        }

        if let Some(from_modified) = timestamps.from_modified.clone() {
            match timestamp_to_string(from_modified) {
                Ok(timestamp) => {
                    let t = format!(" AND modified:[{} TO *}}", timestamp);
                    query.push_str(&t);
                }
                Err(e) => {
                    return Err(TantivyError::InvalidArgument(format!(
                        "Invalid timestamp in from_modified: {}",
                        e
                    )))
                }
            }
        }

        Ok(())
    }

    fn add_filter(filter: &Option<Filter>, prefix: &str, query: &mut String) {
        if let Some(filter) = filter {
            for value in &filter.tags {
                query.push_str(&format!(" AND facets:\"{}/{}\"", prefix, value));
            }
        }
    }

    pub fn paragraph(search: &ParagraphSearchRequest) -> tantivy::Result<Self> {
        let mut query: String = SearchQuery::create_for_paragraph(search);

        SearchQuery::add_filter(&search.filter_document, "/l", &mut query);
        SearchQuery::add_filter(&search.filter_paragraph, "/l", &mut query);

        if let Some(timestamps) = &search.timestamps {
            SearchQuery::add_date_filters(timestamps, &mut query)?;
        }

        Ok(SearchQuery { query })
    }

    pub fn document(search: &DocumentSearchRequest) -> tantivy::Result<Self> {
        let mut query: String = SearchQuery::create_for_document(&search.body);
        SearchQuery::add_filter(&search.filter, "/l", &mut query);

        if let Some(timestamps) = &search.timestamps {
            SearchQuery::add_date_filters(timestamps, &mut query)?;
        }

        Ok(SearchQuery { query })
    }
}
