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

use nucliadb_protos::{DocumentSearchRequest, Filter};
use nucliadb_service_interface::dependencies::*;

pub struct SearchQuery {
    pub query: String,
}

impl SearchQuery {
    fn create(search: &DocumentSearchRequest, body: &str) -> String {
        let fields = &search.fields;
        let filter = &search.filter;

        let mut query = String::from("");
        if !body.is_empty() {
            query.push_str(body);
        }

        if !fields.is_empty() {
            query.push_str(" ( ")
        }

        let mut first: bool = true;

        for field in fields {
            if first {
                let t = format!(" field:\"/{}\" ", field);
                query.push_str(&t);
                first = false;
            } else {
                let t = format!("OR field:\"/{}\" ", field);
                query.push_str(&t);
            }
        }

        if !fields.is_empty() {
            query.push_str(" ) ")
        }

        SearchQuery::add_filter(filter, &mut query);

        // if let Some(timestamps) = &search.timestamps {
        //     SearchQuery::add_date_filters(timestamps, &mut query);
        // }
        query
    }

    fn add_filter(filter: &Option<Filter>, query: &mut String) {
        use std::fmt::Write;
        if let Some(filter) = filter {
            for value in &filter.tags {
                write!(query, " facets:\"{}\"", value).unwrap();
            }
        }
    }

    pub fn document(search: &DocumentSearchRequest, body: &str) -> tantivy::Result<String> {
        let query: String = SearchQuery::create(search, body);

        Ok(query)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_document_creation() {
        let request = DocumentSearchRequest {
            body: "test".to_string(),
            page_number: 0,
            result_per_page: 10,
            id: "".to_string(),
            filter: None,
            order: None,
            faceted: None,
            fields: vec![],
            timestamps: None,
            reload: false,
        };
        let query = SearchQuery::document(&request, &request.body).unwrap();
        println!("{}", query);
    }
}
