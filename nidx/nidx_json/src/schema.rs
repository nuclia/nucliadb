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

use tantivy::schema::JsonObjectOptions;
use tantivy::schema::{FAST, Field, INDEXED, STORED, Schema, TEXT};

#[derive(Debug, Clone)]
pub struct JsonSchema {
    pub schema: Schema,

    /// Resource id
    pub rid: Field, // Encoded as bytes for index
    pub encoded_rid: Field, // Encoded as fast field for collector

    pub json: Field,
}

impl JsonSchema {
    pub fn new() -> Self {
        let mut sb = Schema::builder();

        let rid = sb.add_bytes_field("uuid", STORED | INDEXED);
        let encoded_rid = sb.add_u64_field("encoded_field_id", FAST);

        // STORED: field values are retrievable
        // TEXT: values are tokenised into the inverted index (enables TermQuery)
        // FAST: values are stored in columnar format (enables FastFieldRangeQuery
        //       for numeric and string range predicates on JSON sub-paths)
        let json_options: JsonObjectOptions = (STORED | TEXT | FAST).into();
        let json = sb.add_json_field("json", json_options);

        let schema = sb.build();

        JsonSchema {
            schema,
            rid,
            encoded_rid,
            json,
        }
    }
}

/// Encodes a resource and field id as a series of u64
/// This is stored in the fast field `encoded_field_id`
/// We convert to bytes, batch them in sets of 8, and store
/// as an array of u64
pub fn encode_rid(rid: uuid::Uuid) -> Vec<u64> {
    let (r1, r2) = rid.as_u64_pair();
    vec![r1, r2]
}

/// Decodes a resource and field id from a series of u64
/// This is retrieved from the fast field `encoded_field_id`
/// and used for faster loading in the prefilter Collector
pub fn decode_rid(mut data: impl Iterator<Item = u64>) -> uuid::Uuid {
    let w0 = data.next().expect("encoded_field_id missing first word");
    let w1 = data.next().expect("encoded_field_id missing second word");
    uuid::Uuid::from_u64_pair(w0, w1)
}
