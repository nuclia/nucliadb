// Copyright 2021 Bosutech XXI S.L.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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
        let encoded_rid = sb.add_u64_field("encoded_resource_id", FAST);

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
/// This is stored in the fast field `encoded_resource_id`
/// We convert to bytes, batch them in sets of 8, and store
/// as an array of u64
pub fn encode_rid(rid: uuid::Uuid) -> Vec<u64> {
    let (r1, r2) = rid.as_u64_pair();
    vec![r1, r2]
}

/// Decodes a resource and field id from a series of u64
/// This is retrieved from the fast field `encoded_resource_id`
/// and used for faster loading in the prefilter Collector
pub fn decode_rid(mut data: impl Iterator<Item = u64>) -> uuid::Uuid {
    let w0 = data.next().expect("encoded_resource_id missing first word");
    let w1 = data.next().expect("encoded_resource_id missing second word");
    uuid::Uuid::from_u64_pair(w0, w1)
}
