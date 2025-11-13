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

use std::cmp::min;

use tantivy::schema::DateOptions;
use tantivy::{
    DateTime,
    schema::{FAST, FacetOptions, Field, INDEXED, NumericOptions, STORED, Schema, TEXT},
};

#[derive(Debug, Clone)]
pub struct TextSchema {
    pub schema: Schema,

    /// Resource id
    pub uuid: Field,

    /// Field id
    pub field: Field,

    pub text: Field,
    pub created: Field,
    pub modified: Field,
    pub status: Field,
    pub facets: Field,

    // Security
    pub groups_public: Field,
    pub groups_with_access: Field,

    pub encoded_field_id: Field,
    pub encoded_field_id_bytes: Field,
}

pub fn timestamp_to_datetime_utc(timestamp: &nidx_protos::prost_types::Timestamp) -> DateTime {
    DateTime::from_timestamp_secs(timestamp.seconds)
}

impl TextSchema {
    pub fn new(_version: u64) -> Self {
        let mut sb = Schema::builder();
        let num_options: NumericOptions = NumericOptions::default().set_indexed().set_fast();

        let date_options = DateOptions::default().set_indexed().set_fast();

        let facet_options = FacetOptions::default().set_stored();

        let uuid = sb.add_bytes_field("uuid", STORED | FAST | INDEXED);
        let field = sb.add_facet_field("field", facet_options.clone());

        let text = sb.add_text_field("text", TEXT);

        // Date fields needs to be searched in order, order_by_u64_field seems to work in TopDocs.
        let created = sb.add_date_field("created", date_options.clone());
        let modified = sb.add_date_field("modified", date_options);

        // Status
        let status = sb.add_u64_field("status", num_options.clone());

        // Facets
        let facets = sb.add_facet_field("facets", facet_options.clone());

        // Security
        let groups_public = sb.add_u64_field("groups_public", num_options);
        let groups_with_access = sb.add_facet_field("groups_with_access", facet_options);

        // v4: Field ID encoded as array of u64 for faster retrieval during prefilter
        // Using a bytes field is slow due to tantivy's implementation being slow with many unique values.
        // A better implementation is tracked in https://github.com/quickwit-oss/tantivy/issues/2090
        let encoded_field_id = sb.add_u64_field("encoded_field_id", FAST);
        // v4: Field ID encoded as array of u8 for faster deletions
        let encoded_field_id_bytes = sb.add_bytes_field("encoded_field_id_bytes", INDEXED);

        let schema = sb.build();

        TextSchema {
            schema,
            uuid,
            text,
            created,
            modified,
            status,
            facets,
            field,
            groups_public,
            groups_with_access,
            encoded_field_id,
            encoded_field_id_bytes,
        }
    }
}

/// Encodes a resource and field id as a series of u64
/// This is stored in the fast field `encoded_field_id`
/// We convert to bytes, batch them in sets of 8, and store
/// as an array of u64
pub fn encode_field_id(rid: uuid::Uuid, fid: &str) -> Vec<u64> {
    let (r1, r2) = rid.as_u64_pair();
    let mut out = vec![r1, r2];
    let bb = fid.as_bytes();
    let mut slice = bb;
    while !slice.is_empty() {
        let take = min(8, slice.len());
        let mut data = [0; 8];
        data[..take].copy_from_slice(&slice[..take]);
        slice = &slice[take..];
        out.push(u64::from_le_bytes(data));
    }

    out
}

/// Decodes a resource and field id from a series of u64
/// This is retrieved from the fast field `encoded_field_id`
/// and used for faster loading in the prefilter Collector
pub fn decode_field_id(data: &[u64]) -> (uuid::Uuid, String) {
    let rid = uuid::Uuid::from_u64_pair(data[0], data[1]);
    let mut ubytes = Vec::with_capacity((data.len() - 2) * 8);
    for word in &data[2..] {
        let wb = word.to_le_bytes();
        let mut i = 7;
        while wb[i] == 0 {
            i -= 1;
        }
        ubytes.extend_from_slice(&wb[0..=i]);
    }
    (rid, String::from_utf8(ubytes).unwrap())
}

/// Encodes a resource and field id as a series of u8
/// This is stored in the indexed field `encoded_field_id_bytes`
pub fn encode_field_id_bytes(rid: uuid::Uuid, fid: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(16 + fid.len());
    out.extend_from_slice(rid.as_bytes());
    out.extend_from_slice(fid.as_bytes());
    out
}

#[cfg(test)]
mod tests {
    use super::{decode_field_id, encode_field_id, encode_field_id_bytes};

    /// Decodes a resource and field id from a series of u8
    pub fn decode_field_id_bytes(data: &[u8]) -> (uuid::Uuid, String) {
        if data.len() < 16 {
            panic!("Data is too short to contain a valid UUID");
        }
        let rid = uuid::Uuid::from_slice(&data[..16]).unwrap();
        let fid = String::from_utf8(data[16..].to_vec()).unwrap();
        (rid, fid)
    }

    #[test]
    fn test_encode_decode_field_id() {
        // Test with different lengths to ensure it works when crossing block length
        let testcases = [
            ("03ce0c2cdcc0060069fb8f69c73fa8e2", "/a/title"),
            ("03ce0c2cdcc0060069fb8f69c73fa8e2", "/a/title1"),
            ("03ce0c2cdcc0060069fb8f69c73fa8e2", "/a/title12"),
            ("03ce0c2cdcc0060069fb8f69c73fa8e2", "/a/title123"),
            ("03ce0c2cdcc0060069fb8f69c73fa8e2", "/a/title1234"),
            ("03ce0c2cdcc0060069fb8f69c73fa8e2", "/a/title12345"),
            ("03ce0c2cdcc0060069fb8f69c73fa8e2", "/a/title123456"),
            ("03ce0c2cdcc0060069fb8f69c73fa8e2", "/a/title1234567"),
        ];
        for t in testcases {
            let rid = uuid::Uuid::parse_str(t.0).unwrap();
            let data = encode_field_id(rid, t.1);
            let (decoded_rid, decoded_fid) = decode_field_id(&data);
            assert_eq!(decoded_rid.simple().to_string().as_str(), t.0);
            assert_eq!(decoded_fid, t.1);
        }
    }

    #[test]
    fn test_encode_decode_field_id_bytes() {
        // Test with different lengths to ensure it works when crossing block length
        let testcases = [
            ("03ce0c2cdcc0060069fb8f69c73fa8e2", "a/title"),
            ("03ce0c2cdcc0060069fb8f69c73fa8e2", "a/title1"),
            ("03ce0c2cdcc0060069fb8f69c73fa8e2", "a/title12"),
            ("03ce0c2cdcc0060069fb8f69c73fa8e2", "a/title123"),
            ("03ce0c2cdcc0060069fb8f69c73fa8e2", "a/title1234"),
            ("03ce0c2cdcc0060069fb8f69c73fa8e2", "a/title12345"),
            ("03ce0c2cdcc0060069fb8f69c73fa8e2", "a/title123456"),
            ("03ce0c2cdcc0060069fb8f69c73fa8e2", "a/title1234567"),
        ];
        for t in testcases {
            let rid = uuid::Uuid::parse_str(t.0).unwrap();
            let data = encode_field_id_bytes(rid, t.1);
            let (decoded_rid, decoded_fid) = decode_field_id_bytes(&data);
            assert_eq!(decoded_rid.simple().to_string().as_str(), t.0);
            assert_eq!(decoded_fid, t.1);
        }
    }
}
