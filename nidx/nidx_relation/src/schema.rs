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

use core::str;

use tantivy::TantivyDocument;
use tantivy::schema::{FAST, Field, INDEXED, STORED, STRING, Schema as TantivySchema, TextOptions};
use tantivy::schema::{TEXT, Value};
use uuid::Uuid;

pub fn encode_field_id(rid: Uuid, fid: &str) -> Vec<u8> {
    let mut bytes = rid.as_bytes().to_vec();
    bytes.extend_from_slice(fid.as_bytes());
    bytes
}

pub fn decode_field_id(bytes: &[u8]) -> (Uuid, &str) {
    (
        Uuid::from_bytes(bytes[..16].try_into().unwrap()),
        str::from_utf8(&bytes[16..]).unwrap(),
    )
}

#[derive(Clone, Debug)]
pub struct Schema {
    pub version: u64,
    pub schema: TantivySchema,
    pub resource_id: Field,
    pub source_value: Field,
    pub source_type: Field,
    pub source_subtype: Field,
    pub target_value: Field,
    pub target_type: Field,
    pub target_subtype: Field,
    pub relationship: Field,
    pub label: Field,
    pub metadata: Field,

    pub normalized_source_value: Field,
    pub normalized_target_value: Field,

    // v2 fields
    pub resource_field_id: Field,
    pub encoded_source_id: Field,
    pub encoded_target_id: Field,
    pub encoded_relation_id: Field,
    pub numeric_source_value: Field,
    pub numeric_target_value: Field,
    pub facets: Field,
}

impl Schema {
    pub fn new(version: u64) -> Self {
        let mut builder = TantivySchema::builder();

        let resource_id = builder.add_bytes_field("resource_id", INDEXED);

        let value_options: TextOptions = TEXT | STORED;

        // Special field for searches and don't tokenize values
        let normalized_source_value = builder.add_text_field("indexed_source_value", STRING);
        let source_value = builder.add_text_field("source_value", value_options.clone());
        let source_type = builder.add_u64_field("source_type", INDEXED | STORED);
        let source_subtype = builder.add_text_field("source_subtype", STRING | STORED);

        // Special field for searches and don't tokenize values
        let normalized_target_value = builder.add_text_field("indexed_target_value", STRING);
        let target_value = builder.add_text_field("target_value", value_options);
        let target_type = builder.add_u64_field("to_type", INDEXED | STORED);
        let target_subtype = builder.add_text_field("to_subtype", STRING | STORED);

        let relationship = builder.add_u64_field("relationship", INDEXED | STORED);
        let label = builder.add_text_field("label", STRING | STORED);
        let metadata = builder.add_bytes_field("metadata", STORED);

        let resource_field_id = builder.add_bytes_field("resource_field_id", INDEXED | STORED);
        let encoded_source_id = builder.add_u64_field("encoded_source_id", FAST);
        let encoded_target_id = builder.add_u64_field("encoded_target_id", FAST);
        let encoded_relation_id = builder.add_u64_field("encoded_relation_id", FAST);
        let numeric_source_value = builder.add_u64_field("numeric_source_value", INDEXED | STORED);
        let numeric_target_value = builder.add_u64_field("numeric_target_value", INDEXED | STORED);
        let facets = builder.add_facet_field("facets", STORED);

        let schema = builder.build();

        Schema {
            version,
            schema,
            resource_id,
            source_value,
            source_type,
            source_subtype,
            target_value,
            target_type,
            target_subtype,
            relationship,
            label,
            metadata,
            normalized_source_value,
            normalized_target_value,
            resource_field_id,
            encoded_source_id,
            encoded_target_id,
            encoded_relation_id,
            numeric_source_value,
            numeric_target_value,
            facets,
        }
    }

    pub fn normalize(&self, source: &str) -> String {
        self.normalize_words(source.split_whitespace())
    }

    /// Normalize some words, remove special characters and turn to lowercase
    pub fn normalize_words<'a>(&self, source: impl Iterator<Item = &'a str>) -> String {
        let mut normalized = Vec::new();
        for segment in source {
            let deunicoded = deunicode::deunicode(segment);
            let ascii_lower_cased = deunicoded.to_ascii_lowercase();
            normalized.push(ascii_lower_cased);
        }

        normalized.join(" ")
    }

    pub fn resource_id(&self, doc: &TantivyDocument) -> String {
        let encoded = doc
            .get_first(self.resource_field_id)
            .expect("Documents must have a resource_field id")
            .as_bytes()
            .unwrap();
        decode_field_id(encoded).0.simple().to_string()
    }

    pub fn source_value<'a>(&self, doc: &'a TantivyDocument) -> &'a str {
        doc.get_first(self.source_value)
            .and_then(|i| i.as_str())
            .expect("Documents must have a source value")
    }

    pub fn source_type(&self, doc: &TantivyDocument) -> u64 {
        doc.get_first(self.source_type)
            .and_then(|i| i.as_u64())
            .expect("Documents must have a source type")
    }

    pub fn source_subtype<'a>(&self, doc: &'a TantivyDocument) -> &'a str {
        doc.get_first(self.source_subtype)
            .and_then(|i| i.as_str())
            .expect("Documents must have a source subtype")
    }

    pub fn target_value<'a>(&self, doc: &'a TantivyDocument) -> &'a str {
        doc.get_first(self.target_value)
            .and_then(|i| i.as_str())
            .expect("Documents must have a target value")
    }

    pub fn target_type(&self, doc: &TantivyDocument) -> u64 {
        doc.get_first(self.target_type)
            .and_then(|i| i.as_u64())
            .expect("Documents must have a target type")
    }

    pub fn target_subtype<'a>(&self, doc: &'a TantivyDocument) -> &'a str {
        doc.get_first(self.target_subtype)
            .and_then(|i| i.as_str())
            .expect("Documents must have a target subtype")
    }

    pub fn relationship(&self, doc: &TantivyDocument) -> u64 {
        doc.get_first(self.relationship)
            .and_then(|i| i.as_u64())
            .expect("Documents must have a relationship type")
    }

    pub fn relationship_label<'a>(&self, doc: &'a TantivyDocument) -> &'a str {
        doc.get_first(self.label)
            .and_then(|i| i.as_str())
            .expect("Documents must have a relationship label")
    }

    pub fn metadata<'a>(&self, doc: &'a TantivyDocument) -> Option<&'a [u8]> {
        doc.get_first(self.metadata).and_then(|i| i.as_bytes())
    }
}

/// Encode a graph node as a series of u64. This value is stored in
/// `encoded_source_id` and `encoded_target_id` fast fields.
///
/// We use the following format:
/// - node type (1 byte)
/// - node subtype length (2 bytes)
/// - node subtype (N bytes)
/// - node value (M bytes)
/// - optional 0 padding
///
/// This encoding tries to minimize length as much as possible (while being reasonable)
///
pub fn encode_node(node_value: &str, node_type: u64, node_subtype: &str) -> Vec<u64> {
    // precompute final encoded size to avoid any extra allocation
    let encoded_size = (1 + 2 + node_subtype.len() + node_value.len()).div_ceil(8);
    let mut out = Vec::with_capacity(encoded_size);

    // we'll reuse this to convert from [u8; 8] to u64
    let mut buffer = [0; 8];

    buffer[0] = node_type as u8;

    let subtype_len = (node_subtype.len() as u16).to_le_bytes();
    buffer[1..3].copy_from_slice(&subtype_len);

    let mut free = 5;

    let mut slice = node_subtype.as_bytes();
    while slice.len() >= free {
        buffer[(8 - free)..].copy_from_slice(&slice[..free]);
        slice = &slice[free..];

        out.push(u64::from_le_bytes(buffer));
        buffer = [0; 8];
        free = 8;
    }

    if !slice.is_empty() {
        // we can write some bytes but not enough to fill the buffer
        buffer[(8 - free)..(8 - free + slice.len())].copy_from_slice(slice);
        free -= slice.len();
    }

    // concat the node value immediately after the subtype
    slice = node_value.as_bytes();
    while !slice.is_empty() {
        let take = std::cmp::min(free, slice.len());
        buffer[(8 - free)..(8 - free + take)].copy_from_slice(&slice[..take]);
        slice = &slice[take..];

        out.push(u64::from_le_bytes(buffer));
        buffer = [0; 8];
        free = 8;
    }

    if free < 8 {
        // the buffer has some written bytes but some of the strings were empty and we haven't
        // pushed it to the encoded vector
        out.push(u64::from_le_bytes(buffer));
    }

    debug_assert_eq!(out.capacity(), encoded_size, "wrong encoded size estimation");

    out
}

/// Decodes a node from a series of u64. This is retrieved from
/// `encoded_source_id` and `encoded_target_id` fast fields and used for value
/// deduplication in the graph Collector
pub fn decode_node(data: &[u64]) -> (String, u64, String) {
    // we'll reuse this buffer for decoding u64 to [u8; 8]
    let mut buffer = data[0].to_le_bytes();

    let node_type = buffer[0] as u64;

    let mut encoded_subtype_len = [0; 2];
    encoded_subtype_len.copy_from_slice(&buffer[1..3]);
    let encoded_subtype_len = u16::from_le_bytes(encoded_subtype_len) as usize;
    // value length + extra padding (if needed)
    let encoded_value_len = data.len() * 8 - (1 + 2 + encoded_subtype_len);

    let mut subtype_encoded = Vec::with_capacity(encoded_subtype_len);
    let mut value_encoded = Vec::with_capacity(encoded_value_len);

    let mut slice = data;
    let mut filled = 3;

    if encoded_subtype_len > 0 {
        let mut remaining = encoded_subtype_len;

        if remaining < 8 - filled {
            // we share the u64 with node_value
            subtype_encoded.extend_from_slice(&buffer[filled..(filled + remaining)]);
            filled = 3 + remaining;
        } else {
            // subtype spans across this and maybe more u64 values
            subtype_encoded.extend_from_slice(&buffer[filled..]);
            remaining -= 8 - filled;

            while remaining >= 8 {
                slice = &slice[1..];
                buffer = slice[0].to_le_bytes();
                subtype_encoded.extend_from_slice(&buffer);
                remaining -= 8;
            }

            if remaining == 0 {
                // we finished in the last byte, we'll start in the next u64
                filled = 0;
                slice = &slice[1..];
            } else {
                slice = &slice[1..];
                buffer = slice[0].to_le_bytes();
                subtype_encoded.extend_from_slice(&buffer[..remaining]);
                filled = remaining;
            }
        }
    }
    debug_assert_eq!(
        subtype_encoded.capacity(),
        encoded_subtype_len,
        "wrong encoded length estimation"
    );
    let subtype = String::from_utf8(subtype_encoded).unwrap();

    if encoded_value_len > 0 {
        // if we start in a new u64, we must get it from the slice
        if filled == 0 {
            buffer = slice[0].to_le_bytes();
        }

        let remaining = encoded_value_len;

        if remaining <= 8 - filled {
            // don't copy padding
            let mut i = 7;
            while buffer[i] == 0 && i >= filled {
                i -= 1;
            }
            value_encoded.extend_from_slice(&buffer[filled..=i]);
        } else {
            value_encoded.extend_from_slice(&buffer[filled..]);

            for buffer in &slice[1..] {
                let buffer = buffer.to_le_bytes();
                // don't copy padding
                let mut i = 7;
                while buffer[i] == 0 {
                    i -= 1;
                }
                value_encoded.extend_from_slice(&buffer[..=i]);
            }
        }
    }
    debug_assert_eq!(
        value_encoded.capacity(),
        encoded_value_len,
        "wrong encoded length estimation"
    );
    let value = String::from_utf8(value_encoded).unwrap();

    (value, node_type, subtype)
}

pub fn encode_relation(relation_type: u64, relation_label: &str) -> Vec<u64> {
    let encoded_size = (1 + relation_label.len()).div_ceil(8);
    let mut out = Vec::with_capacity(encoded_size);

    let mut buffer = [0; 8];
    buffer[0] = relation_type as u8;

    let mut slice = relation_label.as_bytes();

    let take = std::cmp::min(7, slice.len());
    buffer[1..(1 + take)].copy_from_slice(&slice[..take]);
    slice = &slice[take..];
    out.push(u64::from_le_bytes(buffer));

    while !slice.is_empty() {
        let take = std::cmp::min(8, slice.len());
        let mut buffer = [0; 8];
        buffer[..take].copy_from_slice(&slice[..take]);
        slice = &slice[take..];
        out.push(u64::from_le_bytes(buffer));
    }

    debug_assert_eq!(out.capacity(), encoded_size, "wrong encoded size estimation");

    out
}

#[allow(dead_code)]
pub fn decode_relation(data: &[u64]) -> (u64, String) {
    let buffer = data[0].to_le_bytes();
    let relation_type = buffer[0] as u64;

    let label_len = data.len() * 8 - 1;
    let mut encoded_label = Vec::with_capacity(label_len);

    // don't copy padding
    let mut i = 7;
    while buffer[i] == 0 && i >= 1 {
        i -= 1;
    }
    encoded_label.extend_from_slice(&buffer[1..=i]);

    for buffer in &data[1..] {
        let buffer = buffer.to_le_bytes();
        // don't copy padding
        let mut i = 7;
        while buffer[i] == 0 {
            i -= 1;
        }
        encoded_label.extend_from_slice(&buffer[..=i]);
    }

    let relation_label = String::from_utf8(encoded_label).unwrap();

    (relation_type, relation_label)
}

#[cfg(test)]
mod tests {
    use nidx_protos::relation::RelationType;
    use nidx_protos::relation_node::NodeType;
    use tantivy::collector::DocSetCollector;
    use tantivy::query::TermQuery;
    use tantivy::schema::IndexRecordOption;
    use tantivy::{Index, IndexWriter, Term, doc};

    use crate::io_maps;

    use super::*;

    #[test]
    fn test_use_schema_v1() -> tantivy::Result<()> {
        let schema = Schema::new(1);
        let index = Index::create_in_ram(schema.schema.clone());
        let node_value = "this is a source";

        let mut index_writer: IndexWriter = index.writer(30_000_000)?;
        index_writer.add_document(doc!(
            schema.resource_id => "uuid1".as_bytes(),
            schema.source_value => node_value,
            schema.normalized_source_value => schema.normalize(node_value),
            schema.target_value => "to2",
            schema.relationship => 0u64,
        ))?;

        index_writer.commit()?;

        let reader = index.reader()?;
        let searcher = reader.searcher();
        let term = Term::from_field_text(schema.normalized_source_value, &schema.normalize(node_value));
        let term_query = TermQuery::new(term, IndexRecordOption::Basic);
        let results = searcher.search(&term_query, &DocSetCollector)?;

        assert_eq!(results.len(), 1);

        let address = results.into_iter().next().unwrap();
        let doc = searcher.doc(address)?;
        let source_value = schema.source_value(&doc);
        assert_eq!(node_value, source_value);

        Ok(())
    }

    #[test]
    fn test_encode_decode_graph_nodes() {
        // test different string lengths combinations to ensure encoding works
        // crossing 8-byte block length
        for node_type in [NodeType::Entity, NodeType::User] {
            for i in 0..9 {
                for j in 0..9 {
                    let node_type = io_maps::node_type_to_u64(node_type);
                    let node_value = ('A'..='Z').take(i).collect::<String>();
                    let node_subtype = ('A'..='Z').take(j).collect::<String>();

                    let encoded = encode_node(&node_value, node_type, &node_subtype);
                    let decoded = decode_node(&encoded);

                    assert_eq!(node_value, decoded.0);
                    assert_eq!(node_type, decoded.1);
                    assert_eq!(node_subtype, decoded.2);
                }
            }
        }
    }

    #[test]
    fn test_encode_decode_graph_relations() {
        // test different string lengths combinations to ensure encoding works
        // crossing 8-byte block length

        for relation_type in [RelationType::Child, RelationType::Other] {
            for i in 0..9 {
                let relation_type = io_maps::relation_type_to_u64(relation_type);
                let relation_label = ('A'..='Z').take(i).collect::<String>();

                let encoded = encode_relation(relation_type, &relation_label);
                let decoded = decode_relation(&encoded);

                assert_eq!(relation_type, decoded.0);
                assert_eq!(relation_label, decoded.1);
            }
        }
    }
}
