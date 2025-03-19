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

    // v1 fields
    /// Indexed only fields used for storing strings that have been normalized through
    /// [`normalize`]. This fields can be used to provide fuzzy and exact match searchability
    /// without the need of tokenizing. This fields are not stored, therefore not meant to be
    /// returned, just queried.
    pub normalized_source_value: Field,
    pub normalized_target_value: Field,

    // v2 fields
    pub resource_field_id: Option<Field>,
    pub encoded_source_id: Option<Field>,
    pub encoded_target_id: Option<Field>,
    pub facets: Option<Field>,
}

impl Schema {
    pub fn new(version: u64) -> Self {
        let mut builder = TantivySchema::builder();

        let resource_id = if version == 1 {
            builder.add_text_field("resource_id", STRING | STORED)
        } else {
            builder.add_bytes_field("resource_id", INDEXED)
        };

        let value_options: TextOptions = if version == 1 { STORED.into() } else { TEXT | STORED };

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

        let mut resource_field_id = None;
        let mut encoded_source_id = None;
        let mut encoded_target_id = None;
        let mut facets = None;
        if version == 2 {
            resource_field_id = Some(builder.add_bytes_field("resource_field_id", INDEXED | STORED));
            encoded_source_id = Some(builder.add_u64_field("encoded_source_id", FAST));
            encoded_target_id = Some(builder.add_u64_field("encoded_target_id", FAST));
            facets = Some(builder.add_facet_field("facets", STORED));
        }

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

        let separator = if self.version == 1 { "" } else { " " };
        normalized.join(separator)
    }

    pub fn resource_id(&self, doc: &TantivyDocument) -> String {
        if let Some(field_id) = self.resource_field_id {
            let encoded = doc
                .get_first(field_id)
                .expect("Documents must have a resource_field id")
                .as_bytes()
                .unwrap();
            decode_field_id(encoded).0.simple().to_string()
        } else {
            doc.get_first(self.resource_id)
                .and_then(|i| i.as_str().map(String::from))
                .expect("Documents must have a resource id")
        }
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
/// - node value length (u64, 8 bytes)
/// - encoded node value
/// - node type (u64, 8 bytes)
/// - node subtype length (u64, 8 bytes)
/// - encoded node subtype
///
/// Node value and subtype are encoded by converting them to bytes, batch them
/// in sets of 8 and store them as an array of u64.
pub fn encode_node(node_value: &str, node_type: u64, node_subtype: &str) -> Vec<u64> {
    // precompute final encoded size to avoid any extra allocation
    let value_size = node_value.len().div_ceil(8) as u64;
    let subtype_size = node_subtype.len().div_ceil(8) as u64;
    let encoded_size = (1 + value_size + 1 + 1 + subtype_size) as usize;
    let mut out = Vec::with_capacity(encoded_size);

    out.push(value_size);

    let mut slice = node_value.as_bytes();
    while !slice.is_empty() {
        let take = std::cmp::min(8, slice.len());
        let mut data = [0; 8];
        data[..take].copy_from_slice(&slice[..take]);
        slice = &slice[take..];
        out.push(u64::from_le_bytes(data));
    }

    out.push(node_type);

    out.push(subtype_size);

    let mut slice = node_subtype.as_bytes();
    while !slice.is_empty() {
        let take = std::cmp::min(8, slice.len());
        let mut data = [0; 8];
        data[..take].copy_from_slice(&slice[..take]);
        slice = &slice[take..];
        out.push(u64::from_le_bytes(data));
    }

    debug_assert_eq!(
        out.capacity(),
        encoded_size,
        "encoded size estimation for node id is wrong!"
    );

    out
}

/// Decodes a node from a series of u64. This is retrieved from
/// `encoded_source_id` and `encoded_target_id` fast fields and used for value
/// deduplication in the graph Collector
pub fn decode_node(data: &[u64]) -> (String, u64, String) {
    let value_size = data[0] as usize;
    let value_slice = &data[1..(1 + value_size)];
    let node_type = data[1 + value_size];
    let subtype_size = data[1 + value_size + 1] as usize;
    let subtype_slice = &data[(1 + value_size + 2)..];

    let mut value_encoded = Vec::with_capacity(value_size);
    for chunk in value_slice {
        let chunk = chunk.to_le_bytes();
        let mut i = 7;
        while chunk[i] == 0 {
            i -= 1;
        }
        value_encoded.extend_from_slice(&chunk[0..=i]);
    }
    let value = String::from_utf8(value_encoded).unwrap();

    let mut subtype_encoded = Vec::with_capacity(subtype_size);
    for chunk in subtype_slice {
        let chunk = chunk.to_le_bytes();
        let mut i = 7;
        while chunk[i] == 0 {
            i -= 1;
        }
        subtype_encoded.extend_from_slice(&chunk[0..=i]);
    }
    let subtype = String::from_utf8(subtype_encoded).unwrap();

    (value, node_type, subtype)
}

#[cfg(test)]
mod tests {
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
            schema.resource_id => "uuid1",
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
        let node_value = "Anna";
        let node_type = NodeType::Entity;
        let node_subtype = "PERSON";

        for i in 0..7 {
            for j in 0..7 {
                let value_suffix = "x".repeat(i);
                let subtype_suffix = "x".repeat(j);

                let node_value = format!("{node_value}{value_suffix}");
                let node_type = io_maps::node_type_to_u64(node_type);
                let node_subtype = format!("{node_subtype}{subtype_suffix}");

                let encoded = encode_node(&node_value, node_type, &node_subtype);
                let decoded = decode_node(&encoded);

                assert_eq!(node_value, decoded.0);
                assert_eq!(node_type, decoded.1);
                assert_eq!(node_subtype, decoded.2);
            }
        }
    }
}
