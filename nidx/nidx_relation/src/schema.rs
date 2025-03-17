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

use tantivy::TantivyDocument;
use tantivy::schema::{FAST, Field, INDEXED, STORED, STRING, Schema as TantivySchema};
use tantivy::schema::{TEXT, Value};

/// The source will be deunicoded, se
pub fn normalize(source: &str) -> String {
    normalize_words(source.split_whitespace())
}

pub fn normalize_words<'a>(source: impl Iterator<Item = &'a str>) -> String {
    let mut normalized = String::new();
    for segment in source {
        let deunicoded = deunicode::deunicode(segment);
        let ascii_lower_cased = deunicoded.to_ascii_lowercase();
        normalized.push_str(&ascii_lower_cased);
    }
    normalized
}

#[derive(Clone, Debug)]
pub struct Schema {
    pub version: u64,
    pub schema: TantivySchema,
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
    pub resource_id: Option<Field>,

    /// Indexed only fields used for storing strings that have been normalized through
    /// [`normalize`]. This fields can be used to provide fuzzy and exact match searchability
    /// without the need of tokenizing. This fields are not stored, therefore not meant to be
    /// returned, just queried.
    pub normalized_source_value: Option<Field>,
    pub normalized_target_value: Option<Field>,

    // v2 fields
    pub resource_field_id: Option<Field>,
    pub encoded_source_id: Option<Field>,
    pub encoded_target_id: Option<Field>,
}

impl Schema {
    pub fn new(version: u64) -> Self {
        let mut builder = TantivySchema::builder();

        let resource_id = if version == 1 {
            Some(builder.add_text_field("resource_id", STRING | STORED))
        } else {
            None
        };

        let (normalized_source_value, source_value) = if version == 1 {
            // Special field for searches and don't tokenize values
            (
                Some(builder.add_text_field("indexed_source_value", STRING)),
                builder.add_text_field("source_value", STORED),
            )
        } else {
            // Tokenize values and use tantivy search
            (None, builder.add_text_field("source_value", TEXT | STORED))
        };
        let source_type = builder.add_u64_field("source_type", INDEXED | STORED);
        let source_subtype = builder.add_text_field("source_subtype", STRING | STORED);
        let (normalized_target_value, target_value) = if version == 1 {
            // Special field for searches and don't tokenize values
            (
                Some(builder.add_text_field("indexed_target_value", STRING)),
                builder.add_text_field("target_value", STORED),
            )
        } else {
            // Tokenize values and use tantivy search
            (None, builder.add_text_field("target_value", TEXT | STORED))
        };
        let target_type = builder.add_u64_field("to_type", INDEXED | STORED);
        let target_subtype = builder.add_text_field("to_subtype", STRING | STORED);
        let relationship = builder.add_u64_field("relationship", INDEXED | STORED);
        let label = builder.add_text_field("label", STRING | STORED);
        let metadata = builder.add_bytes_field("metadata", STORED);

        let mut resource_field_id = None;
        let mut encoded_source_id = None;
        let mut encoded_target_id = None;
        if version == 2 {
            resource_field_id = Some(builder.add_text_field("resource_field_id", STRING | STORED));
            encoded_source_id = Some(builder.add_u64_field("encoded_source_id", FAST));
            encoded_target_id = Some(builder.add_u64_field("encoded_target_id", FAST));
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
        }
    }

    pub fn resource_id(&self, doc: &TantivyDocument) -> String {
        if self.version != 1 {
            todo!()
        }
        doc.get_first(self.resource_id.unwrap())
            .and_then(|i| i.as_str().map(String::from))
            .expect("Documents must have a resource id")
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

#[cfg(test)]
mod tests {
    use tantivy::collector::DocSetCollector;
    use tantivy::query::TermQuery;
    use tantivy::schema::IndexRecordOption;
    use tantivy::{Index, IndexWriter, Term, doc};

    use super::*;

    #[test]
    fn test_use_schema_v1() -> tantivy::Result<()> {
        let schema = Schema::new(1);
        let index = Index::create_in_ram(schema.schema.clone());
        let node_value = "this is a source";

        let mut index_writer: IndexWriter = index.writer(30_000_000)?;
        index_writer.add_document(doc!(
            schema.resource_id.unwrap() => "uuid1",
            schema.source_value => node_value,
            schema.normalized_source_value.unwrap() => normalize(node_value),
            schema.target_value => "to2",
            schema.relationship => 0u64,
        ))?;

        index_writer.commit()?;

        let reader = index.reader()?;
        let searcher = reader.searcher();
        let term = Term::from_field_text(schema.normalized_source_value.unwrap(), &normalize(node_value));
        let term_query = TermQuery::new(term, IndexRecordOption::Basic);
        let results = searcher.search(&term_query, &DocSetCollector)?;

        assert_eq!(results.len(), 1);

        let address = results.into_iter().next().unwrap();
        let doc = searcher.doc(address)?;
        let source_value = schema.source_value(&doc);
        assert_eq!(node_value, source_value);

        Ok(())
    }
}
