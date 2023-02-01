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
use nucliadb_core::protos::prost::Message;
use nucliadb_core::protos::prost_types::Timestamp;
use nucliadb_core::protos::ParagraphMetadata;
use tantivy::chrono::{DateTime, NaiveDateTime, Utc};
use tantivy::schema::{
    Cardinality, FacetOptions, Field, NumericOptions, Schema, STORED, STRING, TEXT,
};
use tantivy::Document;

#[derive(Debug, Clone)]
pub struct ParagraphSchema {
    pub schema: Schema,

    pub uuid: Field,
    pub paragraph: Field,
    pub text: Field,
    pub start_pos: Field,
    pub end_pos: Field,
    pub created: Field,
    pub modified: Field,
    pub status: Field,
    pub facets: Field,
    pub field: Field,
    pub split: Field,
    pub index: Field,
    pub repeated_in_field: Field,
    pub metadata: Field,
}

pub fn timestamp_to_datetime_utc(timestamp: &Timestamp) -> DateTime<Utc> {
    let naive =
        NaiveDateTime::from_timestamp_opt(timestamp.seconds, timestamp.nanos as u32).unwrap();
    DateTime::from_utc(naive, tantivy::chrono::Utc)
}

impl ParagraphSchema {
    pub fn new() -> ParagraphSchema {
        ParagraphSchema::default()
    }
    /// Returns the paragraph metadata for the given document, if any.
    pub fn metadata(&self, doc: &Document) -> Option<ParagraphMetadata> {
        doc.get_first(self.metadata)
            .and_then(|value| ParagraphMetadata::decode(value.as_bytes()?).ok())
    }
}

impl Default for ParagraphSchema {
    fn default() -> Self {
        let mut sb = Schema::builder();
        let num_options: NumericOptions = NumericOptions::default()
            .set_stored()
            .set_fast(Cardinality::SingleValue);

        let date_options = NumericOptions::default()
            .set_indexed()
            .set_stored()
            .set_fast(Cardinality::SingleValue);
        let repeated_options = NumericOptions::default()
            .set_indexed()
            .set_stored()
            .set_fast(Cardinality::SingleValue);
        let facet_options = FacetOptions::default().set_stored();

        let uuid = sb.add_text_field("uuid", STRING | STORED);
        let paragraph = sb.add_text_field("paragraph", STRING | STORED);
        let text = sb.add_text_field("text", TEXT);
        let start_pos = sb.add_u64_field("start_pos", num_options.clone());
        let end_pos = sb.add_u64_field("end_pos", num_options.clone());

        // Date fields needs to be searched in order, order_by_u64_field seems to work in TopDocs.
        let created = sb.add_date_field("created", date_options.clone());
        let modified = sb.add_date_field("modified", date_options);

        // Status
        let status = sb.add_u64_field("status", num_options.clone());
        let index = sb.add_u64_field("index", num_options);

        // Facets
        let facets = sb.add_facet_field("facets", facet_options.clone());
        let field = sb.add_facet_field("field", facet_options);
        let split = sb.add_text_field("split", STRING | STORED);

        let repeated_in_field = sb.add_u64_field("repeated_in_field", repeated_options);
        let metadata = sb.add_bytes_field("metadata", STORED);

        let schema = sb.build();
        ParagraphSchema {
            schema,
            uuid,
            paragraph,
            text,
            start_pos,
            end_pos,
            created,
            modified,
            status,
            facets,
            field,
            split,
            index,
            repeated_in_field,
            metadata,
        }
    }
}
