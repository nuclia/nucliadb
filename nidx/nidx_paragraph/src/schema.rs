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
use nidx_protos::ParagraphMetadata;
use nidx_protos::prost::*;
use tantivy::DateTime;
use tantivy::TantivyDocument;
use tantivy::schema::DateOptions;
use tantivy::schema::Value;
use tantivy::schema::{FacetOptions, Field, NumericOptions, STORED, STRING, Schema, TEXT};

#[derive(Debug, Clone)]
pub struct ParagraphSchema {
    pub schema: Schema,

    /// resource id
    pub uuid: Field,
    /// field id
    pub field: Field,
    /// paragraph id
    pub paragraph: Field,

    // Full field id (resource+field) for prefilter
    pub field_uuid: Field,

    pub text: Field,

    pub start_pos: Field,
    pub end_pos: Field,

    pub created: Field,
    pub modified: Field,

    /// resource status
    pub status: Field,

    /// labels
    pub facets: Field,

    pub split: Field,
    pub index: Field,
    pub repeated_in_field: Field,
    pub metadata: Field,
}

pub fn timestamp_to_datetime_utc(timestamp: &nidx_protos::prost_types::Timestamp) -> DateTime {
    DateTime::from_timestamp_secs(timestamp.seconds)
}

pub fn datetime_utc_to_timestamp(tantivy: &DateTime) -> nidx_protos::prost_types::Timestamp {
    nidx_protos::prost_types::Timestamp {
        seconds: tantivy.into_timestamp_secs(),
        nanos: 0,
    }
}

impl ParagraphSchema {
    pub fn new() -> ParagraphSchema {
        ParagraphSchema::default()
    }

    /// Returns the paragraph metadata for the given document, if any.
    pub fn metadata(&self, doc: &TantivyDocument) -> Option<ParagraphMetadata> {
        doc.get_first(self.metadata)
            .and_then(|value| ParagraphMetadata::decode(value.as_bytes()?).ok())
    }
}

impl Default for ParagraphSchema {
    fn default() -> Self {
        let mut sb = Schema::builder();
        let num_options: NumericOptions = NumericOptions::default().set_stored().set_fast();

        let date_options = DateOptions::default().set_indexed().set_stored().set_fast();
        let repeated_options = NumericOptions::default().set_indexed().set_stored().set_fast();
        let facet_options = FacetOptions::default().set_stored();

        let uuid = sb.add_text_field("uuid", STRING | STORED);
        let field_uuid = sb.add_text_field("field_uuid", STRING);
        let paragraph = sb.add_text_field("paragraph", STRING | STORED);
        // NOTE: review search query tokenization if we change how are we tokenizing this field
        let text = sb.add_text_field("text", TEXT);
        let start_pos = sb.add_u64_field("start_pos", num_options.clone());
        let end_pos = sb.add_u64_field("end_pos", num_options.clone());

        // Date fields needs to be searched in order, order_by_u64_field seems to work in TopDocs.
        let created = sb.add_date_field("created", date_options.clone());
        let modified = sb.add_date_field("modified", date_options);

        // Status
        let status = sb.add_u64_field("status", num_options.clone());
        let index = sb.add_u64_field("index", num_options.clone());

        // Facets
        let facets = sb.add_facet_field("facets", facet_options.clone());
        let field = sb.add_facet_field("field", facet_options.clone());
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
            field_uuid,
            split,
            index,
            repeated_in_field,
            metadata,
        }
    }
}
