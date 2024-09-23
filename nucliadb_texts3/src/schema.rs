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
use nucliadb_core::protos::*;
use tantivy::chrono::{DateTime, Utc};
use tantivy::schema::{Cardinality, FacetOptions, Field, NumericOptions, Schema, FAST, INDEXED, STORED, TEXT};

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
}

pub fn timestamp_to_datetime_utc(timestamp: &prost_types::Timestamp) -> DateTime<Utc> {
    DateTime::from_timestamp(timestamp.seconds, timestamp.nanos as u32).unwrap()
}

impl TextSchema {
    pub fn new() -> Self {
        let mut sb = Schema::builder();
        let num_options: NumericOptions = NumericOptions::default().set_indexed().set_fast(Cardinality::SingleValue);

        let date_options = NumericOptions::default().set_indexed().set_fast(Cardinality::SingleValue);

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
        }
    }
}

impl Default for TextSchema {
    fn default() -> Self {
        Self::new()
    }
}
