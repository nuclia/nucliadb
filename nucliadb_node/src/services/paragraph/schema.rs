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
use tantivy::chrono::{DateTime, NaiveDateTime, Utc};
use tantivy::schema::{Cardinality, FacetOptions, Field, IntOptions, Schema, STORED, STRING, TEXT};

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
}

pub(super) fn timestamp_to_datetime_utc(timestamp: &prost_wkt_types::Timestamp) -> DateTime<Utc> {
    let naive = NaiveDateTime::from_timestamp(timestamp.seconds, timestamp.nanos as u32);
    DateTime::from_utc(naive, tantivy::chrono::Utc)
}

impl ParagraphSchema {
    pub fn new() -> Self {
        tracing::info!("creating paragraph schema");
        let mut sb = Schema::builder();
        let num_options: IntOptions = IntOptions::default()
            .set_stored()
            .set_fast(Cardinality::SingleValue);

        let date_options = IntOptions::default()
            .set_indexed()
            .set_stored()
            .set_fast(Cardinality::SingleValue);

        let facet_options = FacetOptions::default().set_indexed().set_stored();

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

        let schema = sb.build();
        tracing::info!("paragraph schema created");
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
        }
    }

    // pub fn base_document(
    //     resource: &Resource,
    //     creation_date: &Option<DateTime<Utc>>,
    // ) -> tantivy::Document {
    //     let metadata = resource.metadata.as_ref().unwrap();

    //     let modified = metadata.modified.as_ref().unwrap();

    //     let schema = Self::new();

    //     let mut doc = doc!(
    //         schema.uuid => resource.resource.as_ref().unwrap().uuid.as_str(),
    //         schema.modified => timestamp_to_datetime_utc(modified),
    //         schema.status => resource.status as u64
    //     );

    //     if let Some(creation_date) = creation_date {
    //         doc.add_date(schema.created, creation_date)
    //     } else {
    //         doc.add_date(schema.created, &timestamp_to_datetime_utc(modified))
    //     }

    //     // TODO: Check if the labels should be any path.
    //     for label in &resource.labels {
    //         let path = format!("/l/{}", label);
    //         trace!("Document facets: {}", path);
    //         let classification = Facet::from(path.as_str());
    //         doc.add_facet(schema.facets, classification);
    //     }

    //     doc
    // }
}

impl Default for ParagraphSchema {
    fn default() -> Self {
        Self::new()
    }
}
