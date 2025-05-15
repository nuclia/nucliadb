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

use super::schema::ParagraphSchema;
use crate::schema::timestamp_to_datetime_utc;
use crate::search_response::is_label;
use itertools::Itertools;
use nidx_protos::prost::*;
use nidx_tantivy::TantivyIndexer;
use regex::Regex;
use std::collections::HashMap;
use tantivy::doc;
use tantivy::schema::Facet;

lazy_static::lazy_static! {
    static ref REGEX: Regex = Regex::new(r"\\[a-zA-Z0-9]").unwrap();
}

pub fn index_paragraphs(
    writer: &mut TantivyIndexer,
    resource: &nidx_protos::Resource,
    schema: ParagraphSchema,
) -> anyhow::Result<()> {
    let Some(metadata) = resource.metadata.as_ref() else {
        return Err(anyhow::anyhow!("Missing resource metadata"));
    };
    let Some(modified) = metadata.modified.as_ref() else {
        return Err(anyhow::anyhow!("Missing resource modified date in metadata"));
    };
    let Some(created) = metadata.created.as_ref() else {
        return Err(anyhow::anyhow!("Missing resource created date in metadata"));
    };

    let empty_paragraph = HashMap::with_capacity(0);
    let inspect_paragraph = |field: &str| {
        resource
            .paragraphs
            .get(field)
            .map_or_else(|| &empty_paragraph, |i| &i.paragraphs)
    };

    let resource_labels = resource
        .labels
        .iter()
        .map(Facet::from_text)
        .filter_ok(is_label)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| tantivy::TantivyError::InvalidArgument(e.to_string()))?;

    for (field, text_info) in &resource.texts {
        let chars: Vec<char> = REGEX.replace_all(&text_info.text, " ").chars().collect();
        let field_labels = text_info
            .labels
            .iter()
            .map(Facet::from_text)
            .filter_ok(is_label)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| tantivy::TantivyError::InvalidArgument(e.to_string()))?;

        for (paragraph_id, p) in inspect_paragraph(field) {
            let start_pos = p.start as u64;
            let end_pos = p.end as u64;
            let index = p.index;
            let split = &p.split;
            let lower_bound = std::cmp::min(start_pos as usize, chars.len());
            let upper_bound = std::cmp::min(end_pos as usize, chars.len());
            let text: String = chars[lower_bound..upper_bound].iter().collect();
            let facet_field = format!("/{field}");
            let paragraph_labels = p
                .labels
                .iter()
                .map(Facet::from_text)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| tantivy::TantivyError::InvalidArgument(e.to_string()))?;

            let mut doc = doc!(
                schema.uuid => resource.resource.as_ref().expect("Missing resource details").uuid.as_str(),
                schema.modified => timestamp_to_datetime_utc(modified),
                schema.created => timestamp_to_datetime_utc(created),
                schema.status => resource.status as u64,
                schema.repeated_in_field => p.repeated_in_field as u64,
            );

            if let Some(ref metadata) = p.metadata {
                doc.add_bytes(schema.metadata, &metadata.encode_to_vec());
            }

            paragraph_labels
                .into_iter()
                .chain(field_labels.iter().cloned())
                .chain(resource_labels.iter().cloned())
                .for_each(|facet| doc.add_facet(schema.facets, facet));
            doc.add_facet(schema.field, Facet::from(&facet_field));
            doc.add_text(schema.paragraph, paragraph_id.clone());
            doc.add_text(schema.text, &text);
            doc.add_u64(schema.start_pos, start_pos);
            doc.add_u64(schema.end_pos, end_pos);
            doc.add_u64(schema.index, index);
            doc.add_text(schema.split, split);
            let field_uuid = format!("{}/{}", resource.resource.as_ref().unwrap().uuid, field);
            doc.add_text(schema.field_uuid, field_uuid.clone());

            writer.add_document(doc)?;
        }
    }
    Ok(())
}
