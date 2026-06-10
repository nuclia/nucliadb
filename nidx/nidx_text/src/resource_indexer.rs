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

use anyhow::anyhow;
use nidx_tantivy::TantivyIndexer;
use tantivy::{doc, schema::Facet};

use crate::schema::{TextSchema, encode_field_id, encode_field_id_bytes, timestamp_to_datetime_utc};

pub fn index_document(
    writer: &mut TantivyIndexer,
    resource: &nidx_protos::Resource,
    schema: TextSchema,
) -> anyhow::Result<usize> {
    let Some(resource_id) = resource.resource.as_ref().map(|r| r.uuid.as_str()) else {
        return Err(anyhow!("Missing resource ID"));
    };
    let Some(metadata) = resource.metadata.as_ref() else {
        return Err(anyhow!("Missing resource metadata"));
    };
    let Some(modified) = metadata.modified.as_ref() else {
        return Err(anyhow!("Missing resource modified date in metadata"));
    };
    let Some(created) = metadata.created.as_ref() else {
        return Err(anyhow!("Missing resource created date in metadata"));
    };

    let resource_uuid = uuid::Uuid::parse_str(resource_id)?;

    let mut base_doc = doc!(
        schema.uuid => resource_id.as_bytes(),
        schema.modified => timestamp_to_datetime_utc(modified),
        schema.created => timestamp_to_datetime_utc(created),
        schema.status => resource.status as u64,
    );

    let resource_security = resource.security.as_ref();
    if let Some(security_groups) = resource_security.filter(|i| !i.access_groups.is_empty()) {
        base_doc.add_u64(schema.groups_public, 0_u64);
        for group_id in security_groups.access_groups.iter() {
            let mut group_id_key = group_id.clone();
            if !group_id.starts_with('/') {
                // Slash needs to be added to be compatible with tantivy facet fields
                group_id_key = "/".to_string() + group_id;
            }
            let facet = Facet::from(group_id_key.as_str());
            base_doc.add_facet(schema.groups_with_access, facet)
        }
    } else {
        base_doc.add_u64(schema.groups_public, 1_u64);
    }

    for label in resource.labels.iter() {
        let facet = Facet::from(label.as_str());
        base_doc.add_facet(schema.facets, facet);
    }

    for (field, text_info) in &resource.texts {
        let mut field_doc = base_doc.clone();
        let mut facet_key: String = "/".to_owned();
        facet_key.push_str(field.as_str());
        let facet_field = Facet::from(facet_key.as_str());
        field_doc.add_facet(schema.field, facet_field);
        field_doc.add_text(schema.text, &text_info.text);

        for d in encode_field_id(resource_uuid, &format!("/{field}")) {
            field_doc.add_u64(schema.encoded_field_id, d);
        }
        let encoded = encode_field_id_bytes(resource_uuid, field);
        field_doc.add_bytes(schema.encoded_field_id_bytes, encoded.as_slice());

        for label in text_info.labels.iter() {
            let facet = Facet::from(label.as_str());
            field_doc.add_facet(schema.facets, facet);
        }
        writer.add_document(field_doc).unwrap();
    }

    Ok(resource.texts.len())
}
