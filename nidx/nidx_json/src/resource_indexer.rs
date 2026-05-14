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

use anyhow::anyhow;
use nidx_tantivy::TantivyIndexer;
use tantivy::schema::OwnedValue;
use uuid::Uuid;

use crate::schema::{JsonSchema, encode_rid};

pub fn index_json_fields(
    writer: &mut TantivyIndexer,
    resource: &nidx_protos::Resource,
    schema: JsonSchema,
) -> anyhow::Result<()> {
    if resource.json_fields.is_empty() {
        return Ok(());
    }

    let Some(resource_id) = resource.resource.as_ref().map(|r| r.uuid.as_str()) else {
        return Err(anyhow!("Missing resource ID"));
    };

    let resource_uuid = Uuid::parse_str(resource_id)?;
    let encoded = encode_rid(resource_uuid);

    // Build a single nested object per resource: { "field_id": <json> }
    let mut nested: Vec<(String, OwnedValue)> = Vec::with_capacity(resource.json_fields.len());
    for (field_key, json_info) in resource.json_fields.iter() {
        let parsed: serde_json::Value = serde_json::from_str(&json_info.value)?;
        let owned = OwnedValue::from(parsed);
        nested.push((field_key.clone(), owned));
    }

    let mut doc = tantivy::TantivyDocument::default();
    doc.add_bytes(schema.rid, resource_id.as_bytes());
    for word in encoded.iter().copied() {
        doc.add_u64(schema.encoded_rid, word);
    }
    doc.add_field_value(schema.json, &OwnedValue::Object(nested));

    writer.add_document(doc)?;

    Ok(())
}
