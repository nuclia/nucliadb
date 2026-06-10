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
