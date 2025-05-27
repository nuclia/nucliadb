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

use crate::io_maps;
use crate::schema::{Schema, encode_field_id, encode_node, encode_relation};
use anyhow::anyhow;
use nidx_protos::prost::*;
use nidx_tantivy::TantivyIndexer;
use tantivy::doc;
use tantivy::schema::Facet;
use uuid::Uuid;

pub fn index_relations(
    writer: &mut TantivyIndexer,
    resource: &nidx_protos::Resource,
    schema: Schema,
) -> anyhow::Result<()> {
    let resource_id_str = resource
        .resource
        .as_ref()
        .map(|r| r.uuid.as_str())
        .expect("Missing resource ID");

    for (field_key, relations) in &resource.field_relations {
        for index_relation in &relations.relations {
            let relation = index_relation.relation.as_ref().unwrap();

            let rid = Uuid::parse_str(resource_id_str)?;

            let source = relation.source.as_ref().ok_or(anyhow!("Missing source"))?;
            let source_value = source.value.as_str();
            let source_type = io_maps::node_type_to_u64(source.ntype());
            let source_subtype = source.subtype.as_str();

            let target = relation.to.as_ref().ok_or(anyhow!("Missing target"))?;
            let target_value = target.value.as_str();
            let target_type = io_maps::node_type_to_u64(target.ntype());
            let target_subtype = target.subtype.as_str();

            let label = relation.relation_label.as_str();
            let relationship = io_maps::relation_type_to_u64(relation.relation());
            let normalized_source_value = schema.normalize(source_value);
            let normalized_target_value = schema.normalize(target_value);

            let mut new_doc = doc!(
                schema.source_value => source_value,
                schema.source_type => source_type,
                schema.source_subtype => source_subtype,
                schema.target_value => target_value,
                schema.target_type => target_type,
                schema.target_subtype => target_subtype,
                schema.relationship => relationship,
                schema.label => label,
                schema.normalized_source_value => normalized_source_value,
                schema.normalized_target_value => normalized_target_value,
                schema.resource_id => rid.as_bytes().to_vec(),
                schema.resource_field_id => encode_field_id(rid, field_key),

            );

            for facet in &index_relation.facets {
                new_doc.add_facet(schema.facets, Facet::from_text(facet)?);
            }

            // Encode source and target nodes and relation for faster retrieval
            for b in encode_node(source_value, source_type, source_subtype) {
                new_doc.add_u64(schema.encoded_source_id, b);
            }
            for b in encode_node(target_value, target_type, target_subtype) {
                new_doc.add_u64(schema.encoded_target_id, b);
            }
            for b in encode_relation(relationship, label) {
                new_doc.add_u64(schema.encoded_relation_id, b);
            }

            if let Some(metadata) = relation.metadata.as_ref() {
                let encoded_metadata = metadata.encode_to_vec();
                new_doc.add_bytes(schema.metadata, &encoded_metadata);
            }

            writer.add_document(new_doc)?;
        }
    }
    Ok(())
}
