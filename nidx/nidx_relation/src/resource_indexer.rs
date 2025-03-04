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
use crate::schema::{Schema, normalize};
use nidx_protos::prost::*;
use nidx_tantivy::TantivyIndexer;
use tantivy::doc;

pub fn index_relations(
    writer: &mut TantivyIndexer,
    resource: &nidx_protos::Resource,
    schema: Schema,
) -> anyhow::Result<()> {
    let resource_id = resource
        .resource
        .as_ref()
        .map(|r| r.uuid.as_str())
        .expect("Missing resource ID");

    let iter = resource
        .relations
        .iter()
        .filter(|rel| rel.to.is_some() || rel.source.is_some());

    for relation in iter {
        let source = relation.source.as_ref().expect("Missing source");
        let source_value = source.value.as_str();
        let source_type = io_maps::node_type_to_u64(source.ntype());
        let source_subtype = source.subtype.as_str();

        let target = relation.to.as_ref().expect("Missing target");
        let target_value = target.value.as_str();
        let target_type = io_maps::node_type_to_u64(target.ntype());
        let target_subtype = target.subtype.as_str();

        let label = relation.relation_label.as_str();
        let relationship = io_maps::relation_type_to_u64(relation.relation());
        let normalized_source_value = normalize(source_value);
        let normalized_target_value = normalize(target_value);

        let mut new_doc = doc!(
            schema.normalized_source_value => normalized_source_value,
            schema.normalized_target_value => normalized_target_value,
            schema.resource_id => resource_id,
            schema.source_value => source_value,
            schema.source_type => source_type,
            schema.source_subtype => source_subtype,
            schema.target_value => target_value,
            schema.target_type => target_type,
            schema.target_subtype => target_subtype,
            schema.relationship => relationship,
            schema.label => label,
        );

        if let Some(metadata) = relation.metadata.as_ref() {
            let encoded_metadata = metadata.encode_to_vec();
            new_doc.add_bytes(schema.metadata, encoded_metadata);
        }

        writer.add_document(new_doc)?;
    }
    Ok(())
}
