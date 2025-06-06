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

use nidx_protos::prost::*;
use nidx_protos::relation::RelationType;
use nidx_protos::relation_node::NodeType;
use nidx_protos::{RelationMetadata, RelationNode};
use nidx_tantivy::utils::decode_facet;
use tantivy::TantivyDocument;
use tantivy::schema::Value;

use crate::schema::{Schema, decode_field_id};

pub fn decode_metadata(schema: &Schema, doc: &TantivyDocument) -> Option<RelationMetadata> {
    schema
        .metadata(doc)
        .map(RelationMetadata::decode)
        .map(|m| m.expect("Corrupted metadata stored in the index"))
}

pub fn relation_type_to_u64(relation: RelationType) -> u64 {
    match relation {
        RelationType::Child => 0,
        RelationType::About => 1,
        RelationType::Entity => 2,
        RelationType::Colab => 3,
        RelationType::Synonym => 4,
        RelationType::Other => 5,
    }
}

pub fn u64_to_relation_type<T>(raw_relation: u64) -> T
where
    RelationType: Into<T>,
{
    match raw_relation {
        0 => RelationType::Child.into(),
        1 => RelationType::About.into(),
        2 => RelationType::Entity.into(),
        3 => RelationType::Colab.into(),
        4 => RelationType::Synonym.into(),
        5 => RelationType::Other.into(),
        invalid => panic!("Invalid relation type {invalid}, stored data may be corrupted"),
    }
}

pub fn node_type_to_u64(node_type: NodeType) -> u64 {
    match node_type {
        NodeType::Entity => 0,
        NodeType::Label => 1,
        NodeType::Resource => 2,
        NodeType::User => 3,
    }
}

pub fn u64_to_node_type<T>(raw_node_type: u64) -> T
where
    NodeType: Into<T>,
{
    match raw_node_type {
        0 => NodeType::Entity.into(),
        1 => NodeType::Label.into(),
        2 => NodeType::Resource.into(),
        3 => NodeType::User.into(),
        invalid => panic!("Invalid node type {invalid}, stored data may be corrupted"),
    }
}

pub fn source_to_relation_node(schema: &Schema, doc: &TantivyDocument) -> RelationNode {
    RelationNode {
        value: schema.source_value(doc).to_string(),
        ntype: u64_to_node_type::<i32>(schema.source_type(doc)),
        subtype: schema.source_subtype(doc).to_string(),
    }
}

pub fn target_to_relation_node(schema: &Schema, doc: &TantivyDocument) -> RelationNode {
    RelationNode {
        value: schema.target_value(doc).to_string(),
        ntype: u64_to_node_type::<i32>(schema.target_type(doc)),
        subtype: schema.target_subtype(doc).to_string(),
    }
}

pub fn doc_to_resource_field_id(schema: &Schema, doc: &TantivyDocument) -> Option<String> {
    doc.get_first(schema.resource_field_id).map(|v| {
        let (rid, fid) = decode_field_id(v.as_bytes().unwrap());
        format!("{}/{}", rid.simple(), fid)
    })
}

pub fn doc_to_facets(schema: &Schema, doc: &TantivyDocument) -> Vec<String> {
    doc.get_all(schema.facets)
        .map(|f| decode_facet(f.as_facet().unwrap()).to_path_string())
        .collect()
}

pub fn doc_to_graph_relation(schema: &Schema, doc: &TantivyDocument) -> nidx_protos::graph_search_response::Relation {
    nidx_protos::graph_search_response::Relation {
        relation_type: u64_to_relation_type::<i32>(schema.relationship(doc)),
        label: schema.relationship_label(doc).to_string(),
    }
}
