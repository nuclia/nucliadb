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

use nucliadb_core::protos::relation::RelationType;
use nucliadb_core::protos::relation_node::NodeType;
use nucliadb_core::protos::{Relation, RelationMetadata, RelationNode};

pub fn create_relation_node(source: String, node_type: NodeType, subtype: String) -> RelationNode {
    RelationNode {
        subtype,
        value: source,
        ntype: node_type.into(),
    }
}

pub fn create_relation(
    source: String,
    source_node_type: NodeType,
    source_subtype: String,
    to: String,
    to_node_type: NodeType,
    to_subtype: String,
    rel_type: RelationType,
) -> Relation {
    Relation {
        source: Some(create_relation_node(
            source,
            source_node_type,
            source_subtype,
        )),
        to: Some(create_relation_node(to, to_node_type, to_subtype)),
        relation: rel_type.into(),
        relation_label: "relation_label".to_string(),
        metadata: Some(RelationMetadata {
            ..Default::default()
        }),
    }
}
