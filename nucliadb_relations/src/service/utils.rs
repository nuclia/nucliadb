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

pub mod dictionary {
    pub const ENTITY: &str = "Entity";
    pub const ABOUT: &str = "About";
    pub const CHILD: &str = "Child";
    pub const COLAB: &str = "Colab";
    pub const SYNONYM: &str = "Synonym";
    pub const OTHER: &str = "Other";
    pub const RESOURCE: &str = "Resource";
    pub const USER: &str = "User";
    pub const LABEL: &str = "Label";
}

pub fn relation_type_parsing(rtype: RelationType, subtype: &str) -> (&str, Option<&str>) {
    let subtype = if subtype.is_empty() {
        None
    } else {
        Some(subtype)
    };
    let xtype = match rtype {
        RelationType::Entity => dictionary::ENTITY,
        RelationType::About => dictionary::ABOUT,
        RelationType::Child => dictionary::CHILD,
        RelationType::Colab => dictionary::COLAB,
        RelationType::Synonym => dictionary::SYNONYM,
        RelationType::Other => dictionary::OTHER,
    };
    (xtype, subtype)
}

pub fn string_to_rtype(rtype: &str) -> RelationType {
    match rtype {
        dictionary::ENTITY => RelationType::Entity,
        dictionary::ABOUT => RelationType::About,
        dictionary::CHILD => RelationType::Child,
        dictionary::COLAB => RelationType::Colab,
        dictionary::SYNONYM => RelationType::Synonym,
        dictionary::OTHER => RelationType::Other,
        v => unreachable!("unknown type {v}"),
    }
}

pub fn node_type_parsing(rtype: NodeType, subtype: &str) -> (&'static str, Option<&str>) {
    let subtype = if subtype.is_empty() {
        None
    } else {
        Some(subtype)
    };
    let xtype = match rtype {
        NodeType::Entity => dictionary::ENTITY,
        NodeType::Label => dictionary::LABEL,
        NodeType::Resource => dictionary::RESOURCE,
        NodeType::User => dictionary::USER,
    };
    (xtype, subtype)
}

pub fn string_to_node_type(rtype: &str) -> NodeType {
    match rtype {
        dictionary::ENTITY => NodeType::Entity,
        dictionary::LABEL => NodeType::Label,
        dictionary::RESOURCE => NodeType::Resource,
        dictionary::USER => NodeType::User,
        v => panic!("Invalid node type {}", v),
    }
}
