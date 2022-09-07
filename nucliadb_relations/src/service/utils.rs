use nucliadb_protos::relation::RelationType;
use nucliadb_protos::relation_node::NodeType;
use nucliadb_service_interface::prelude::*;

use crate::graph::*;

pub fn rtype_parsing(rtype: RelationType, additional: &str) -> EdgeType {
    match rtype {
        RelationType::Entity => EdgeType::from("Entity"),
        RelationType::About => EdgeType::from("About"),
        RelationType::Child => EdgeType::from("Child"),
        RelationType::Colab => EdgeType::from("Colab"),
        RelationType::Synonym => EdgeType::from("Synonym"),
        RelationType::Other => EdgeType::from(additional),
    }
}

pub fn string_to_rtype(rtype: &str) -> (RelationType, String) {
    match rtype {
        "Entity" => (RelationType::Entity, String::new()),
        "About" => (RelationType::About, String::new()),
        "Child" => (RelationType::Child, String::new()),
        "Colab" => (RelationType::Colab, String::new()),
        "Synonym" => (RelationType::Synonym, String::new()),
        v => (RelationType::Other, v.to_string()),
    }
}

pub fn node_type_parsing(rtype: NodeType) -> String {
    match rtype {
        NodeType::Entity => String::from("Entity"),
        NodeType::Label => String::from("Label"),
        NodeType::Resource => String::from("Resource"),
        NodeType::User => String::from("User"),
    }
}

pub fn string_to_node_type(rtype: &str) -> NodeType {
    match rtype {
        "Entity" => NodeType::Entity,
        "Label" => NodeType::Label,
        "Resource" => NodeType::Resource,
        "User" => NodeType::User,
        v => panic!("Invalid node type {}", v),
    }
}
