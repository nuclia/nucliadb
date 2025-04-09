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

use std::collections::HashMap;

use nidx_protos::{
    GraphSearchResponse, IndexRelation, Relation, RelationNode, relation::RelationType, relation_node::NodeType,
};

/// Parse a graph search response and return a list of triplets (source,
/// relation, target). This is a simplified view but yet useful view of the
/// response.
pub fn friendly_parse(relations: &GraphSearchResponse) -> Vec<(&str, &str, &str)> {
    relations
        .graph
        .iter()
        .map(|path| {
            let source = relations.nodes.get(path.source as usize).unwrap();
            let relation = relations.relations.get(path.relation as usize).unwrap();
            let destination = relations.nodes.get(path.destination as usize).unwrap();
            (
                source.value.as_str(),
                relation.label.as_str(),
                destination.value.as_str(),
            )
        })
        .collect()
}

/// Debug function useful while developing. Prints information and all paths
/// found in the graph response
pub fn friendly_print(result: &nidx_protos::GraphSearchResponse) {
    for path in result.graph.iter() {
        let source = result.nodes.get(path.source as usize).unwrap();
        let relation = result.relations.get(path.relation as usize).unwrap();
        let destination = result.nodes.get(path.destination as usize).unwrap();

        println!(
            "({:?})-[{:?}]->({:?})",
            (&source.value, &source.subtype),
            &relation.label,
            (&destination.value, &destination.subtype)
        );
    }
    println!(
        "Matched {} paths in {} nodes and {} relations",
        result.graph.len(),
        result.nodes.len(),
        result.relations.len()
    );
    println!();
}

/// Returns a simple knowledge graph as a list of relations
pub fn knowledge_graph_as_relations() -> Vec<IndexRelation> {
    let entities = HashMap::from([
        ("Anastasia", "PERSON"),
        ("Anna", "PERSON"),
        ("Apollo", "PROJECT"),
        ("Cat", "ANIMAL"),
        ("Climbing", "ACTIVITY"),
        ("Computer science", "STUDY_FIELD"),
        ("Dimitri", "PERSON"),
        ("Erin", "PERSON"),
        ("Jerry", "ANIMAL"),
        ("Mr. P", "AGENT"),
        ("Margaret", "PERSON"),
        ("Mouse", "ANIMAL"),
        ("New York", "PLACE"),
        ("Olympic athlete", "SPORT"),
        ("Peter", "PERSON"),
        ("Rocket", "VEHICLE"),
        ("Tom", "ANIMAL"),
        ("UK", "PLACE"),
    ]);

    let relations = HashMap::from([
        ("ALIAS", RelationType::Synonym),
        ("BORN_IN", RelationType::Entity),
        ("CHASE", RelationType::Entity),
        ("DEVELOPED", RelationType::Entity),
        ("FOLLOW", RelationType::Entity),
        ("IS", RelationType::Entity),
        ("IS_FRIEND", RelationType::Entity),
        ("LIVE_IN", RelationType::Entity),
        ("LOVE", RelationType::Entity),
        ("WORK_IN", RelationType::Entity),
    ]);

    let graph = vec![
        ("Anastasia", "IS_FRIEND", "Anna"),
        ("Anna", "FOLLOW", "Erin"),
        ("Anna", "LIVE_IN", "New York"),
        ("Anna", "WORK_IN", "New York"),
        ("Anna", "LOVE", "Cat"),
        ("Apollo", "IS", "Rocket"),
        ("Dimitri", "LOVE", "Anastasia"),
        ("Erin", "BORN_IN", "UK"),
        ("Erin", "IS", "Olympic athlete"),
        ("Erin", "LOVE", "Climbing"),
        ("Jerry", "IS", "Mouse"),
        ("Margaret", "DEVELOPED", "Apollo"),
        ("Margaret", "WORK_IN", "Computer science"),
        ("Mr. P", "ALIAS", "Peter"),
        ("Peter", "LIVE_IN", "New York"),
        ("Tom", "CHASE", "Jerry"),
        ("Tom", "IS", "Cat"),
    ];

    let mut pb_relations = vec![];
    for (source, relation, target) in graph {
        pb_relations.push(IndexRelation {
            relation: Some(Relation {
                source: Some(RelationNode {
                    value: source.to_string(),
                    ntype: NodeType::Entity as i32,
                    subtype: entities.get(source).unwrap().to_string(),
                }),
                relation: *relations.get(relation).unwrap() as i32,
                relation_label: relation.to_string(),
                to: Some(RelationNode {
                    value: target.to_string(),
                    ntype: NodeType::Entity as i32,
                    subtype: entities.get(target).unwrap().to_string(),
                }),
                metadata: None,
            }),
            ..Default::default()
        })
    }

    pb_relations
}
