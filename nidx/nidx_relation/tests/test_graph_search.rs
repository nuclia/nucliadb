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
mod common;

use std::collections::HashMap;

use nidx_protos::relation::RelationType;
use nidx_protos::relation_node::NodeType;
use nidx_protos::{Resource, ResourceId};
use nidx_relation::graph_query_parser::{
    Expression, FuzzyTerm, GraphQuery, Node, NodeQuery, PathQuery, Relation, RelationQuery, Term,
};
use nidx_relation::{RelationIndexer, RelationSearcher};
use tempfile::TempDir;

use common::TestOpener;

fn create_reader() -> anyhow::Result<RelationSearcher> {
    let dir = TempDir::new().unwrap();

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
        ("Margaret", "PERSON"),
        ("Mouse", "ANIMAL"),
        ("New York", "PLACE"),
        ("Olympic athlete", "SPORT"),
        ("Peter", "PERSON"),
        ("Rocket", "VEHICLE"),
        ("Tom", "ANIMAL"),
        ("UK", "PLACE"),
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
        ("Peter", "LIVE_IN", "New York"),
        ("Tom", "CHASE", "Jerry"),
        ("Tom", "IS", "Cat"),
    ];

    let mut relations = vec![];
    for (source, relation, target) in graph {
        relations.push(common::create_relation(
            source.to_string(),
            NodeType::Entity,
            entities.get(source).unwrap().to_string(),
            target.to_string(),
            NodeType::Entity,
            entities.get(target).unwrap().to_string(),
            RelationType::Entity,
            relation.to_string(),
        ))
    }

    let resource = Resource {
        resource: Some(ResourceId {
            uuid: "uuid".to_string(),
            shard_id: "shard_id".to_string(),
        }),
        relations: relations,
        ..Default::default()
    };

    let segment_meta = RelationIndexer.index_resource(dir.path(), &resource).unwrap().unwrap();
    RelationSearcher::open(TestOpener::new(vec![(segment_meta, 1i64.into())], vec![]))
}

// Debug function, useful while developing
#[allow(dead_code)]
fn friendly_print(result: &nidx_protos::GraphSearchResponse) {
    println!("Matched {} paths in {} nodes and {}", result.graph.len(), result.nodes.len(), result.relations.len());
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
    println!();
}

fn friendly_parse<'a>(relations: &'a nidx_protos::GraphSearchResponse) -> Vec<(&'a str, &'a str, &'a str)> {
    relations
        .graph
        .iter()
        .map(|path| {
            let source = relations.nodes.get(path.source as usize).unwrap();
            let relation = relations.relations.get(path.relation as usize).unwrap();
            let destination = relations.nodes.get(path.destination as usize).unwrap();
            (source.value.as_str(), relation.label.as_str(), destination.value.as_str())
        })
        .collect()
}

#[test]
fn test_graph_node_query() -> anyhow::Result<()> {
    let reader = create_reader()?;

    // (s)-[]->()
    let result = reader.reader.graph_search(GraphQuery::NodeQuery(NodeQuery::SourceNode(Expression::Value(Node {
        value: None,
        node_type: None,
        node_subtype: None,
        ..Default::default()
    }))))?;
    assert_eq!(result.graph.len(), 16);

    // (:PERSON)-[]->()
    let result = reader.reader.graph_search(GraphQuery::NodeQuery(NodeQuery::SourceNode(Expression::Value(Node {
        value: None,
        node_type: Some(NodeType::Entity),
        node_subtype: Some("PERSON".to_string()),
        ..Default::default()
    }))))?;
    assert_eq!(result.graph.len(), 12);

    // (:Anna)-[]->()
    let result = reader.reader.graph_search(GraphQuery::NodeQuery(NodeQuery::SourceNode(Expression::Value(Node {
        value: Some("Anna".into()),
        node_type: Some(NodeType::Entity),
        node_subtype: Some("PERSON".to_string()),
        ..Default::default()
    }))))?;
    let relations = friendly_parse(&result);
    assert_eq!(relations.len(), 4);
    assert!(relations.contains(&("Anna", "FOLLOW", "Erin")));
    assert!(relations.contains(&("Anna", "LIVE_IN", "New York")));
    assert!(relations.contains(&("Anna", "LOVE", "Cat")));
    assert!(relations.contains(&("Anna", "WORK_IN", "New York")));

    // ()-[]->(:Anna)
    let result =
        reader.reader.graph_search(GraphQuery::NodeQuery(NodeQuery::DestinationNode(Expression::Value(Node {
            value: Some("Anna".into()),
            node_type: Some(NodeType::Entity),
            node_subtype: Some("PERSON".to_string()),
            ..Default::default()
        }))))?;
    let relations = friendly_parse(&result);
    assert_eq!(relations.len(), 1);
    assert!(relations.contains(&("Anastasia", "IS_FRIEND", "Anna")));

    // (:Anna)
    let result = reader.reader.graph_search(GraphQuery::NodeQuery(NodeQuery::Node(Expression::Value(Node {
        value: Some("Anna".into()),
        node_type: Some(NodeType::Entity),
        node_subtype: Some("PERSON".to_string()),
        ..Default::default()
    }))))?;
    let relations = friendly_parse(&result);
    assert_eq!(relations.len(), 5);
    assert!(relations.contains(&("Anna", "FOLLOW", "Erin")));
    assert!(relations.contains(&("Anna", "LIVE_IN", "New York")));
    assert!(relations.contains(&("Anna", "LOVE", "Cat")));
    assert!(relations.contains(&("Anna", "WORK_IN", "New York")));
    assert!(relations.contains(&("Anastasia", "IS_FRIEND", "Anna")));

    Ok(())
}

#[test]
fn test_graph_fuzzy_node_query() -> anyhow::Result<()> {
    let reader = create_reader()?;

    // (:~Anastas)
    let result = reader.reader.graph_search(GraphQuery::NodeQuery(NodeQuery::SourceNode(Expression::Value(Node {
        value: Some(Term::Fuzzy(FuzzyTerm {
            value: "Anastas".to_string(),
            fuzzy_distance: 2,
            is_prefix: false,
        })),
        ..Default::default()
    }))))?;
    let relations = friendly_parse(&result);
    assert_eq!(relations.len(), 1);
    assert!(relations.contains(&("Anastasia", "IS_FRIEND", "Anna")));

    // (:~AnXstXsia) with fuzzy=1
    let result = reader.reader.graph_search(GraphQuery::NodeQuery(NodeQuery::SourceNode(Expression::Value(Node {
        value: Some(Term::Fuzzy(FuzzyTerm {
            value: "AnXstXsia".to_string(),
            fuzzy_distance: 1,
            is_prefix: false,
        })),
        ..Default::default()
    }))))?;
    let relations = friendly_parse(&result);
    assert_eq!(relations.len(), 0);

    // (:~AnXstXsia) with fuzzy=2
    let result = reader.reader.graph_search(GraphQuery::NodeQuery(NodeQuery::SourceNode(Expression::Value(Node {
        value: Some(Term::Fuzzy(FuzzyTerm {
            value: "AnXstXsia".to_string(),
            fuzzy_distance: 2,
            is_prefix: false,
        })),
        ..Default::default()
    }))))?;
    let relations = friendly_parse(&result);
    assert_eq!(relations.len(), 1);
    assert!(relations.contains(&("Anastasia", "IS_FRIEND", "Anna")));

    // (:^Ana)
    let result = reader.reader.graph_search(GraphQuery::NodeQuery(NodeQuery::SourceNode(Expression::Value(Node {
        value: Some(Term::Fuzzy(FuzzyTerm {
            value: "Anas".to_string(),
            fuzzy_distance: 0,
            is_prefix: true,
        })),
        ..Default::default()
    }))))?;
    let relations = friendly_parse(&result);
    assert_eq!(relations.len(), 1);
    assert!(relations.contains(&("Anastasia", "IS_FRIEND", "Anna")));

    // (:^~Anas)
    let result = reader.reader.graph_search(GraphQuery::NodeQuery(NodeQuery::SourceNode(Expression::Value(Node {
        value: Some(Term::Fuzzy(FuzzyTerm {
            value: "Anas".to_string(),
            fuzzy_distance: 2,
            is_prefix: true,
        })),
        ..Default::default()
    }))))?;
    let relations = friendly_parse(&result);
    assert_eq!(relations.len(), 5);
    assert!(relations.contains(&("Anastasia", "IS_FRIEND", "Anna")));
    assert!(relations.contains(&("Anna", "FOLLOW", "Erin")));
    assert!(relations.contains(&("Anna", "LIVE_IN", "New York")));
    assert!(relations.contains(&("Anna", "LOVE", "Cat")));
    assert!(relations.contains(&("Anna", "WORK_IN", "New York")));

    Ok(())
}

#[test]
fn test_graph_relation_query() -> anyhow::Result<()> {
    let reader = create_reader()?;

    // ()-[:LIVE_IN]->()
    let result = reader.reader.graph_search(GraphQuery::RelationQuery(RelationQuery(Expression::Value(Relation {
        value: Some("LIVE_IN".to_string()),
    }))))?;
    let relations = friendly_parse(&result);
    assert_eq!(relations.len(), 2);
    assert!(relations.contains(&("Anna", "LIVE_IN", "New York")));
    assert!(relations.contains(&("Peter", "LIVE_IN", "New York")));

    // ()-[:LIVE_IN | BORN_IN]->()
    let result = reader.reader.graph_search(GraphQuery::RelationQuery(RelationQuery(Expression::Or(vec![
        Relation {
            value: Some("BORN_IN".to_string()),
        },
        Relation {
            value: Some("LIVE_IN".to_string()),
        },
    ]))))?;
    let relations = friendly_parse(&result);
    assert_eq!(relations.len(), 3);
    assert!(relations.contains(&("Anna", "LIVE_IN", "New York")));
    assert!(relations.contains(&("Erin", "BORN_IN", "UK")));
    assert!(relations.contains(&("Peter", "LIVE_IN", "New York")));

    // // ()-[:!LIVE_IN]->()
    let result = reader.reader.graph_search(GraphQuery::RelationQuery(RelationQuery(Expression::Not(Relation {
        value: Some("LIVE_IN".to_string()),
    }))))?;
    let relations = friendly_parse(&result);
    assert_eq!(relations.len(), 14);

    Ok(())
}

#[test]
fn test_graph_directed_path_query() -> anyhow::Result<()> {
    let reader = create_reader()?;

    // (:Erin)-[]->(:UK)
    let result = reader.reader.graph_search(GraphQuery::PathQuery(PathQuery::DirectedPath((
        Expression::Value(Node {
            value: Some("Erin".into()),
            node_type: Some(NodeType::Entity),
            node_subtype: Some("PERSON".to_string()),
        }),
        Expression::Value(Relation {
            value: None,
        }),
        Expression::Value(Node {
            value: Some("UK".into()),
            node_type: Some(NodeType::Entity),
            node_subtype: Some("PLACE".to_string()),
        }),
    ))))?;
    let relations = friendly_parse(&result);
    assert_eq!(relations.len(), 1);
    assert!(relations.contains(&("Erin", "BORN_IN", "UK")));

    // (:PERSON)-[]->(:PLACE)
    let result = reader.reader.graph_search(GraphQuery::PathQuery(PathQuery::DirectedPath((
        Expression::Value(Node {
            value: None,
            node_type: Some(NodeType::Entity),
            node_subtype: Some("PERSON".to_string()),
            ..Default::default()
        }),
        Expression::Value(Relation {
            value: None,
        }),
        Expression::Value(Node {
            value: None,
            node_type: None,
            node_subtype: Some("PLACE".to_string()),
            ..Default::default()
        }),
    ))))?;
    let relations = friendly_parse(&result);
    assert_eq!(relations.len(), 4);
    assert!(relations.contains(&("Anna", "LIVE_IN", "New York")));
    assert!(relations.contains(&("Anna", "WORK_IN", "New York")));
    assert!(relations.contains(&("Erin", "BORN_IN", "UK")));
    assert!(relations.contains(&("Peter", "LIVE_IN", "New York")));

    // (:PERSON)-[:LIVE_IN]->(:PLACE)
    let result = reader.reader.graph_search(GraphQuery::PathQuery(PathQuery::DirectedPath((
        Expression::Value(Node {
            value: None,
            node_type: None,
            node_subtype: Some("PERSON".to_string()),
            ..Default::default()
        }),
        Expression::Value(Relation {
            value: Some("LIVE_IN".to_string()),
        }),
        Expression::Value(Node {
            value: None,
            node_type: None,
            node_subtype: Some("PLACE".to_string()),
            ..Default::default()
        }),
    ))))?;
    let relations = friendly_parse(&result);
    assert_eq!(relations.len(), 2);
    assert!(relations.contains(&("Anna", "LIVE_IN", "New York")));
    assert!(relations.contains(&("Peter", "LIVE_IN", "New York")));

    // (:!Anna)-[:LIVE_IN|LOVE]->()
    let result = reader.reader.graph_search(GraphQuery::PathQuery(PathQuery::DirectedPath((
        Expression::Not(Node {
            value: Some("Anna".into()),
            node_type: Some(NodeType::Entity),
            node_subtype: Some("PERSON".to_string()),
            ..Default::default()
        }),
        Expression::Or(vec![
            Relation {
                value: Some("LIVE_IN".to_string()),
            },
            Relation {
                value: Some("LOVE".to_string()),
            },
        ]),
        Expression::Value(Node {
            value: None,
            node_type: None,
            node_subtype: None,
            ..Default::default()
        }),
    ))))?;
    let relations = friendly_parse(&result);
    assert_eq!(relations.len(), 3);
    assert!(relations.contains(&("Erin", "LOVE", "Climbing")));
    assert!(relations.contains(&("Dimitri", "LOVE", "Anastasia")));
    assert!(relations.contains(&("Peter", "LIVE_IN", "New York")));

    Ok(())
}

#[test]
fn test_graph_undirected_path_query() -> anyhow::Result<()> {
    let reader = create_reader()?;

    // (:Anna)-[:IS_FRIEND]-()
    let result = reader.reader.graph_search(GraphQuery::PathQuery(PathQuery::UndirectedPath((
        Expression::Value(Node {
            value: Some("Anna".into()),
            node_type: Some(NodeType::Entity),
            node_subtype: Some("PERSON".to_string()),
            ..Default::default()
        }),
        Expression::Value(Relation {
            value: Some("IS_FRIEND".to_string()),
        }),
        Expression::Value(Node::default()),
    ))))?;
    let relations = friendly_parse(&result);
    assert_eq!(relations.len(), 1);
    assert!(relations.contains(&("Anastasia", "IS_FRIEND", "Anna")));

    Ok(())
}

#[test]
fn test_graph_response() -> anyhow::Result<()> {
    let reader = create_reader()?;

    // (:Anna)-[]->()
    let result = reader.reader.graph_search(GraphQuery::NodeQuery(NodeQuery::SourceNode(Expression::Value(Node {
        value: Some("Anna".into()),
        node_type: Some(NodeType::Entity),
        node_subtype: Some("PERSON".to_string()),
        ..Default::default()
    }))))?;
    let relations = friendly_parse(&result);
    assert_eq!(relations.len(), 4);
    assert!(relations.contains(&("Anna", "FOLLOW", "Erin")));
    assert!(relations.contains(&("Anna", "LIVE_IN", "New York")));
    assert!(relations.contains(&("Anna", "LOVE", "Cat")));
    assert!(relations.contains(&("Anna", "WORK_IN", "New York")));
    // Validate we are compressing the graph and retuning deduplicated nodes
    assert_eq!(result.graph.len(), 4);
    assert_eq!(result.nodes.len(), 4);
    assert_eq!(result.relations.len(), 4);

    Ok(())
}
