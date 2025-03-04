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

use nidx_protos::relation_node::NodeType;
use nidx_protos::{Resource, ResourceId};
use nidx_relation::graph_query_parser::{
    Expression, FuzzyTerm, GraphQuery, Node, NodeQuery, PathQuery, Relation, RelationQuery, Term,
};
use nidx_relation::{RelationIndexer, RelationSearcher};
use nidx_tests::graph::friendly_parse;
use nidx_tests::graph::friendly_print;
use tempfile::TempDir;

use common::TestOpener;

#[test]
fn test_graph_node_query() -> anyhow::Result<()> {
    let reader = create_reader()?;

    // (s)-[]->()
    let result = reader
        .reader
        .inner_graph_search(GraphQuery::NodeQuery(NodeQuery::SourceNode(Expression::Value(Node {
            value: None,
            node_type: None,
            node_subtype: None,
            ..Default::default()
        }))))?;
    assert_eq!(result.graph.len(), 16);

    // (:PERSON)-[]->()
    let result = reader
        .reader
        .inner_graph_search(GraphQuery::NodeQuery(NodeQuery::SourceNode(Expression::Value(Node {
            value: None,
            node_type: Some(NodeType::Entity),
            node_subtype: Some("PERSON".to_string()),
            ..Default::default()
        }))))?;
    assert_eq!(result.graph.len(), 12);

    // (:Anna)-[]->()
    let result = reader
        .reader
        .inner_graph_search(GraphQuery::NodeQuery(NodeQuery::SourceNode(Expression::Value(Node {
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
    let result = reader
        .reader
        .inner_graph_search(GraphQuery::NodeQuery(NodeQuery::DestinationNode(Expression::Value(
            Node {
                value: Some("Anna".into()),
                node_type: Some(NodeType::Entity),
                node_subtype: Some("PERSON".to_string()),
                ..Default::default()
            },
        ))))?;
    let relations = friendly_parse(&result);
    assert_eq!(relations.len(), 1);
    assert!(relations.contains(&("Anastasia", "IS_FRIEND", "Anna")));

    // (:Anna)
    let result = reader
        .reader
        .inner_graph_search(GraphQuery::NodeQuery(NodeQuery::Node(Expression::Value(Node {
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
    let result = reader
        .reader
        .inner_graph_search(GraphQuery::NodeQuery(NodeQuery::SourceNode(Expression::Value(Node {
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
    let result = reader
        .reader
        .inner_graph_search(GraphQuery::NodeQuery(NodeQuery::SourceNode(Expression::Value(Node {
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
    let result = reader
        .reader
        .inner_graph_search(GraphQuery::NodeQuery(NodeQuery::SourceNode(Expression::Value(Node {
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
    let result = reader
        .reader
        .inner_graph_search(GraphQuery::NodeQuery(NodeQuery::SourceNode(Expression::Value(Node {
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
    let result = reader
        .reader
        .inner_graph_search(GraphQuery::NodeQuery(NodeQuery::SourceNode(Expression::Value(Node {
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
    let result = reader
        .reader
        .inner_graph_search(GraphQuery::RelationQuery(RelationQuery(Expression::Value(Relation {
            value: Some("LIVE_IN".to_string()),
        }))))?;
    let relations = friendly_parse(&result);
    assert_eq!(relations.len(), 2);
    assert!(relations.contains(&("Anna", "LIVE_IN", "New York")));
    assert!(relations.contains(&("Peter", "LIVE_IN", "New York")));

    // ()-[:LIVE_IN | BORN_IN]->()
    let result = reader
        .reader
        .inner_graph_search(GraphQuery::RelationQuery(RelationQuery(Expression::Or(vec![
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
    let result = reader
        .reader
        .inner_graph_search(GraphQuery::RelationQuery(RelationQuery(Expression::Not(Relation {
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
    let result = reader
        .reader
        .inner_graph_search(GraphQuery::PathQuery(PathQuery::DirectedPath((
            Expression::Value(Node {
                value: Some("Erin".into()),
                node_type: Some(NodeType::Entity),
                node_subtype: Some("PERSON".to_string()),
            }),
            Expression::Value(Relation { value: None }),
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
    let result = reader
        .reader
        .inner_graph_search(GraphQuery::PathQuery(PathQuery::DirectedPath((
            Expression::Value(Node {
                value: None,
                node_type: Some(NodeType::Entity),
                node_subtype: Some("PERSON".to_string()),
                ..Default::default()
            }),
            Expression::Value(Relation { value: None }),
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
    let result = reader
        .reader
        .inner_graph_search(GraphQuery::PathQuery(PathQuery::DirectedPath((
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
    let result = reader
        .reader
        .inner_graph_search(GraphQuery::PathQuery(PathQuery::DirectedPath((
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
    let result = reader
        .reader
        .inner_graph_search(GraphQuery::PathQuery(PathQuery::UndirectedPath((
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

    // ()-[:LIVE_IN]->()
    let result = reader
        .reader
        .inner_graph_search(GraphQuery::RelationQuery(RelationQuery(Expression::Or(vec![
            Relation {
                value: Some("LIVE_IN".to_string()),
            },
            Relation {
                value: Some("WORK_IN".to_string()),
            },
        ]))))?;
    friendly_print(&result);
    let relations = friendly_parse(&result);
    assert_eq!(relations.len(), 4);
    assert!(relations.contains(&("Anna", "LIVE_IN", "New York")));
    assert!(relations.contains(&("Anna", "WORK_IN", "New York")));
    assert!(relations.contains(&("Margaret", "WORK_IN", "Computer science")));
    assert!(relations.contains(&("Peter", "LIVE_IN", "New York")));

    // Validate we are compressing the graph and retuning deduplicated nodes
    assert_eq!(result.graph.len(), 4);
    assert_eq!(result.nodes.len(), 8); // this could be 5 with dedup
    assert_eq!(result.relations.len(), 4); // this could be 2 with dedup

    Ok(())
}

fn create_reader() -> anyhow::Result<RelationSearcher> {
    let dir = TempDir::new().unwrap();

    let relations = nidx_tests::graph::knowledge_graph_as_relations();
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
