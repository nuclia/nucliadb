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
use nidx_protos::{IndexRelations, Resource, ResourceId};
use nidx_relation::graph_query_parser::{
    Expression, FuzzyTerm, GraphQuery, GraphQueryParser, Node, NodeQuery, PathQuery, Relation, RelationQuery, Term,
};
use nidx_relation::{RelationConfig, RelationIndexer, RelationSchema, RelationSearcher};
use tantivy::TantivyDocument;
use tantivy::collector::TopDocs;
use tempfile::TempDir;

use common::TestOpener;

struct SearchResult {
    docs: Vec<TantivyDocument>,
    schema: RelationSchema,
}

impl SearchResult {
    fn relations(&self) -> Vec<(&str, &str, &str)> {
        self.docs
            .iter()
            .map(|doc| {
                (
                    self.schema.source_value(doc),
                    self.schema.relationship_label(doc),
                    self.schema.target_value(doc),
                )
            })
            .collect()
    }

    fn len(&self) -> usize {
        self.docs.len()
    }
}

fn inner_graph_search(reader: &RelationSearcher, query: GraphQuery) -> anyhow::Result<SearchResult> {
    let schema = &reader.reader.schema;
    let parser = GraphQueryParser::new(schema);
    let index_query = parser.parse(query);

    let collector = TopDocs::with_limit(1000_usize);

    let searcher = reader.reader.reader.searcher();
    let matching_docs = searcher.search(&index_query, &collector)?;

    Ok(SearchResult {
        docs: matching_docs
            .into_iter()
            .map(|(_, docaddr)| searcher.doc(docaddr).unwrap())
            .collect(),
        schema: schema.clone(),
    })
}

#[test]
fn test_graph_node_query() -> anyhow::Result<()> {
    let reader = create_reader()?;

    // (s)-[]->()
    let result = inner_graph_search(
        &reader,
        GraphQuery::NodeQuery(NodeQuery::SourceNode(Expression::Value(Node {
            value: None,
            node_type: None,
            node_subtype: None,
        }))),
    )?;
    assert_eq!(result.len(), 17);

    // (:PERSON)-[]->()
    let result = inner_graph_search(
        &reader,
        GraphQuery::NodeQuery(NodeQuery::SourceNode(Expression::Value(Node {
            value: None,
            node_type: Some(NodeType::Entity),
            node_subtype: Some("PERSON".to_string()),
        }))),
    )?;
    assert_eq!(result.len(), 12);

    // (:Anna)-[]->()
    let result = inner_graph_search(
        &reader,
        GraphQuery::NodeQuery(NodeQuery::SourceNode(Expression::Value(Node {
            value: Some("Anna".into()),
            node_type: Some(NodeType::Entity),
            node_subtype: Some("PERSON".to_string()),
        }))),
    )?;
    assert_eq!(result.len(), 4);
    let relations = result.relations();
    assert!(relations.contains(&("Anna", "FOLLOW", "Erin")));
    assert!(relations.contains(&("Anna", "LIVE_IN", "New York")));
    assert!(relations.contains(&("Anna", "LOVE", "Cat")));
    assert!(relations.contains(&("Anna", "WORK_IN", "New York")));

    // ()-[]->(:Anna)
    let result = inner_graph_search(
        &reader,
        GraphQuery::NodeQuery(NodeQuery::DestinationNode(Expression::Value(Node {
            value: Some("Anna".into()),
            node_type: Some(NodeType::Entity),
            node_subtype: Some("PERSON".to_string()),
        }))),
    )?;
    let relations = result.relations();
    assert_eq!(relations.len(), 1);
    assert!(relations.contains(&("Anastasia", "IS_FRIEND", "Anna")));

    // (:Anna)
    let result = inner_graph_search(
        &reader,
        GraphQuery::NodeQuery(NodeQuery::Node(Expression::Value(Node {
            value: Some("Anna".into()),
            node_type: Some(NodeType::Entity),
            node_subtype: Some("PERSON".to_string()),
        }))),
    )?;
    let relations = result.relations();
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
    let result = inner_graph_search(
        &reader,
        GraphQuery::NodeQuery(NodeQuery::SourceNode(Expression::Value(Node {
            value: Some(Term::Fuzzy(FuzzyTerm {
                value: "Anastas".to_string(),
                fuzzy_distance: 2,
                is_prefix: false,
            })),
            ..Default::default()
        }))),
    )?;
    let relations = result.relations();
    assert_eq!(relations.len(), 1);
    assert!(relations.contains(&("Anastasia", "IS_FRIEND", "Anna")));

    // (:~AnXstXsia) with fuzzy=1
    let result = inner_graph_search(
        &reader,
        GraphQuery::NodeQuery(NodeQuery::SourceNode(Expression::Value(Node {
            value: Some(Term::Fuzzy(FuzzyTerm {
                value: "AnXstXsia".to_string(),
                fuzzy_distance: 1,
                is_prefix: false,
            })),
            ..Default::default()
        }))),
    )?;
    let relations = result.relations();
    assert_eq!(relations.len(), 0);

    // (:~AnXstXsia) with fuzzy=2
    let result = inner_graph_search(
        &reader,
        GraphQuery::NodeQuery(NodeQuery::SourceNode(Expression::Value(Node {
            value: Some(Term::Fuzzy(FuzzyTerm {
                value: "AnXstXsia".to_string(),
                fuzzy_distance: 2,
                is_prefix: false,
            })),
            ..Default::default()
        }))),
    )?;
    let relations = result.relations();
    assert_eq!(relations.len(), 1);
    assert!(relations.contains(&("Anastasia", "IS_FRIEND", "Anna")));

    // (:^Ana)
    let result = inner_graph_search(
        &reader,
        GraphQuery::NodeQuery(NodeQuery::SourceNode(Expression::Value(Node {
            value: Some(Term::Fuzzy(FuzzyTerm {
                value: "Anas".to_string(),
                fuzzy_distance: 0,
                is_prefix: true,
            })),
            ..Default::default()
        }))),
    )?;
    let relations = result.relations();
    assert_eq!(relations.len(), 1);
    assert!(relations.contains(&("Anastasia", "IS_FRIEND", "Anna")));

    // (:^~Anas)
    let result = inner_graph_search(
        &reader,
        GraphQuery::NodeQuery(NodeQuery::SourceNode(Expression::Value(Node {
            value: Some(Term::Fuzzy(FuzzyTerm {
                value: "Anas".to_string(),
                fuzzy_distance: 2,
                is_prefix: true,
            })),
            ..Default::default()
        }))),
    )?;
    let relations = result.relations();
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
    let result = inner_graph_search(
        &reader,
        GraphQuery::RelationQuery(RelationQuery(Expression::Value(Relation {
            value: Some("LIVE_IN".to_string()),
            ..Default::default()
        }))),
    )?;
    let relations = result.relations();
    assert_eq!(relations.len(), 2);
    assert!(relations.contains(&("Anna", "LIVE_IN", "New York")));
    assert!(relations.contains(&("Peter", "LIVE_IN", "New York")));

    // ()-[:Synonym]->()
    let result = inner_graph_search(
        &reader,
        GraphQuery::RelationQuery(RelationQuery(Expression::Value(Relation {
            relation_type: Some(RelationType::Synonym),
            ..Default::default()
        }))),
    )?;
    let relations = result.relations();
    assert_eq!(relations.len(), 1);
    assert!(relations.contains(&("Mr. P", "ALIAS", "Peter")));

    // Synonym relations with nonexistent label
    let result = inner_graph_search(
        &reader,
        GraphQuery::RelationQuery(RelationQuery(Expression::Value(Relation {
            value: Some("FAKE".to_string()),
            relation_type: Some(RelationType::Synonym),
        }))),
    )?;
    let relations = result.relations();
    assert_eq!(relations.len(), 0);

    // ()-[:LIVE_IN | BORN_IN]->()
    let result = inner_graph_search(
        &reader,
        GraphQuery::RelationQuery(RelationQuery(Expression::Or(vec![
            Relation {
                value: Some("BORN_IN".to_string()),
                ..Default::default()
            },
            Relation {
                value: Some("LIVE_IN".to_string()),
                ..Default::default()
            },
        ]))),
    )?;
    let relations = result.relations();
    assert_eq!(relations.len(), 3);
    assert!(relations.contains(&("Anna", "LIVE_IN", "New York")));
    assert!(relations.contains(&("Erin", "BORN_IN", "UK")));
    assert!(relations.contains(&("Peter", "LIVE_IN", "New York")));

    // ()-[:!LIVE_IN]->()
    let result = inner_graph_search(
        &reader,
        GraphQuery::RelationQuery(RelationQuery(Expression::Not(Relation {
            value: Some("LIVE_IN".to_string()),
            ..Default::default()
        }))),
    )?;
    let relations = result.relations();
    assert_eq!(relations.len(), 15);

    Ok(())
}

#[test]
fn test_graph_directed_path_query() -> anyhow::Result<()> {
    let reader = create_reader()?;

    // (:Erin)-[]->(:UK)
    let result = inner_graph_search(
        &reader,
        GraphQuery::PathQuery(PathQuery::DirectedPath((
            Expression::Value(Node {
                value: Some("Erin".into()),
                node_type: Some(NodeType::Entity),
                node_subtype: Some("PERSON".to_string()),
            }),
            Expression::Value(Relation {
                value: None,
                ..Default::default()
            }),
            Expression::Value(Node {
                value: Some("UK".into()),
                node_type: Some(NodeType::Entity),
                node_subtype: Some("PLACE".to_string()),
            }),
        ))),
    )?;
    let relations = result.relations();
    assert_eq!(relations.len(), 1);
    assert!(relations.contains(&("Erin", "BORN_IN", "UK")));

    // (:PERSON)-[]->(:PLACE)
    let result = inner_graph_search(
        &reader,
        GraphQuery::PathQuery(PathQuery::DirectedPath((
            Expression::Value(Node {
                value: None,
                node_type: Some(NodeType::Entity),
                node_subtype: Some("PERSON".to_string()),
            }),
            Expression::Value(Relation {
                value: None,
                ..Default::default()
            }),
            Expression::Value(Node {
                value: None,
                node_type: None,
                node_subtype: Some("PLACE".to_string()),
            }),
        ))),
    )?;
    let relations = result.relations();
    assert_eq!(relations.len(), 4);
    assert!(relations.contains(&("Anna", "LIVE_IN", "New York")));
    assert!(relations.contains(&("Anna", "WORK_IN", "New York")));
    assert!(relations.contains(&("Erin", "BORN_IN", "UK")));
    assert!(relations.contains(&("Peter", "LIVE_IN", "New York")));

    // (:PERSON)-[:LIVE_IN]->(:PLACE)
    let result = inner_graph_search(
        &reader,
        GraphQuery::PathQuery(PathQuery::DirectedPath((
            Expression::Value(Node {
                value: None,
                node_type: None,
                node_subtype: Some("PERSON".to_string()),
            }),
            Expression::Value(Relation {
                value: Some("LIVE_IN".to_string()),
                ..Default::default()
            }),
            Expression::Value(Node {
                value: None,
                node_type: None,
                node_subtype: Some("PLACE".to_string()),
            }),
        ))),
    )?;
    let relations = result.relations();
    assert_eq!(relations.len(), 2);
    assert!(relations.contains(&("Anna", "LIVE_IN", "New York")));
    assert!(relations.contains(&("Peter", "LIVE_IN", "New York")));

    // (:!Anna)-[:LIVE_IN|LOVE]->()
    let result = inner_graph_search(
        &reader,
        GraphQuery::PathQuery(PathQuery::DirectedPath((
            Expression::Not(Node {
                value: Some("Anna".into()),
                node_type: Some(NodeType::Entity),
                node_subtype: Some("PERSON".to_string()),
            }),
            Expression::Or(vec![
                Relation {
                    value: Some("LIVE_IN".to_string()),
                    ..Default::default()
                },
                Relation {
                    value: Some("LOVE".to_string()),
                    ..Default::default()
                },
            ]),
            Expression::Value(Node {
                value: None,
                node_type: None,
                node_subtype: None,
            }),
        ))),
    )?;
    let relations = result.relations();
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
    let result = inner_graph_search(
        &reader,
        GraphQuery::PathQuery(PathQuery::UndirectedPath((
            Expression::Value(Node {
                value: Some("Anna".into()),
                node_type: Some(NodeType::Entity),
                node_subtype: Some("PERSON".to_string()),
            }),
            Expression::Value(Relation {
                value: Some("IS_FRIEND".to_string()),
                ..Default::default()
            }),
            Expression::Value(Node::default()),
        ))),
    )?;
    let relations = result.relations();
    assert_eq!(relations.len(), 1);
    assert!(relations.contains(&("Anastasia", "IS_FRIEND", "Anna")));

    Ok(())
}

fn create_reader() -> anyhow::Result<RelationSearcher> {
    let dir = TempDir::new().unwrap();

    let relations = nidx_tests::graph::knowledge_graph_as_relations();
    let field_relations = HashMap::from([("a/metadata".to_string(), IndexRelations { relations })]);
    let resource = Resource {
        resource: Some(ResourceId {
            uuid: "0123456789abcdef0123456789abcdef".to_string(),
            shard_id: "shard_id".to_string(),
        }),
        field_relations,
        ..Default::default()
    };

    let segment_meta = RelationIndexer
        .index_resource(dir.path(), &RelationConfig::default(), &resource)
        .unwrap()
        .unwrap();
    RelationSearcher::open(
        RelationConfig::default(),
        TestOpener::new(vec![(segment_meta, 1i64.into())], vec![]),
    )
}
