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

use std::collections::{HashMap, HashSet};

use nidx_protos::graph_query::node::{ExactMatch, FuzzyMatch, NewMatchKind};
use nidx_protos::graph_query::node::{MatchKind, MatchLocation};
use nidx_protos::graph_query::path_query::Query;
use nidx_protos::graph_query::{BoolQuery, FacetFilter, Node, Path, PathQuery, Relation};
use nidx_protos::graph_search_request::QueryKind;
use nidx_protos::relation::RelationType;
use nidx_protos::relation_node::NodeType;
use nidx_protos::{
    GraphQuery, GraphSearchRequest, GraphSearchResponse, IndexRelation, IndexRelations, RelationNode, Resource,
    ResourceId,
};
use nidx_relation::{RelationConfig, RelationIndexer, RelationSearcher};
use nidx_tests::graph::friendly_parse;
use nidx_types::prefilter::{FieldId, PrefilterResult};
use tempfile::TempDir;

use common::TestOpener;
use uuid::Uuid;

fn search(reader: &RelationSearcher, query: Query) -> anyhow::Result<GraphSearchResponse> {
    _search(reader, query, QueryKind::Path, PrefilterResult::All)
}

fn search_nodes(reader: &RelationSearcher, query: Query) -> anyhow::Result<GraphSearchResponse> {
    _search(reader, query, QueryKind::Nodes, PrefilterResult::All)
}

fn search_relations(reader: &RelationSearcher, query: Query) -> anyhow::Result<GraphSearchResponse> {
    _search(reader, query, QueryKind::Relations, PrefilterResult::All)
}

fn search_with_prefilter(
    reader: &RelationSearcher,
    query: Query,
    prefilter: PrefilterResult,
) -> anyhow::Result<GraphSearchResponse> {
    _search(reader, query, QueryKind::Path, prefilter)
}

fn _search(
    reader: &RelationSearcher,
    query: Query,
    kind: QueryKind,
    prefilter: PrefilterResult,
) -> anyhow::Result<GraphSearchResponse> {
    reader.graph_search(
        &GraphSearchRequest {
            query: Some(GraphQuery {
                path: Some(PathQuery { query: Some(query) }),
            }),
            top_k: 100,
            kind: kind.into(),
            ..Default::default()
        },
        &prefilter,
    )
}

#[test]
fn test_node_search() -> anyhow::Result<()> {
    let reader = create_reader()?;

    // (:PLACE)
    let result = search_nodes(
        &reader,
        Query::Path(Path {
            source: Some(Node {
                node_subtype: Some("PLACE".to_string()),
                ..Default::default()
            }),
            undirected: true,
            ..Default::default()
        }),
    )?;
    assert_eq!(result.nodes.len(), 2);
    let nodes: HashSet<&str> = result.nodes.iter().map(|node| node.value.as_str()).collect();
    assert!(nodes.contains(&"New York"));
    assert!(nodes.contains(&"UK"));

    Ok(())
}

#[test]
fn test_relation_search() -> anyhow::Result<()> {
    let reader = create_reader()?;

    // (:Synonym)
    let result = search_relations(
        &reader,
        Query::Path(Path {
            relation: Some(Relation {
                relation_type: Some(RelationType::Synonym.into()),
                ..Default::default()
            }),
            ..Default::default()
        }),
    )?;
    assert_eq!(result.relations.len(), 1);
    let relations: HashSet<&str> = result
        .relations
        .iter()
        .map(|relation| relation.label.as_str())
        .collect();
    assert!(relations.contains(&"ALIAS"));

    Ok(())
}

#[test]
fn test_graph_node_query() -> anyhow::Result<()> {
    let reader = create_reader()?;

    // (:Anna)-[]->()
    let result = search(
        &reader,
        Query::Path(Path {
            source: Some(Node {
                value: Some("Anna".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        }),
    )?;
    let relations = friendly_parse(&result);
    assert_eq!(relations.len(), 4);
    assert!(relations.contains(&("Anna", "FOLLOW", "Erin")));
    assert!(relations.contains(&("Anna", "LIVE_IN", "New York")));
    assert!(relations.contains(&("Anna", "LOVE", "Cat")));
    assert!(relations.contains(&("Anna", "WORK_IN", "New York")));

    // (:PERSON)-[]->()
    let result = search(
        &reader,
        Query::Path(Path {
            source: Some(Node {
                node_subtype: Some("PERSON".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        }),
    )?;
    assert_eq!(result.graph.len(), 12);

    // ()-[]->(:Anna)
    let result = search(
        &reader,
        Query::Path(Path {
            destination: Some(Node {
                node_subtype: Some("PERSON".to_string()),
                value: Some("Anna".to_string()),
                node_type: Some(NodeType::Entity.into()),
                ..Default::default()
            }),
            ..Default::default()
        }),
    )?;
    let relations = friendly_parse(&result);
    assert_eq!(relations.len(), 1);
    assert!(relations.contains(&("Anastasia", "IS_FRIEND", "Anna")));

    // (:Anna)
    let result = search(
        &reader,
        Query::Path(Path {
            destination: Some(Node {
                node_subtype: Some("PERSON".to_string()),
                value: Some("Anna".to_string()),
                node_type: Some(NodeType::Entity.into()),
                ..Default::default()
            }),
            undirected: true,
            ..Default::default()
        }),
    )?;
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
fn test_graph_node_exact_matches() -> anyhow::Result<()> {
    let reader = create_reader()?;

    // Exact match (full)

    let result = search(
        &reader,
        Query::Path(Path {
            destination: Some(Node {
                value: Some("Computer science".to_string()),
                new_match_kind: Some(NewMatchKind::Exact(ExactMatch {
                    kind: MatchLocation::Full.into(),
                })),
                ..Default::default()
            }),
            ..Default::default()
        }),
    )?;
    let relations = friendly_parse(&result);
    assert_eq!(relations.len(), 1);
    assert!(relations.contains(&("Margaret", "WORK_IN", "Computer science")));

    // Prefix

    let result = search(
        &reader,
        Query::Path(Path {
            destination: Some(Node {
                value: Some("Computer sci".to_string()),
                new_match_kind: Some(NewMatchKind::Exact(ExactMatch {
                    kind: MatchLocation::Prefix.into(),
                })),
                ..Default::default()
            }),
            ..Default::default()
        }),
    )?;
    let relations = friendly_parse(&result);
    assert_eq!(relations.len(), 1);
    assert!(relations.contains(&("Margaret", "WORK_IN", "Computer science")));

    let result = search(
        &reader,
        Query::Path(Path {
            destination: Some(Node {
                value: Some("Compu".to_string()),
                new_match_kind: Some(NewMatchKind::Exact(ExactMatch {
                    kind: MatchLocation::Prefix.into(),
                })),
                ..Default::default()
            }),
            ..Default::default()
        }),
    )?;
    let relations = friendly_parse(&result);
    assert_eq!(relations.len(), 1);
    assert!(relations.contains(&("Margaret", "WORK_IN", "Computer science")));

    // Exact words

    let result = search(
        &reader,
        Query::Path(Path {
            destination: Some(Node {
                value: Some("Computer".to_string()),
                new_match_kind: Some(NewMatchKind::Exact(ExactMatch {
                    kind: MatchLocation::Words.into(),
                })),
                ..Default::default()
            }),
            ..Default::default()
        }),
    )?;
    let relations = friendly_parse(&result);
    assert_eq!(relations.len(), 1);
    assert!(relations.contains(&("Margaret", "WORK_IN", "Computer science")));

    let result = search(
        &reader,
        Query::Path(Path {
            destination: Some(Node {
                value: Some("science".to_string()),
                new_match_kind: Some(NewMatchKind::Exact(ExactMatch {
                    kind: MatchLocation::Words.into(),
                })),
                ..Default::default()
            }),
            ..Default::default()
        }),
    )?;
    let relations = friendly_parse(&result);
    assert_eq!(relations.len(), 1);
    assert!(relations.contains(&("Margaret", "WORK_IN", "Computer science")));

    // Prefix words

    let result = search(
        &reader,
        Query::Path(Path {
            destination: Some(Node {
                value: Some("sci".to_string()),
                new_match_kind: Some(NewMatchKind::Exact(ExactMatch {
                    kind: MatchLocation::PrefixWords.into(),
                })),
                ..Default::default()
            }),
            ..Default::default()
        }),
    )?;
    let relations = friendly_parse(&result);
    assert_eq!(relations.len(), 1);
    assert!(relations.contains(&("Margaret", "WORK_IN", "Computer science")));

    Ok(())
}

#[test]
fn test_graph_fuzzy_node_query() -> anyhow::Result<()> {
    let reader = create_reader()?;

    // (:~Anastas)
    #[allow(deprecated)]
    let result = search(
        &reader,
        Query::Path(Path {
            source: Some(Node {
                node_subtype: Some("PERSON".to_string()),
                value: Some("Anastas".to_string()),
                match_kind: MatchKind::DeprecatedFuzzy.into(),
                ..Default::default()
            }),
            ..Default::default()
        }),
    )?;
    let relations = friendly_parse(&result);
    assert_eq!(relations.len(), 1);
    assert!(relations.contains(&("Anastasia", "IS_FRIEND", "Anna")));

    // (:~AnXstXsia) with fuzzy=1
    #[allow(deprecated)]
    let result = search(
        &reader,
        Query::Path(Path {
            source: Some(Node {
                node_subtype: Some("PERSON".to_string()),
                value: Some("AnXstXsia".to_string()),
                match_kind: MatchKind::DeprecatedFuzzy.into(),
                ..Default::default()
            }),
            ..Default::default()
        }),
    )?;
    let relations = friendly_parse(&result);
    assert_eq!(relations.len(), 0);

    // (:~AnXstasia) with fuzzy=1
    #[allow(deprecated)]
    let result = search(
        &reader,
        Query::Path(Path {
            source: Some(Node {
                node_subtype: Some("PERSON".to_string()),
                value: Some("AnXstasia".to_string()),
                match_kind: MatchKind::DeprecatedFuzzy.into(),
                ..Default::default()
            }),
            ..Default::default()
        }),
    )?;
    let relations = friendly_parse(&result);
    assert_eq!(relations.len(), 1);
    assert!(relations.contains(&("Anastasia", "IS_FRIEND", "Anna")));

    // (:^~Ana) matches Anna & Anastasia
    #[allow(deprecated)]
    let result = search(
        &reader,
        Query::Path(Path {
            source: Some(Node {
                node_subtype: Some("PERSON".to_string()),
                value: Some("Ana".to_string()),
                match_kind: MatchKind::DeprecatedFuzzy.into(),
                ..Default::default()
            }),
            ..Default::default()
        }),
    )?;
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
fn test_graph_node_fuzzy_matches() -> anyhow::Result<()> {
    let reader = create_reader()?;

    // Fuzzy (full)

    let result = search(
        &reader,
        Query::Path(Path {
            destination: Some(Node {
                value: Some("Computer scXence".to_string()),
                new_match_kind: Some(NewMatchKind::Fuzzy(FuzzyMatch {
                    kind: MatchLocation::Full.into(),
                    distance: 1,
                })),
                ..Default::default()
            }),
            ..Default::default()
        }),
    )?;
    let relations = friendly_parse(&result);
    assert_eq!(relations.len(), 1);
    assert!(relations.contains(&("Margaret", "WORK_IN", "Computer science")));

    // Fuzzy prefix

    let result = search(
        &reader,
        Query::Path(Path {
            destination: Some(Node {
                value: Some("CompuXer sci".to_string()),
                new_match_kind: Some(NewMatchKind::Fuzzy(FuzzyMatch {
                    kind: MatchLocation::Prefix.into(),
                    distance: 1,
                })),
                ..Default::default()
            }),
            ..Default::default()
        }),
    )?;
    let relations = friendly_parse(&result);
    assert_eq!(relations.len(), 1);
    assert!(relations.contains(&("Margaret", "WORK_IN", "Computer science")));

    let result = search(
        &reader,
        Query::Path(Path {
            destination: Some(Node {
                value: Some("CoXpu".to_string()),
                new_match_kind: Some(NewMatchKind::Fuzzy(FuzzyMatch {
                    kind: MatchLocation::Prefix.into(),
                    distance: 1,
                })),
                ..Default::default()
            }),
            ..Default::default()
        }),
    )?;
    let relations = friendly_parse(&result);
    assert_eq!(relations.len(), 1);
    assert!(relations.contains(&("Margaret", "WORK_IN", "Computer science")));

    // Fuzzy words

    let result = search(
        &reader,
        Query::Path(Path {
            destination: Some(Node {
                value: Some("ComXuter".to_string()),
                new_match_kind: Some(NewMatchKind::Fuzzy(FuzzyMatch {
                    kind: MatchLocation::Words.into(),
                    distance: 1,
                })),
                ..Default::default()
            }),
            ..Default::default()
        }),
    )?;
    let relations = friendly_parse(&result);
    assert_eq!(relations.len(), 1);
    assert!(relations.contains(&("Margaret", "WORK_IN", "Computer science")));

    let result = search(
        &reader,
        Query::Path(Path {
            destination: Some(Node {
                value: Some("sciXnce".to_string()),
                new_match_kind: Some(NewMatchKind::Fuzzy(FuzzyMatch {
                    kind: MatchLocation::Words.into(),
                    distance: 1,
                })),
                ..Default::default()
            }),
            ..Default::default()
        }),
    )?;
    let relations = friendly_parse(&result);
    assert_eq!(relations.len(), 1);
    assert!(relations.contains(&("Margaret", "WORK_IN", "Computer science")));

    // Fuzzy prefix words

    let result = search(
        &reader,
        Query::Path(Path {
            destination: Some(Node {
                value: Some("scXen".to_string()),
                new_match_kind: Some(NewMatchKind::Fuzzy(FuzzyMatch {
                    kind: MatchLocation::PrefixWords.into(),
                    distance: 1,
                })),
                ..Default::default()
            }),
            ..Default::default()
        }),
    )?;
    let relations = friendly_parse(&result);
    assert_eq!(relations.len(), 1);
    assert!(relations.contains(&("Margaret", "WORK_IN", "Computer science")));

    Ok(())
}

#[test]
fn test_graph_relation_query() -> anyhow::Result<()> {
    let reader = create_reader()?;

    // ()-[:LIVE_IN]->()
    let result = search(
        &reader,
        Query::Path(Path {
            relation: Some(Relation {
                value: Some("LIVE_IN".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        }),
    )?;
    let relations = friendly_parse(&result);
    assert_eq!(relations.len(), 2);
    assert!(relations.contains(&("Anna", "LIVE_IN", "New York")));
    assert!(relations.contains(&("Peter", "LIVE_IN", "New York")));

    // ()-[:Synonym]->()
    let result = search(
        &reader,
        Query::Path(Path {
            relation: Some(Relation {
                relation_type: Some(RelationType::Synonym.into()),
                ..Default::default()
            }),
            ..Default::default()
        }),
    )?;
    let relations = friendly_parse(&result);
    assert_eq!(relations.len(), 1);
    assert!(relations.contains(&("Mr. P", "ALIAS", "Peter")));

    // Synonym relations with nonexistent label
    let result = search(
        &reader,
        Query::Path(Path {
            relation: Some(Relation {
                relation_type: Some(RelationType::Synonym.into()),
                value: Some("FAKE".to_string()),
            }),
            ..Default::default()
        }),
    )?;
    let relations = friendly_parse(&result);
    assert_eq!(relations.len(), 0);

    // ()-[:LIVE_IN | BORN_IN]->()
    let result = search(
        &reader,
        Query::BoolOr(BoolQuery {
            operands: vec![
                PathQuery {
                    query: Some(Query::Path(Path {
                        relation: Some(Relation {
                            value: Some("LIVE_IN".to_string()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    })),
                },
                PathQuery {
                    query: Some(Query::Path(Path {
                        relation: Some(Relation {
                            value: Some("BORN_IN".to_string()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    })),
                },
            ],
        }),
    )?;
    let relations = friendly_parse(&result);
    assert_eq!(relations.len(), 3);
    assert!(relations.contains(&("Anna", "LIVE_IN", "New York")));
    assert!(relations.contains(&("Erin", "BORN_IN", "UK")));
    assert!(relations.contains(&("Peter", "LIVE_IN", "New York")));

    // ()-[:!LIVE_IN]->()
    let result = search(
        &reader,
        Query::BoolNot(Box::new(PathQuery {
            query: Some(Query::Path(Path {
                relation: Some(Relation {
                    value: Some("LIVE_IN".to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            })),
        })),
    )?;
    let relations = friendly_parse(&result);
    assert_eq!(relations.len(), 15);

    Ok(())
}

#[test]
fn test_graph_directed_path_query() -> anyhow::Result<()> {
    let reader = create_reader()?;

    // ()-[]->()
    let result = search(&reader, Query::Path(Path::default()))?;
    assert_eq!(result.graph.len(), 17);

    // (:Erin)-[]->(:UK)
    let result = search(
        &reader,
        Query::Path(Path {
            source: Some(Node {
                value: Some("Erin".into()),
                node_type: Some(NodeType::Entity.into()),
                node_subtype: Some("PERSON".to_string()),
                ..Default::default()
            }),
            destination: Some(Node {
                value: Some("UK".into()),
                node_type: Some(NodeType::Entity.into()),
                node_subtype: Some("PLACE".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        }),
    )?;
    let relations = friendly_parse(&result);
    assert_eq!(relations.len(), 1);
    assert!(relations.contains(&("Erin", "BORN_IN", "UK")));

    // (:PERSON)-[]->(:PLACE)
    let result = search(
        &reader,
        Query::Path(Path {
            source: Some(Node {
                node_type: Some(NodeType::Entity.into()),
                node_subtype: Some("PERSON".to_string()),
                ..Default::default()
            }),
            destination: Some(Node {
                node_type: Some(NodeType::Entity.into()),
                node_subtype: Some("PLACE".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        }),
    )?;
    let relations = friendly_parse(&result);
    assert_eq!(relations.len(), 4);
    assert!(relations.contains(&("Anna", "LIVE_IN", "New York")));
    assert!(relations.contains(&("Anna", "WORK_IN", "New York")));
    assert!(relations.contains(&("Erin", "BORN_IN", "UK")));
    assert!(relations.contains(&("Peter", "LIVE_IN", "New York")));

    // (:PERSON)-[:LIVE_IN]->(:PLACE)
    let result = search(
        &reader,
        Query::Path(Path {
            source: Some(Node {
                node_type: Some(NodeType::Entity.into()),
                node_subtype: Some("PERSON".to_string()),
                ..Default::default()
            }),
            relation: Some(Relation {
                value: Some("LIVE_IN".to_string()),
                ..Default::default()
            }),
            destination: Some(Node {
                node_type: Some(NodeType::Entity.into()),
                node_subtype: Some("PLACE".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        }),
    )?;
    let relations = friendly_parse(&result);
    assert_eq!(relations.len(), 2);
    assert!(relations.contains(&("Anna", "LIVE_IN", "New York")));
    assert!(relations.contains(&("Peter", "LIVE_IN", "New York")));

    // (:!Anna)-[:LIVE_IN|LOVE]->()
    let result = search(
        &reader,
        Query::BoolAnd(BoolQuery {
            operands: vec![
                PathQuery {
                    query: Some(Query::BoolOr(BoolQuery {
                        operands: vec![
                            PathQuery {
                                query: Some(Query::Path(Path {
                                    relation: Some(Relation {
                                        value: Some("LIVE_IN".to_string()),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                })),
                            },
                            PathQuery {
                                query: Some(Query::Path(Path {
                                    relation: Some(Relation {
                                        value: Some("LOVE".to_string()),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                })),
                            },
                        ],
                    })),
                },
                PathQuery {
                    query: Some(Query::BoolNot(Box::new(PathQuery {
                        query: Some(Query::Path(Path {
                            source: Some(Node {
                                value: Some("Anna".to_string()),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })),
                    }))),
                },
            ],
        }),
    )?;
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
    let result = search(
        &reader,
        Query::Path(Path {
            source: Some(Node {
                node_type: Some(NodeType::Entity.into()),
                node_subtype: Some("PERSON".to_string()),
                value: Some("Anna".to_string()),
                ..Default::default()
            }),
            relation: Some(Relation {
                value: Some("IS_FRIEND".to_string()),
                ..Default::default()
            }),
            undirected: true,
            ..Default::default()
        }),
    )?;
    let relations = friendly_parse(&result);
    assert_eq!(relations.len(), 1);
    assert!(relations.contains(&("Anastasia", "IS_FRIEND", "Anna")));

    Ok(())
}

#[test]
fn test_graph_response() -> anyhow::Result<()> {
    let reader = create_reader()?;

    // ()-[:LIVE_IN|WORK_IN]->()
    let result = search(
        &reader,
        Query::BoolOr(BoolQuery {
            operands: vec![
                PathQuery {
                    query: Some(Query::Path(Path {
                        relation: Some(Relation {
                            value: Some("LIVE_IN".to_string()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    })),
                },
                PathQuery {
                    query: Some(Query::Path(Path {
                        relation: Some(Relation {
                            value: Some("WORK_IN".to_string()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    })),
                },
            ],
        }),
    )?;
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

#[test]
fn test_prefilter() -> anyhow::Result<()> {
    // Basic prefilter tests, other tests are done at upper levels with access to the text index
    let reader = create_reader()?;

    // All
    let result = search_with_prefilter(&reader, Query::Path(Path::default()), PrefilterResult::All)?;
    assert_eq!(result.graph.len(), 17);

    // None
    let result = search_with_prefilter(&reader, Query::Path(Path::default()), PrefilterResult::None)?;
    assert_eq!(result.graph.len(), 0);

    // Some excludes our result
    let result = search_with_prefilter(
        &reader,
        Query::Path(Path::default()),
        PrefilterResult::Some(vec![FieldId {
            resource_id: Uuid::nil(),
            field_id: "/f/fake".to_string(),
        }]),
    )?;
    assert_eq!(result.graph.len(), 0);

    // Some includes our result
    let result = search_with_prefilter(
        &reader,
        Query::Path(Path::default()),
        PrefilterResult::Some(vec![FieldId {
            resource_id: Uuid::parse_str("0123456789abcdef0123456789abcdef").unwrap(),
            field_id: "/a/metadata".to_string(),
        }]),
    )?;
    assert_eq!(result.graph.len(), 17);

    // Some includes a/metadata
    let result = search_with_prefilter(
        &reader,
        Query::Path(Path::default()),
        PrefilterResult::Some(vec![FieldId {
            resource_id: Uuid::parse_str("0123456789abcdef0123456789abcdef").unwrap(),
            field_id: "/f/fake".to_string(),
        }]),
    )?;
    assert_eq!(result.graph.len(), 17);

    Ok(())
}

#[test]
fn test_prefilter_file_field() -> anyhow::Result<()> {
    // Basic prefilter tests, other tests are done at upper levels with access to the text index
    let reader = create_reader_file_field()?;

    // All
    let result = search_with_prefilter(&reader, Query::Path(Path::default()), PrefilterResult::All)?;
    assert_eq!(result.graph.len(), 17);

    // None
    let result = search_with_prefilter(&reader, Query::Path(Path::default()), PrefilterResult::None)?;
    assert_eq!(result.graph.len(), 0);

    // Some excludes our result
    let result = search_with_prefilter(
        &reader,
        Query::Path(Path::default()),
        PrefilterResult::Some(vec![FieldId {
            resource_id: Uuid::parse_str("0123456789abcdef0123456789abcdef").unwrap(),
            field_id: "/f/fake".to_string(),
        }]),
    )?;
    assert_eq!(result.graph.len(), 0);

    // Some includes our result
    let result = search_with_prefilter(
        &reader,
        Query::Path(Path::default()),
        PrefilterResult::Some(vec![FieldId {
            resource_id: Uuid::parse_str("0123456789abcdef0123456789abcdef").unwrap(),
            field_id: "/f/my_file".to_string(),
        }]),
    )?;
    assert_eq!(result.graph.len(), 17);

    Ok(())
}

#[test]
fn test_facet_filter() -> anyhow::Result<()> {
    let dir = TempDir::new().unwrap();

    // Create a graph with relations with different facets
    let relations = vec![
        IndexRelation {
            relation: Some(nidx_protos::Relation {
                source: Some(RelationNode {
                    ntype: NodeType::Entity.into(),
                    subtype: "PERSON".to_string(),
                    value: "Peter Processor".to_string(),
                }),
                to: Some(RelationNode {
                    ntype: NodeType::Entity.into(),
                    subtype: "PERSON".to_string(),
                    value: "Pedro Procesador".to_string(),
                }),
                relation: RelationType::Entity.into(),
                relation_label: "SAME".to_string(),
                metadata: None,
            }),
            facets: vec![],
            ..Default::default()
        },
        IndexRelation {
            relation: Some(nidx_protos::Relation {
                source: Some(RelationNode {
                    ntype: NodeType::Entity.into(),
                    subtype: "PERSON".to_string(),
                    value: "Ursula User".to_string(),
                }),
                to: Some(RelationNode {
                    ntype: NodeType::Entity.into(),
                    subtype: "PERSON".to_string(),
                    value: "Úrsula Usuaria".to_string(),
                }),
                relation: RelationType::Entity.into(),
                relation_label: "SAME".to_string(),
                metadata: None,
            }),
            facets: vec!["/g/u".to_string()],
            ..Default::default()
        },
        IndexRelation {
            relation: Some(nidx_protos::Relation {
                source: Some(RelationNode {
                    ntype: NodeType::Entity.into(),
                    subtype: "PERSON".to_string(),
                    value: "Alfred Agent".to_string(),
                }),
                to: Some(RelationNode {
                    ntype: NodeType::Entity.into(),
                    subtype: "PERSON".to_string(),
                    value: "Alfred Agente".to_string(),
                }),
                relation: RelationType::Entity.into(),
                relation_label: "SAME".to_string(),
                metadata: None,
            }),
            facets: vec!["/g/da/mytask".to_string()],
            ..Default::default()
        },
    ];
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
    let reader = RelationSearcher::open(
        RelationConfig::default(),
        TestOpener::new(vec![(segment_meta, 1i64.into())], vec![]),
    )?;

    // Everything
    let result = search(&reader, Query::Path(Path::default()))?;
    assert_eq!(result.relations.len(), 3);

    // Processor
    let result = search(
        &reader,
        Query::BoolNot(Box::new(PathQuery {
            query: Some(Query::Facet(FacetFilter {
                facet: "/g".to_string(),
            })),
        })),
    )?;
    assert_eq!(result.relations.len(), 1);
    assert_eq!(result.nodes[0].value, "Peter Processor");

    // User
    let result = search(
        &reader,
        Query::Facet(FacetFilter {
            facet: "/g/u".to_string(),
        }),
    )?;
    assert_eq!(result.relations.len(), 1);
    assert_eq!(result.nodes[0].value, "Ursula User");

    // Any DA
    let result = search(
        &reader,
        Query::Facet(FacetFilter {
            facet: "/g/da".to_string(),
        }),
    )?;
    assert_eq!(result.relations.len(), 1);
    assert_eq!(result.nodes[0].value, "Alfred Agent");

    // Specific DA
    let result = search(
        &reader,
        Query::Facet(FacetFilter {
            facet: "/g/da/mytask".to_string(),
        }),
    )?;
    assert_eq!(result.relations.len(), 1);
    assert_eq!(result.nodes[0].value, "Alfred Agent");

    // Non-existing facet
    let result = search(
        &reader,
        Query::Facet(FacetFilter {
            facet: "/g/da/faketask".to_string(),
        }),
    )?;
    assert_eq!(result.relations.len(), 0);

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

fn create_reader_file_field() -> anyhow::Result<RelationSearcher> {
    let dir = TempDir::new().unwrap();

    let relations = nidx_tests::graph::knowledge_graph_as_relations();
    let field_relations = HashMap::from([("f/my_file".to_string(), IndexRelations { relations })]);
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
