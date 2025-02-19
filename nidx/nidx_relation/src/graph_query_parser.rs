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
use nidx_protos::relation_node::NodeType;
use tantivy::collector::TopDocs;
use tantivy::query::{AllQuery, BooleanQuery, FuzzyTermQuery, Occur, Query, TermQuery};
use tantivy::schema::IndexRecordOption;
use tantivy::{Searcher, Term};

use crate::schema::Schema;
use crate::{io_maps, schema};

pub struct GraphSearcher {
    schema: Schema,
    parser: GraphQueryParser,
    searcher: Searcher,
}

#[derive(Default, Clone)]
pub struct Node {
    pub value: Option<String>,
    pub node_type: Option<NodeType>,
    pub node_subtype: Option<String>,
    pub fuzzy_distance: Option<u8>,
    pub fuzzy_prefix: bool,
}

#[derive(Default, Clone)]
pub struct Relation {
    pub value: Option<String>,
}

#[derive(Clone)]
pub enum Expression<T> {
    Value(T),
    Not(T),
    And(Vec<T>),
    Or(Vec<T>),
}

pub enum NodeQuery {
    // (:S)-
    SourceNode(Expression<Node>),
    // ->(:T)
    DestinationNode(Expression<Node>),
    // A is either (:A)- or ->(:A)
    Node(Expression<Node>),
}

pub struct RelationQuery(pub Expression<Relation>);

pub enum PathQuery {
    // (:A)-[:R]->(:B)
    DirectedPath((Expression<Node>, Expression<Relation>, Expression<Node>)),
    // (:A)-[:R]-(:B) = (:A)-[:R]->(:B) OR (:A)<-[:R]-(:B)
    UndirectedPath((Expression<Node>, Expression<Relation>, Expression<Node>)),
}

/// Graphs are made of nodes and relations, each of them with different searchable properties
/// (label, type, subtype...). `GraphQuiery` allows expressing graph search in a declarative way.
/// One can express search for nodes, relations or paths.
///
/// All queries are meant to be single-hop, so each query refers to a triplet in the graph.
pub enum GraphQuery {
    // (:A)
    NodeQuery(NodeQuery),
    // ()-[:R]->()
    RelationQuery(RelationQuery),
    // (:A)-[:R]->(:B)
    PathQuery(PathQuery),
}

pub struct GraphQueryParser {
    schema: Schema,
}

impl GraphSearcher {
    pub fn new(searcher: Searcher) -> Self {
        Self {
            schema: Schema::new(),
            parser: GraphQueryParser::new(),
            searcher,
        }
    }

    pub fn search(&self, query: GraphQuery) -> anyhow::Result<Vec<nidx_protos::Relation>> {
        let index_query: Box<dyn Query> = self.parser.parse_graph_query(query);
        // println!("Index query: {index_query:#?}");

        let collector = TopDocs::with_limit(1000);

        let matching_docs = self.searcher.search(&index_query, &collector)?;
        let mut relations = Vec::with_capacity(matching_docs.len());
        for (_, doc_addr) in matching_docs {
            let doc = self.searcher.doc(doc_addr)?;
            let relation = io_maps::doc_to_relation(&self.schema, &doc);
            relations.push(relation);
        }
        Ok(relations)
    }
}

impl GraphQueryParser {
    pub fn new() -> Self {
        Self {
            schema: Schema::new(),
        }
    }

    pub fn parse_graph_query(&self, query: GraphQuery) -> Box<dyn Query> {
        // REVIEW: if at some point we only want to return what the query really asks (nodes,
        // relations or paths), we may want to return the query and some kind of response builder
        match query {
            GraphQuery::NodeQuery(query) => self.parse_node_query(query),
            GraphQuery::RelationQuery(query) => self.parse_relation_query(query),
            GraphQuery::PathQuery(query) => self.parse_path_query(query),
        }
    }

    fn parse_node_query(&self, query: NodeQuery) -> Box<dyn Query> {
        match query {
            NodeQuery::SourceNode(source) => self.parse_path_query(PathQuery::DirectedPath((
                source,
                Expression::Value(Relation::default()),
                Expression::Value(Node::default()),
            ))),

            NodeQuery::DestinationNode(destination) => self.parse_path_query(PathQuery::DirectedPath((
                Expression::Value(Node::default()),
                Expression::Value(Relation::default()),
                destination,
            ))),

            NodeQuery::Node(node) => self.parse_path_query(PathQuery::UndirectedPath((
                node,
                Expression::Value(Relation::default()),
                Expression::Value(Node::default()),
            ))),
        }
    }

    fn parse_relation_query(&self, query: RelationQuery) -> Box<dyn Query> {
        self.parse_path_query(PathQuery::DirectedPath((
            Expression::Value(Node::default()),
            query.0,
            Expression::Value(Node::default()),
        )))
    }

    fn parse_path_query(&self, query: PathQuery) -> Box<dyn Query> {
        match query {
            PathQuery::DirectedPath((source, relation, destination)) => {
                match (source, relation, destination) {
                    // AND expressions like `(A & B)-[R]->C` must be splitted and queried as an
                    // intersection, as there is no document that satisfies a source `A & B` at the
                    // same time (as they'll be different triplets). However, `(A & B)-[R]->C` is
                    // equivalent to `A-[R]->C & B-[R]->C`, so we can query the intersection of
                    // both.
                    //
                    // TODO: avoid too many clone by passing Node/Relation by reference
                    (Expression::And(sources), relation, destination) => {
                        let subqueries = sources.into_iter().map(|source| {
                            self.parse_path_query(PathQuery::DirectedPath((
                                Expression::Value(source),
                                relation.clone(),
                                destination.clone(),
                            )))
                        });
                        Box::new(BooleanQuery::intersection(subqueries.collect()))
                    }

                    (source, Expression::And(relations), destination) => {
                        let subqueries = relations.into_iter().map(|relation| {
                            self.parse_path_query(PathQuery::DirectedPath((
                                source.clone(),
                                Expression::Value(relation),
                                destination.clone(),
                            )))
                        });
                        Box::new(BooleanQuery::intersection(subqueries.collect()))
                    }

                    (source, relation, Expression::And(destinations)) => {
                        let subqueries = destinations.into_iter().map(|destination| {
                            self.parse_path_query(PathQuery::DirectedPath((
                                source.clone(),
                                relation.clone(),
                                Expression::Value(destination),
                            )))
                        });
                        Box::new(BooleanQuery::intersection(subqueries.collect()))
                    }

                    // The rest of operators (VALUE, NOT and OR) don't need to be splitted. Only NOT
                    // need special attention to flip the Occur type, as a Must { MustNot { X } }
                    // doesn't work as a Not inside the expression we mount
                    (source_expression, relation_expression, destination_expression) => {
                        let mut source_occur = Occur::Must;
                        let source_node_query: Box<dyn Query> = match source_expression {
                            Expression::Value(query) => self.has_node_as_source(&query),
                            Expression::Not(query) => {
                                source_occur = Occur::MustNot;
                                self.has_node_as_source(&query)
                            }
                            Expression::Or(nodes) => {
                                let subqueries = nodes.into_iter().map(|node| self.has_node_as_source(&node));
                                Box::new(BooleanQuery::union(subqueries.collect()))
                            }
                            Expression::And(_) => unreachable!("Pattern already matched in the outer match"),
                        };

                        let mut relation_occur = Occur::Must;
                        let relation_query: Box<dyn Query> = match relation_expression {
                            Expression::Value(query) => self.has_relation(query),
                            Expression::Not(query) => {
                                relation_occur = Occur::MustNot;
                                self.has_relation(query)
                            }
                            Expression::Or(queries) => {
                                let subqueries = queries.into_iter().map(|query| self.has_relation(query));
                                Box::new(BooleanQuery::union(subqueries.collect()))
                            }
                            Expression::And(_) => unreachable!("Pattern already matched in the outer match"),
                        };

                        let mut target_occur = Occur::Must;
                        let target_node_query: Box<dyn Query> = match destination_expression {
                            Expression::Value(query) => self.has_node_as_destination(&query),
                            Expression::Not(query) => {
                                target_occur = Occur::MustNot;
                                self.has_node_as_destination(&query)
                            }
                            Expression::Or(queries) => {
                                let subqueries = queries.into_iter().map(|query| self.has_node_as_destination(&query));
                                Box::new(BooleanQuery::union(subqueries.collect()))
                            }
                            Expression::And(_) => unreachable!("Pattern already matched in the outer match"),
                        };

                        Box::new(BooleanQuery::new(vec![
                            (source_occur, source_node_query),
                            (relation_occur, relation_query),
                            (target_occur, target_node_query),
                        ]))
                    }
                }
            }
            PathQuery::UndirectedPath((source, relation, destination)) => Box::new(BooleanQuery::union(vec![
                self.parse_path_query(PathQuery::DirectedPath((source.clone(), relation.clone(), destination.clone()))),
                self.parse_path_query(PathQuery::DirectedPath((destination, relation, source))),
            ])),
        }
    }

    fn has_node_as_source(&self, query: &Node) -> Box<dyn Query> {
        let mut subqueries: Vec<Box<dyn Query>> = vec![];

        if let Some(ref value) = query.value {
            if !value.is_empty() {
                let normalized_value = schema::normalize(&value);

                let node_value_query: Box<dyn Query> = match (query.fuzzy_distance, query.fuzzy_prefix) {
                    (None, false) => {
                        Box::new(TermQuery::new(
                            Term::from_field_text(self.schema.normalized_source_value, &normalized_value),
                            IndexRecordOption::Basic,
                        ))
                    }
                    (None, true) => {
                        Box::new(FuzzyTermQuery::new_prefix(
                            Term::from_field_text(self.schema.normalized_source_value, &normalized_value),
                            0,
                            true,
                        ))
                    }
                    (Some(distance), true) => {
                        Box::new(FuzzyTermQuery::new(
                            Term::from_field_text(self.schema.normalized_source_value, &normalized_value),
                            distance,
                            true,
                        ))
                    }
                    (Some(distance), false) => {
                        Box::new(FuzzyTermQuery::new_prefix(
                            Term::from_field_text(self.schema.normalized_source_value, &normalized_value),
                            distance,
                            true,
                        ))
                    }
                };

                subqueries.push(node_value_query)
            }
        }

        if let Some(node_type) = query.node_type {
            let node_type = io_maps::node_type_to_u64(node_type);
            let node_type_query = Box::new(TermQuery::new(
                Term::from_field_u64(self.schema.source_type, node_type),
                IndexRecordOption::Basic,
            ));
            subqueries.push(node_type_query);
        }

        if let Some(ref node_subtype) = query.node_subtype {
            if !node_subtype.is_empty() {
                let node_subtype_query = Box::new(TermQuery::new(
                    Term::from_field_text(self.schema.source_subtype, &node_subtype),
                    IndexRecordOption::Basic,
                ));
                subqueries.push(node_subtype_query);
            }
        }

        let query: Box<dyn Query> = if subqueries.len() > 0 {
            Box::new(BooleanQuery::intersection(subqueries))
        } else {
            Box::new(AllQuery)
        };

        query
    }

    fn has_node_as_destination(&self, query: &Node) -> Box<dyn Query> {
        let mut subqueries: Vec<Box<dyn Query>> = vec![];

        if let Some(ref value) = query.value {
            if !value.is_empty() {
                let normalized_value = schema::normalize(&value);
                let node_value_query = Box::new(TermQuery::new(
                    Term::from_field_text(self.schema.normalized_target_value, &normalized_value),
                    IndexRecordOption::Basic,
                ));
                subqueries.push(node_value_query)
            }
        }

        if let Some(node_type) = query.node_type {
            let node_type = io_maps::node_type_to_u64(node_type);
            let node_type_query = Box::new(TermQuery::new(
                Term::from_field_u64(self.schema.target_type, node_type),
                IndexRecordOption::Basic,
            ));
            subqueries.push(node_type_query);
        }

        if let Some(ref node_subtype) = query.node_subtype {
            if !node_subtype.is_empty() {
                let node_subtype_query = Box::new(TermQuery::new(
                    Term::from_field_text(self.schema.target_subtype, &node_subtype),
                    IndexRecordOption::Basic,
                ));
                subqueries.push(node_subtype_query);
            }
        }

        let query: Box<dyn Query> = if subqueries.len() > 0 {
            Box::new(BooleanQuery::intersection(subqueries))
        } else {
            Box::new(AllQuery)
        };

        query
    }

    fn has_relation(&self, query: Relation) -> Box<dyn Query> {
        match query.value {
            Some(value) if !value.is_empty() => Box::new(BooleanQuery::new(vec![(
                Occur::Should,
                Box::new(TermQuery::new(Term::from_field_text(self.schema.label, &value), IndexRecordOption::Basic)),
            )])),
            Some(_) | None => Box::new(AllQuery),
        }
    }
}
