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
use tantivy::schema::{Field, IndexRecordOption};
use tantivy::Searcher;

use crate::schema::Schema;
use crate::{io_maps, schema};

pub struct GraphSearcher {
    schema: Schema,
    parser: GraphQueryParser,
    searcher: Searcher,
}

#[derive(Clone)]
pub struct FuzzyTerm {
    pub value: String,
    pub fuzzy_distance: Option<u8>,
    pub is_prefix: bool,
}

#[derive(Clone)]
pub enum Term {
    Exact(String),
    Fuzzy(FuzzyTerm),
}

#[derive(Default, Clone)]
pub struct Node {
    pub value: Option<Term>,
    pub node_type: Option<NodeType>,
    pub node_subtype: Option<String>,
}

#[derive(Default, Clone)]
pub struct Relation {
    pub value: Option<String>,
    // TODO: fuzzy distance and fuzzy_prefix
    // pub fuzzy_distance: Option<u8>,
    // pub fuzzy_prefix: bool,
}

// Generic (simple) boolean expression for graph querying
//
// AND has not been implemented as it would require multi-hop queries. To resolve a query like
// (:A)-[R&P]->(:B), we need two know if (:A)-[R]->(:B) and (:A)-[P]->(:B) and then validate if both
// exist. I.e., if we want to know whether A and B are connected though both relations R and P, we
// need to know if both triplets exist.
//
// That's like this because there's no possible triplet satisfying an AND over the same value. A
// node/relation can't be X and Y at the same time.
//
// Once we have results from both queries, we can then compute an AND validating both triplets
// exist. But, as said, this has to be done after querying the index and retrieving such triplets.
//
// Furthemore, the complete graph can be splitted in multiple shards, as the index is partitioned.
// This means we should first resolve both queries, gather all the results and finally check the AND
// condition. As it's more complex than useful, we don't do ANDs here.
#[derive(Clone)]
pub enum Expression<T> {
    Value(T),
    Not(T),
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
    // Combine queries with an OR
    // (:A)-[:R]->(:B), (:A)-[:R]->(:B)
    MultiStatement(Vec<PathQuery>),
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
            GraphQuery::MultiStatement(queries) => self.parse_multi_statement(queries),
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
            PathQuery::DirectedPath((source_expression, relation_expression, destination_expression)) => {
                // NOT need special attention to flip the Occur type, as a Must { MustNot {
                // X } } doesn't work as a Not inside the expression we mount

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
                };

                Box::new(BooleanQuery::new(vec![
                    (source_occur, source_node_query),
                    (relation_occur, relation_query),
                    (target_occur, target_node_query),
                ]))
            }
            PathQuery::UndirectedPath((source, relation, destination)) => Box::new(BooleanQuery::union(vec![
                self.parse_path_query(PathQuery::DirectedPath((source.clone(), relation.clone(), destination.clone()))),
                self.parse_path_query(PathQuery::DirectedPath((destination, relation, source))),
            ])),
        }
    }

    fn parse_multi_statement(&self, queries: Vec<PathQuery>) -> Box<dyn Query> {
        Box::new(BooleanQuery::union(queries.into_iter().map(|query| self.parse_path_query(query)).collect()))
    }

    fn has_node_as_source(&self, query: &Node) -> Box<dyn Query> {
        self.has_node_as(
            query,
            self.schema.normalized_source_value,
            self.schema.source_type,
            self.schema.source_subtype,
        )
    }

    fn has_node_as_destination(&self, query: &Node) -> Box<dyn Query> {
        self.has_node_as(
            query,
            self.schema.normalized_target_value,
            self.schema.target_type,
            self.schema.target_subtype,
        )
    }

    fn has_node_as(
        &self,
        query: &Node,
        normalized_node_value_field: Field,
        node_type_field: Field,
        node_subtype_field: Field,
    ) -> Box<dyn Query> {
        let mut subqueries: Vec<Box<dyn Query>> = vec![];

        if let Some(ref term) = query.value {
            match term {
                Term::Exact(value)
                | Term::Fuzzy(FuzzyTerm {
                    value,
                    ..
                }) if value.is_empty() => {}
                Term::Exact(value)
                | Term::Fuzzy(FuzzyTerm {
                    value,
                    fuzzy_distance: None,
                    is_prefix: false,
                }) => {
                    let normalized_value = schema::normalize(&value);
                    let term_query = Box::new(TermQuery::new(
                        tantivy::Term::from_field_text(normalized_node_value_field, &normalized_value),
                        IndexRecordOption::Basic,
                    ));
                    subqueries.push(term_query);
                }
                Term::Fuzzy(fuzzy_term) => {
                    let normalized_value = schema::normalize(&fuzzy_term.value);
                    let fuzzy_query = match fuzzy_term {
                        FuzzyTerm {
                            fuzzy_distance: None,
                            is_prefix: true,
                            ..
                        } => Box::new(FuzzyTermQuery::new_prefix(
                            tantivy::Term::from_field_text(normalized_node_value_field, &normalized_value),
                            0,
                            true,
                        )),
                        FuzzyTerm {
                            fuzzy_distance: Some(distance),
                            is_prefix: true,
                            ..
                        } => Box::new(FuzzyTermQuery::new(
                            tantivy::Term::from_field_text(normalized_node_value_field, &normalized_value),
                            *distance,
                            true,
                        )),
                        FuzzyTerm {
                            fuzzy_distance: Some(distance),
                            is_prefix: false,
                            ..
                        } => Box::new(FuzzyTermQuery::new_prefix(
                            tantivy::Term::from_field_text(normalized_node_value_field, &normalized_value),
                            *distance,
                            true,
                        )),
                        _ => unreachable!("Pattern already matched above"),
                    };
                    subqueries.push(fuzzy_query);
                }
            }
        }

        if let Some(node_type) = query.node_type {
            let node_type = io_maps::node_type_to_u64(node_type);
            let node_type_query = Box::new(TermQuery::new(
                tantivy::Term::from_field_u64(node_type_field, node_type),
                IndexRecordOption::Basic,
            ));
            subqueries.push(node_type_query);
        }

        if let Some(ref node_subtype) = query.node_subtype {
            if !node_subtype.is_empty() {
                let node_subtype_query = Box::new(TermQuery::new(
                    tantivy::Term::from_field_text(node_subtype_field, &node_subtype),
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
                Box::new(TermQuery::new(
                    tantivy::Term::from_field_text(self.schema.label, &value),
                    IndexRecordOption::Basic,
                )),
            )])),
            Some(_) | None => Box::new(AllQuery),
        }
    }
}

impl From<String> for Term {
    fn from(value: String) -> Self {
        Self::Exact(value)
    }
}

impl From<&str> for Term {
    fn from(value: &str) -> Self {
        Self::Exact(value.to_string())
    }
}
