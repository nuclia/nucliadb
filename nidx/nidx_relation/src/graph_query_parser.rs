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
    // (:A)-[:R]->(:B), !(:A)-[:P]->(:B)
    MultiStatement(Vec<Expression<PathQuery>>),
}

#[derive(Clone, Copy)]
struct NodeSchemaFields {
    normalized_value: Field,
    node_type: Field,
    node_subtype: Field,
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
        let equivalent_path_query = match query {
            NodeQuery::SourceNode(source) => PathQuery::DirectedPath((
                source,
                Expression::Value(Relation::default()),
                Expression::Value(Node::default()),
            )),

            NodeQuery::DestinationNode(destination) => PathQuery::DirectedPath((
                Expression::Value(Node::default()),
                Expression::Value(Relation::default()),
                destination,
            )),

            NodeQuery::Node(node) => PathQuery::UndirectedPath((
                node,
                Expression::Value(Relation::default()),
                Expression::Value(Node::default()),
            )),
        };
        self.parse_path_query(equivalent_path_query)
    }

    fn parse_relation_query(&self, query: RelationQuery) -> Box<dyn Query> {
        let equivalent_path_query =
            PathQuery::DirectedPath((Expression::Value(Node::default()), query.0, Expression::Value(Node::default())));
        self.parse_path_query(equivalent_path_query)
    }

    fn parse_path_query(&self, query: PathQuery) -> Box<dyn Query> {
        match query {
            PathQuery::DirectedPath((source_expression, relation_expression, destination_expression)) => {
                let mut subqueries = vec![];

                subqueries.extend(self.has_node_expression_as_source(&source_expression));
                subqueries.extend(self.has_relation(relation_expression).into_iter());
                subqueries.extend(self.has_node_expression_as_destination(&destination_expression));

                // Due to implementation details on tantivy, a query containing only MustNot won't
                // match anything. In this case, we need to add an AllQuery to get results
                if subqueries.iter().all(|(occur, _)| *occur == Occur::MustNot) {
                    subqueries.push((Occur::Must, Box::new(AllQuery)));
                }

                Box::new(BooleanQuery::new(subqueries))
            }
            PathQuery::UndirectedPath((source, relation, destination)) => Box::new(BooleanQuery::union(vec![
                self.parse_path_query(PathQuery::DirectedPath((source.clone(), relation.clone(), destination.clone()))),
                self.parse_path_query(PathQuery::DirectedPath((destination, relation, source))),
            ])),
        }
    }

    fn parse_multi_statement(&self, queries: Vec<Expression<PathQuery>>) -> Box<dyn Query> {
        let mut subqueries = vec![];
        for expression in queries {
            let subquery = match expression {
                Expression::Value(query) => (Occur::Should, self.parse_path_query(query) as Box<dyn Query>),
                Expression::Not(query) => (Occur::MustNot, self.parse_path_query(query) as Box<dyn Query>),
                Expression::Or(queries) => (
                    Occur::Should,
                    Box::new(BooleanQuery::union(
                        queries.into_iter().map(|query| self.parse_path_query(query)).collect(),
                    )) as Box<dyn Query>,
                ),
            };
            subqueries.push(subquery);
        }

        // Due to implementation details on tantivy, a query containing only MustNot won't match
        // anything. In this case, we need to add an AllQuery to get results
        if subqueries.iter().all(|(occur, _)| *occur == Occur::MustNot) {
            subqueries.push((Occur::Must, Box::new(AllQuery)));
        }

        Box::new(BooleanQuery::new(subqueries))
    }

    #[inline]
    fn has_node_expression_as_source(&self, expression: &Expression<Node>) -> Vec<(Occur, Box<dyn Query>)> {
        self.has_node_expression(
            expression,
            NodeSchemaFields {
                normalized_value: self.schema.normalized_source_value,
                node_type: self.schema.source_type,
                node_subtype: self.schema.source_subtype,
            },
        )
    }

    #[inline]
    fn has_node_expression_as_destination(&self, expression: &Expression<Node>) -> Vec<(Occur, Box<dyn Query>)> {
        self.has_node_expression(
            expression,
            NodeSchemaFields {
                normalized_value: self.schema.normalized_target_value,
                node_type: self.schema.target_type,
                node_subtype: self.schema.target_subtype,
            },
        )
    }

    // Generic version of has_node_expression_as_X to avoid code duplication for source and
    // destination node queries
    fn has_node_expression(
        &self,
        expression: &Expression<Node>,
        fields: NodeSchemaFields,
    ) -> Vec<(Occur, Box<dyn Query>)> {
        let mut subqueries = vec![];

        match expression {
            Expression::Value(query) => {
                subqueries.extend(self.has_node(query, fields).into_iter().map(|query| (Occur::Must, query)));
            }
            Expression::Not(query) => {
                // NOT granularity is a node, so we to use a Must { MustNot { X } } instead of
                // unnest it in multiple MustNot { x }
                let subquery: Box<dyn Query> = Box::new(BooleanQuery::intersection(self.has_node(&query, fields)));
                subqueries.push((Occur::MustNot, subquery));
            }
            Expression::Or(nodes) => {
                let subquery: Box<dyn Query> = Box::new(BooleanQuery::union(
                    nodes.into_iter().flat_map(|node| self.has_node(&node, fields)).collect(),
                ));
                subqueries.push((Occur::Must, subquery));
            }
        };

        subqueries
    }

    // Return a list of queries needed to match a triplet such node expression
    fn has_node(&self, node: &Node, fields: NodeSchemaFields) -> Vec<Box<dyn Query>> {
        let mut subqueries = vec![];

        let value_query = node.value.as_ref().and_then(|value| self.has_node_value(value, fields.normalized_value));
        if let Some(query) = value_query {
            subqueries.push(query);
        }

        if let Some(node_type) = node.node_type {
            subqueries.push(self.has_node_type(node_type, fields.node_type));
        }

        if let Some(ref node_subtype) = node.node_subtype {
            if !node_subtype.is_empty() {
                subqueries.push(self.has_node_subtype(node_subtype, fields.node_subtype));
            }
        }

        subqueries
    }

    // Return a list of queries needed to match a triplet containing such relation expression
    fn has_relation(&self, expression: Expression<Relation>) -> Vec<(Occur, Box<dyn Query>)> {
        let mut subqueries = vec![];

        match expression {
            Expression::Value(Relation {
                value: None,
            }) => {}
            Expression::Value(Relation {
                value: Some(value),
            }) if value.is_empty() => {}
            Expression::Value(Relation {
                value: Some(value),
            }) => {
                subqueries.push((Occur::Must, self.has_relation_label(&value)));
            }

            Expression::Not(Relation {
                value: None,
            }) => {}
            Expression::Not(Relation {
                value: Some(value),
            }) if value.is_empty() => {}
            Expression::Not(Relation {
                value: Some(value),
            }) => {
                subqueries.push((Occur::MustNot, self.has_relation_label(&value)));
            }

            Expression::Or(relations) => {
                let mut or_subqueries: Vec<_> = relations
                    .into_iter()
                    .flat_map(|relation| {
                        if let Some(value) = relation.value {
                            if !value.is_empty() {
                                return Some((Occur::Should, self.has_relation_label(&value)));
                            }
                        }
                        None
                    })
                    .collect();
                match or_subqueries.len() {
                    0 => {}
                    1 => subqueries.push(or_subqueries.pop().unwrap()),
                    _n => {
                        let or_query: Box<dyn Query> = Box::new(BooleanQuery::new(or_subqueries));
                        subqueries.push((Occur::Must, or_query));
                    }
                }
            }
        };

        subqueries
    }

    fn has_node_value(&self, value: &Term, field: Field) -> Option<Box<dyn Query>> {
        match value {
            Term::Exact(value)
            | Term::Fuzzy(FuzzyTerm {
                value,
                fuzzy_distance: None,
                is_prefix: false,
            }) => {
                if value.is_empty() {
                    return None;
                }
                let normalized_value = schema::normalize(&value);
                let query = Box::new(TermQuery::new(
                    tantivy::Term::from_field_text(field, &normalized_value),
                    IndexRecordOption::Basic,
                ));
                Some(query)
            }

            Term::Fuzzy(fuzzy) => {
                let normalized_value = schema::normalize(&fuzzy.value);
                match fuzzy {
                    FuzzyTerm {
                        value,
                        ..
                    } if value.is_empty() => None,

                    FuzzyTerm {
                        fuzzy_distance: None,
                        is_prefix: false,
                        ..
                    } => {
                        unreachable!("Already matched as exact match")
                    }

                    FuzzyTerm {
                        fuzzy_distance: None,
                        is_prefix: true,
                        ..
                    } => Some(Box::new(FuzzyTermQuery::new_prefix(
                        tantivy::Term::from_field_text(field, &normalized_value),
                        0,
                        true,
                    ))),

                    FuzzyTerm {
                        fuzzy_distance: Some(distance),
                        is_prefix: true,
                        ..
                    } => Some(Box::new(FuzzyTermQuery::new(
                        tantivy::Term::from_field_text(field, &normalized_value),
                        *distance,
                        true,
                    ))),

                    FuzzyTerm {
                        fuzzy_distance: Some(distance),
                        is_prefix: false,
                        ..
                    } => Some(Box::new(FuzzyTermQuery::new_prefix(
                        tantivy::Term::from_field_text(field, &normalized_value),
                        *distance,
                        true,
                    ))),
                }
            }
        }
    }

    fn has_node_type(&self, node_type: NodeType, field: Field) -> Box<dyn Query> {
        let node_type = io_maps::node_type_to_u64(node_type);
        Box::new(TermQuery::new(tantivy::Term::from_field_u64(field, node_type), IndexRecordOption::Basic))
    }

    fn has_node_subtype(&self, node_subtype: &str, field: Field) -> Box<dyn Query> {
        Box::new(TermQuery::new(tantivy::Term::from_field_text(field, node_subtype), IndexRecordOption::Basic))
    }

    fn has_relation_label(&self, label: &str) -> Box<dyn Query> {
        Box::new(TermQuery::new(tantivy::Term::from_field_text(self.schema.label, &label), IndexRecordOption::Basic))
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
