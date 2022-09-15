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
use std::collections::{HashMap, HashSet, LinkedList};

use crate::graph::*;
use crate::string_normalization::normalize;

#[derive(Default, derive_builder::Builder, Debug)]
#[builder(name = "QueryConstructor", pattern = "owned")]
pub struct Query {
    #[builder(setter(skip))]
    types_allowed: HashSet<(usize, usize)>,
    #[builder(setter(skip))]
    #[builder(default = "HashMap::new()")]
    type_storage: HashMap<String, usize>,
    #[builder(default = "HashSet::new()")]
    always_jump: HashSet<EdgeType>,
    #[builder(default = "String::new()")]
    #[builder(setter(custom))]
    prefixed: String,
    #[builder(default = "0")]
    depth: u32,
}
impl QueryConstructor {
    pub fn prefixed(mut self, prefix: String) -> Self {
        self.prefixed = Some(normalize(&prefix));
        self
    }
}
impl Query {
    pub fn add_types(&mut self, ntype: String, subtype: String) {
        let len = self.type_storage.len();
        let ntype_p = *self.type_storage.entry(ntype).or_insert(len);
        let len = self.type_storage.len();
        let subtype_p = *self.type_storage.entry(subtype).or_insert(len);
        self.types_allowed.insert((ntype_p, subtype_p));
    }
    fn type_qualifies(&self, node: &Node) -> bool {
        let type_reg = self.type_storage.get(node.get_type()).copied();
        let subtype_reg = self.type_storage.get(node.get_subtype()).copied();
        let subtype_wildcard = self.type_storage.get("").copied();
        let mix = type_reg.and_then(|t| subtype_reg.map(|st| (t, st)));
        let mix_wc = type_reg.and_then(|t| subtype_wildcard.map(|st| (t, st)));
        self.types_allowed.is_empty()
            || mix.map_or(false, |v| self.types_allowed.contains(&v))
            || mix_wc.map_or(false, |v| self.types_allowed.contains(&v))
    }
    fn prefix_qualifies(&self, node: &Node) -> bool {
        node.get_normalized().starts_with(&self.prefixed)
    }
    pub fn qualifies(&self, node: &Node) -> bool {
        self.prefix_qualifies(node) && self.type_qualifies(node)
    }
    pub fn always_jump(&self, edge: &Edge) -> bool {
        self.always_jump.contains(&edge.ctype)
    }
}

pub struct QueryMatches {
    pub matches: Vec<NodeId>,
}

struct RoutePoint {
    id: NodeId,
    node: Node,
    depth: u32,
}

impl RoutePoint {
    pub fn expand(
        &self,
        txn: &RoToken,
        sys: &StorageSystem,
        visited: &HashSet<NodeId>,
        query: &Query,
    ) -> Vec<RoutePoint> {
        sys.match_edges(txn, PartialEdge::all(self.id))
            .filter(|edge| !visited.contains(&edge.to))
            .map(|edge| (query.always_jump(&edge), edge))
            .map(|(always, edge)| RoutePoint {
                id: edge.to,
                node: sys.get_node(txn, edge.to).unwrap().into(),
                depth: if always { self.depth } else { self.depth + 1 },
            })
            .collect()
    }
    pub fn new(id: NodeId, sys: &StorageSystem, txn: &RoToken) -> RoutePoint {
        RoutePoint {
            id,
            depth: 0,
            node: sys.get_node(txn, id).unwrap().into(),
        }
    }
}

fn standard_query(entry_points: &[NodeId], storage: &StorageSystem, query: Query) -> QueryMatches {
    let txn = storage.ro_txn();
    let mut work: LinkedList<_> = entry_points
        .iter()
        .copied()
        .map(|ep| RoutePoint::new(ep, storage, &txn))
        .collect();
    let mut visited: HashSet<_> = entry_points.iter().copied().collect();
    let mut matches = Vec::new();
    while let Some(point) = work.pop_back() {
        if query.qualifies(&point.node) {
            matches.push(point.id);
        }
        for value in point.expand(&txn, storage, &visited, &query) {
            if value.depth <= query.depth && !visited.contains(&value.id) {
                visited.insert(value.id);
                work.push_front(value);
            }
        }
    }
    QueryMatches { matches }
}

fn all_prefixed(storage: &StorageSystem, query: Query) -> QueryMatches {
    let txn = storage.ro_txn();
    let matches: Vec<_> = storage
        .get_keys(&txn)
        .map(Node::from)
        .filter(|node| query.qualifies(node))
        .map(|node| storage.get_id(&txn, &node.to_string()).unwrap())
        .collect();
    QueryMatches { matches }
}

pub fn process_query(
    entry_points: &[NodeId],
    storage: &StorageSystem,
    query: Query,
) -> QueryMatches {
    if entry_points.is_empty() {
        all_prefixed(storage, query)
    } else {
        standard_query(entry_points, storage, query)
    }
}

pub fn get_node_types(storage: &StorageSystem) -> HashSet<(String, String)> {
    let txn = storage.ro_txn();
    storage
        .get_keys(&txn)
        .map(Node::from)
        .map(|node| (node.get_type().to_string(), node.get_subtype().to_string()))
        .collect()
}

pub fn get_edge_types(storage: &StorageSystem) -> HashSet<String> {
    storage
        .match_edges(&storage.ro_txn(), PartialEdge::wildcard())
        .map(|edge| edge.ctype.to_string())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    lazy_static::lazy_static! {
        pub static ref NA: Node = Node::from(r#"{ "normalized": "book_a", "value": "Book_A", "type": "other", "subtype": "" }"#);
        pub static ref NB: Node = Node::from(r#"{ "normalized": "book_b", "value": "Book_B", "type": "other", "subtype": "" }"#);
        pub static ref NC: Node = Node::from(r#"{ "normalized": "actor_c", "value": "Actor_C","type": "other", "subtype": "person" }"#);
        pub static ref ND: Node = Node::from(r#"{ "normalized": "film_d", "value": "Film_D", "type": "other", "subtype": "" }"#);
    }
    const E_CNX0: &str = "other";
    const E_CNX1: &str = "some_other";

    fn generate_graph() -> StorageSystem {
        let dir = tempfile::tempdir().unwrap();
        let system = StorageSystem::create(dir.path());
        let mut txn = system.rw_txn();
        {
            system.add_node(&mut txn, NA.to_string());
            system.add_node(&mut txn, NB.to_string());
            system.add_node(&mut txn, NC.to_string());
            system.add_node(&mut txn, ND.to_string());
        }
        {
            let a_id = system.get_id(&txn, &NA.to_string()).unwrap();
            let b_id = system.get_id(&txn, &NB.to_string()).unwrap();
            let c_id = system.get_id(&txn, &NC.to_string()).unwrap();
            let d_id = system.get_id(&txn, &ND.to_string()).unwrap();

            system.add_edge(&mut txn, Edge::new(a_id, EdgeType::from(E_CNX0), b_id));
            system.add_edge(&mut txn, Edge::new(b_id, EdgeType::from(E_CNX1), c_id));
            system.add_edge(&mut txn, Edge::new(c_id, EdgeType::from(E_CNX0), d_id));
            system.add_edge(&mut txn, Edge::new(d_id, EdgeType::from(E_CNX1), a_id));
        }
        txn.commit().unwrap();
        system
    }
    #[test]
    fn search() {
        let graph = generate_graph();
        let txn = graph.ro_txn();
        let a_id = graph.get_id(&txn, &NA.to_string()).unwrap();
        let b_id = graph.get_id(&txn, &NB.to_string()).unwrap();
        let c_id = graph.get_id(&txn, &NC.to_string()).unwrap();
        let entry_points = vec![a_id, b_id];
        let mut query = QueryConstructor::default().depth(1).build().unwrap();
        HashSet::from([("other".to_string(), "person".to_string())])
            .into_iter()
            .for_each(|(t, st)| query.add_types(t, st));
        let QueryMatches { matches } = process_query(&entry_points, &graph, query);
        assert_eq!(matches, vec![c_id]);
    }

    #[test]
    fn prefix_search() {
        let graph = generate_graph();
        let txn = graph.ro_txn();
        let a_id = graph.get_id(&txn, &NA.to_string()).unwrap();
        let b_id = graph.get_id(&txn, &NB.to_string()).unwrap();
        let query = QueryConstructor::default()
            .depth(1)
            .prefixed("book".to_string())
            .build()
            .unwrap();
        let QueryMatches { matches } = process_query(&[], &graph, query);
        let lower_cases: HashSet<_> = matches.into_iter().collect();

        let query = QueryConstructor::default()
            .depth(1)
            .prefixed("BOOk".to_string())
            .build()
            .unwrap();
        let QueryMatches { matches } = process_query(&[], &graph, query);
        let with_caps: HashSet<_> = matches.into_iter().collect();

        let expected = HashSet::from([a_id, b_id]);
        assert_eq!(lower_cases, expected);
        assert_eq!(with_caps, expected);
    }

    #[test]
    fn not_enough_depth() {
        let graph = generate_graph();
        let txn = graph.ro_txn();
        let a_id = graph.get_id(&txn, &NA.to_string()).unwrap();
        let b_id = graph.get_id(&txn, &NB.to_string()).unwrap();
        let entry_points = vec![a_id, b_id];
        let mut query = QueryConstructor::default().depth(0).build().unwrap();
        HashSet::from([("other".to_string(), "person".to_string())])
            .into_iter()
            .for_each(|(t, st)| query.add_types(t, st));
        let QueryMatches { matches } = process_query(&entry_points, &graph, query);
        assert!(matches.is_empty());
    }

    #[test]
    fn use_wildcard_type() {
        let graph = generate_graph();
        let txn = graph.ro_txn();
        let a_id = graph.get_id(&txn, &NA.to_string()).unwrap();
        let b_id = graph.get_id(&txn, &NB.to_string()).unwrap();
        let c_id = graph.get_id(&txn, &NC.to_string()).unwrap();
        let d_id = graph.get_id(&txn, &ND.to_string()).unwrap();
        let entry_points = vec![a_id];
        let mut query = QueryConstructor::default().depth(4).build().unwrap();
        HashSet::from([("other".to_string(), "".to_string())])
            .into_iter()
            .for_each(|(t, st)| query.add_types(t, st));
        let QueryMatches { matches } = process_query(&entry_points, &graph, query);
        let expected = HashSet::from([a_id, b_id, c_id, d_id]);
        let got: HashSet<_> = matches.into_iter().collect();
        assert_eq!(got, expected);
    }

    #[test]
    fn use_always_jump() {
        let graph = generate_graph();
        let txn = graph.ro_txn();
        let a_id = graph.get_id(&txn, &NA.to_string()).unwrap();
        let b_id = graph.get_id(&txn, &NB.to_string()).unwrap();
        let c_id = graph.get_id(&txn, &NC.to_string()).unwrap();
        let entry_points = vec![a_id, b_id];
        let mut query = QueryConstructor::default()
            .depth(0)
            .always_jump(HashSet::from([EdgeType::from(E_CNX1)]))
            .build()
            .unwrap();
        HashSet::from([("other".to_string(), "person".to_string())])
            .into_iter()
            .for_each(|(t, st)| query.add_types(t, st));
        let QueryMatches { matches } = process_query(&entry_points, &graph, query);
        let expected = HashSet::from([c_id]);
        let got: HashSet<_> = matches.into_iter().collect();
        assert_eq!(got, expected);
    }

    #[test]
    fn no_match_prefixed() {
        let graph = generate_graph();
        let txn = graph.ro_txn();
        let a_id = graph.get_id(&txn, &NA.to_string()).unwrap();
        let b_id = graph.get_id(&txn, &NB.to_string()).unwrap();
        let entry_points = vec![a_id, b_id];
        let mut query = QueryConstructor::default()
            .depth(1)
            .prefixed("Node".to_string())
            .build()
            .unwrap();
        HashSet::from([("other".to_string(), "person".to_string())])
            .into_iter()
            .for_each(|(t, st)| query.add_types(t, st));
        let QueryMatches { matches } = process_query(&entry_points, &graph, query);
        assert!(matches.is_empty());
    }

    #[test]
    fn node_types() {
        let graph = generate_graph();
        let expect = HashSet::from([
            ("other".to_string(), "".to_string()),
            ("other".to_string(), "person".to_string()),
        ]);
        let got = get_node_types(&graph);
        assert_eq!(expect, got);
    }

    #[test]
    fn edge_types() {
        let graph = generate_graph();
        let expect = HashSet::from([E_CNX0.to_string(), E_CNX1.to_string()]);
        let got = get_edge_types(&graph);
        assert_eq!(got, expect);
    }
}
