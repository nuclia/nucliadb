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

use std::fmt::Display;
use std::hash::Hash;

use crate::node::*;

fn apply_encoding(from: Option<NodeId>, etype: Option<EdgeType>, to: Option<NodeId>) -> String {
    let from = from.map_or_else(String::new, |n| format!("[{n}/"));
    let etype = etype.map_or_else(String::new, |e| format!("{e}/"));
    let to = to.map_or_else(String::new, |n| format!("{n}]"));
    format!("{}{}{}", from, etype, to)
}

fn apply_decoding(encoded: &str) -> (Option<NodeId>, Option<EdgeType>, Option<NodeId>) {
    let raw = encoded
        .strip_prefix('[')
        .unwrap()
        .strip_suffix(']')
        .unwrap();
    let parts: Vec<&str> = raw.split('/').collect();
    let from = parts.first().copied().map(NodeId::from);
    let edge = parts.get(1).copied().map(EdgeType::from);
    let to = parts.get(2).copied().map(NodeId::from);
    (from, edge, to)
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EdgeType(String);

impl Display for EdgeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let EdgeType(value) = self;
        write!(f, "{value}")
    }
}
impl From<&str> for EdgeType {
    fn from(raw: &str) -> Self {
        EdgeType(raw.to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Edge {
    pub ctype: EdgeType,
    pub from: NodeId,
    pub to: NodeId,
}
impl Display for Edge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let encoded = apply_encoding(Some(self.from), Some(self.ctype.clone()), Some(self.to));
        write!(f, "{encoded}")
    }
}

impl From<&str> for Edge {
    fn from(raw: &str) -> Self {
        let (from, ctye, to) = apply_decoding(raw);
        Edge {
            from: from.unwrap(),
            ctype: ctye.unwrap(),
            to: to.unwrap(),
        }
    }
}
impl Edge {
    pub fn new(from: NodeId, ctype: EdgeType, to: NodeId) -> Edge {
        Edge { ctype, from, to }
    }
    pub fn inverse(self) -> Edge {
        Edge {
            from: self.to,
            to: self.from,
            ..self
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PartialEdge {
    pub from: Option<NodeId>,
    pub to: Option<NodeId>,
    pub qtype: Option<EdgeType>,
}

impl Display for PartialEdge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let encoded = apply_encoding(self.from, self.qtype.clone(), self.to);
        if !encoded.is_empty() {
            write!(f, "{encoded}")
        } else {
            write!(f, "[")
        }
    }
}

impl PartialEdge {
    fn new(from: Option<NodeId>, qtype: Option<EdgeType>, to: Option<NodeId>) -> PartialEdge {
        PartialEdge { from, qtype, to }
    }
    #[allow(unused)]
    pub fn with_type(from: NodeId, qtype: EdgeType, to: Option<NodeId>) -> PartialEdge {
        PartialEdge::new(Some(from), Some(qtype), to)
    }
    pub fn all(from: NodeId) -> PartialEdge {
        PartialEdge::new(Some(from), None, None)
    }
    pub fn wildcard() -> PartialEdge {
        PartialEdge::new(None, None, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn from_to_string() {
        const CHILD: &str = "child";
        const ABOUT: &str = "about";
        const ENTITY: &str = "entity";
        const COLAB: &str = "COLAB";
        fn child(from: NodeId, to: NodeId) -> Edge {
            Edge::new(from, EdgeType(CHILD.to_string()), to)
        }
        fn about(from: NodeId, to: NodeId) -> Edge {
            Edge::new(from, EdgeType(ABOUT.to_string()), to)
        }
        fn entity(from: NodeId, to: NodeId) -> Edge {
            Edge::new(from, EdgeType(ENTITY.to_string()), to)
        }
        fn colab(from: NodeId, to: NodeId) -> Edge {
            Edge::new(from, EdgeType(COLAB.to_string()), to)
        }
        fn par_child(from: NodeId, to: Option<NodeId>) -> PartialEdge {
            PartialEdge::with_type(from, EdgeType(CHILD.to_string()), to)
        }
        fn par_about(from: NodeId, to: Option<NodeId>) -> PartialEdge {
            PartialEdge::with_type(from, EdgeType(ABOUT.to_string()), to)
        }
        fn par_entity(from: NodeId, to: Option<NodeId>) -> PartialEdge {
            PartialEdge::with_type(from, EdgeType(ENTITY.to_string()), to)
        }
        fn par_colab(from: NodeId, to: Option<NodeId>) -> PartialEdge {
            PartialEdge::with_type(from, EdgeType(COLAB.to_string()), to)
        }
        let mut node_id = NodeId::new();
        let (from, to) = (node_id.next(), node_id.next());
        let child_edge = child(from, to);
        let child_query = par_child(from, Some(to));

        let (from, to) = (node_id.next(), node_id.next());
        let about_edge = about(from, to);
        let about_query = par_about(from, Some(to));

        let (from, to) = (node_id.next(), node_id.next());
        let entity_edge = entity(from, to);
        let entity_query = par_entity(from, Some(to));

        let (from, to) = (node_id.next(), node_id.next());
        let colab_edge = colab(from, to);
        let colab_query = par_colab(from, Some(to));

        assert_eq!(child_edge, Edge::from(child_edge.to_string().as_str()));
        assert_eq!(child_edge, Edge::from(child_query.to_string().as_str()));
        assert_eq!(about_edge, Edge::from(about_edge.to_string().as_str()));
        assert_eq!(about_edge, Edge::from(about_query.to_string().as_str()));
        assert_eq!(entity_edge, Edge::from(entity_edge.to_string().as_str()));
        assert_eq!(entity_edge, Edge::from(entity_query.to_string().as_str()));
        assert_eq!(colab_edge, Edge::from(colab_edge.to_string().as_str()));
        assert_eq!(colab_edge, Edge::from(colab_query.to_string().as_str()));
    }
}
