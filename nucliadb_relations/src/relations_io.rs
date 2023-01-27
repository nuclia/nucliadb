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

use std::hash::Hash;

use data_encoding::HEXUPPER;
use nucliadb_core::protos::RelationMetadata;
use ring::digest::{Context, SHA256};
use serde::{Deserialize, Serialize};

pub fn compute_hash<D: AsRef<[u8]>>(data: &[D]) -> String {
    let mut context = Context::new(&SHA256);
    data.iter().for_each(|d| context.update(d.as_ref()));
    let digest = context.finish();
    HEXUPPER.encode(digest.as_ref())
}

#[derive(Default, Debug, Serialize, Deserialize, Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
pub enum Source {
    #[default]
    Null,
    User,
    System,
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, Ord, PartialEq, PartialOrd)]
pub struct IoNode {
    source: Source,
    name: String,
    xtype: String,
    subtype: Option<String>,
    hash: String,
}
impl IoNode {
    fn inner_new(source: Source, name: String, xtype: String, subtype: Option<String>) -> IoNode {
        let hash = compute_hash(&[
            name.as_bytes(),
            xtype.as_bytes(),
            subtype.as_ref().map(|s| s.as_bytes()).unwrap_or(&[]),
        ]);
        IoNode {
            name,
            xtype,
            subtype,
            hash,
            source,
        }
    }
    pub fn user_node(name: String, xtype: String, subtype: Option<String>) -> IoNode {
        IoNode::inner_new(Source::User, name, xtype, subtype)
    }
    pub fn system_node(name: String, xtype: String, subtype: Option<String>) -> IoNode {
        IoNode::inner_new(Source::System, name, xtype, subtype)
    }
    pub fn new(name: String, xtype: String, subtype: Option<String>) -> IoNode {
        IoNode::inner_new(Source::default(), name, xtype, subtype)
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn xtype(&self) -> &str {
        &self.xtype
    }
    pub fn subtype(&self) -> Option<&str> {
        self.subtype.as_deref()
    }
    pub fn defined_by_user(&self) -> bool {
        self.source == Source::User
    }
    pub fn hash(&self) -> &str {
        &self.hash
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct IoEdgeMetadata {
    pub paragraph_id: Option<String>,
    pub source_start: Option<i32>,
    pub source_end: Option<i32>,
    pub to_start: Option<i32>,
    pub to_end: Option<i32>,
}
impl From<RelationMetadata> for IoEdgeMetadata {
    fn from(value: RelationMetadata) -> Self {
        IoEdgeMetadata {
            paragraph_id: value.paragraph_id,
            source_start: value.source_start,
            source_end: value.source_end,
            to_start: value.to_start,
            to_end: value.to_end,
        }
    }
}

impl From<IoEdgeMetadata> for RelationMetadata {
    fn from(value: IoEdgeMetadata) -> Self {
        RelationMetadata {
            paragraph_id: value.paragraph_id,
            source_start: value.source_start,
            source_end: value.source_end,
            to_start: value.to_start,
            to_end: value.to_end,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct IoEdge {
    xtype: String,
    subtype: Option<String>,
}
impl IoEdge {
    pub fn new(xtype: String, subtype: Option<String>) -> IoEdge {
        IoEdge { xtype, subtype }
    }
    pub fn xtype(&self) -> &str {
        &self.xtype
    }
    pub fn subtype(&self) -> Option<&str> {
        self.subtype.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_db::*;
    #[test]
    fn graph_insertion() {
        let dir = tempfile::tempdir().unwrap();
        let graph = GraphDB::new(dir.path(), 1048576 * 100000).unwrap();
        let node1 = IoNode::new("N1".to_string(), "T1".to_string(), Some("ST1".to_string()));
        let node1p = IoNode::new("N1".to_string(), "T1".to_string(), None);
        let node2 = IoNode::new("N2".to_string(), "T2".to_string(), Some("ST2".to_string()));
        let mut txn = graph.rw_txn().unwrap();
        let node1_uid = graph.add_node(&mut txn, &node1).unwrap();
        let node1_uidf = graph.add_node(&mut txn, &node1).unwrap();
        assert_eq!(node1_uid, node1_uidf);
        let node1p_uid = graph.add_node(&mut txn, &node1p).unwrap();
        assert_ne!(node1_uid, node1p_uid);
        let node2_uid = graph.add_node(&mut txn, &node2).unwrap();
        assert_ne!(node1_uid, node2_uid);
        assert_ne!(node1p_uid, node2_uid);
    }
}
