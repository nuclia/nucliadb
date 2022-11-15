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

use std::path::Path;

use heed::flags::Flags;
use heed::types::{ByteSlice, Str, Unit};
use heed::{Database, Env, EnvOpenOptions, RoIter, RoPrefix, RoTxn, RwTxn};
use nucliadb_byte_rpr::*;

use crate::edge::*;
use crate::node::*;

mod db_name {
    pub const KEYS: &str = "KEYS_LMDB";
    pub const INVERSE_KEYS: &str = "INVERSE_KEYS_LMDB";
    pub const EDGES: &str = "EDGES_LMDB";
    pub const INVERSE_EDGES: &str = "INVERSE_EDGES_LMDB";
    pub const STATE: &str = "STATE_LMDB";
}

mod env_config {
    pub const ENV: &str = "ENV_lmdb";
    pub const STAMP: &str = "stamp.nuclia";
    pub const MAP_SIZE: usize = 1048576 * 100000;
    pub const MAX_DBS: u32 = 5;
}

mod state_fields {
    pub const FRESH_NODE: &str = "fresh_node";
}

pub struct RoToken<'a>(RoTxn<'a>);
impl<'a> RoToken<'a> {
    pub fn commit(self) -> Result<(), heed::Error> {
        self.0.commit()
    }
}
impl<'a> std::ops::Deref for RoToken<'a> {
    type Target = RoTxn<'a>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl<'a> std::ops::DerefMut for RoToken<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

pub struct RwToken<'a>(RwTxn<'a, 'a>);
impl<'a> RwToken<'a> {
    pub fn commit(self) -> Result<(), heed::Error> {
        self.0.commit()
    }
}
impl<'a> std::ops::Deref for RwToken<'a> {
    type Target = RwTxn<'a, 'a>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl<'a> std::ops::DerefMut for RwToken<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

pub struct QueryIter<'a> {
    iter: RoPrefix<'a, Str, Unit>,
}
impl<'a> Iterator for QueryIter<'a> {
    type Item = Edge;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter
            .next()
            .transpose()
            .unwrap()
            .map(|(k, _)| Edge::from(k))
    }
}

pub struct NodeIter<'a> {
    iter: RoIter<'a, Str, ByteSlice>,
}
impl<'a> Iterator for NodeIter<'a> {
    type Item = String;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter
            .next()
            .transpose()
            .unwrap()
            .map(|(k, _)| k.to_string())
    }
}

pub struct StorageSystem {
    env: Env,
    // key -> NodeId
    keys: Database<Str, ByteSlice>,
    // NodeID -> key
    inv_keys: Database<ByteSlice, Str>,
    // Edges of the graph
    edges: Database<Str, Unit>,
    // In edges of the graph
    in_edges: Database<Str, Unit>,
    // Name of the field -> current value
    state: Database<Str, ByteSlice>,
}

impl StorageSystem {
    pub fn create(path: &Path) -> StorageSystem {
        let env_path = path.join(env_config::ENV);
        std::fs::create_dir_all(&env_path).unwrap();
        let mut env_builder = EnvOpenOptions::new();
        env_builder.max_dbs(env_config::MAX_DBS);
        env_builder.map_size(env_config::MAP_SIZE);
        unsafe {
            env_builder.flag(Flags::MdbNoLock);
        }
        let env = env_builder.open(&env_path).expect("Opening env failed");
        let keys = env
            .create_database(Some(db_name::KEYS))
            .expect("Keys db could not be created");
        let inv_keys = env
            .create_database(Some(db_name::INVERSE_KEYS))
            .expect("InvKeys db could not be created");
        let edges = env
            .create_database(Some(db_name::EDGES))
            .expect("Edges db could not be created");
        let in_edges = env
            .create_database(Some(db_name::INVERSE_EDGES))
            .expect("InEdges db could not be created");
        let state = env
            .create_database(Some(db_name::STATE))
            .expect("State db could not be created");
        std::fs::File::create(path.join(env_config::STAMP)).expect("Stamp could not be created");
        StorageSystem {
            env,
            keys,
            inv_keys,
            edges,
            in_edges,
            state,
        }
    }

    pub fn rw_txn(&self) -> RwToken<'_> {
        RwToken(self.env.write_txn().unwrap())
    }
    pub fn ro_txn(&self) -> RoToken<'_> {
        RoToken(self.env.read_txn().unwrap())
    }
    pub fn add_node(&self, txn: &mut RwTxn, value: String) -> bool {
        println!("Adding node: {value}");
        let mut had_effect = false;
        if self.keys.get(txn, &value).unwrap().is_none() {
            let node_id = self.get_fresh_node_id(txn);
            println!("Adding to keys");
            self.keys.put(txn, &value, &node_id.as_byte_rpr()).unwrap();
            println!("Adding inv keys");
            self.inv_keys
                .put(txn, &node_id.as_byte_rpr(), &value)
                .unwrap();
            println!("End");
            had_effect = true;
        }
        had_effect
    }
    pub fn delete_node(&self, txn: &mut RwTxn, id: NodeId) {
        let node_value = self.get_node(txn, id).unwrap();
        let delete_query = PartialEdge::all(id);
        self.keys.delete(txn, &node_value).unwrap();
        self.inv_keys.delete(txn, &id.as_byte_rpr()).unwrap();
        self.delete_matches(txn, delete_query.clone());
        let to_delete: Vec<_> = self
            .match_edges_with_db(&self.in_edges, txn, delete_query)
            .map(|edge| (edge.clone(), edge.inverse()))
            .collect();
        for (inverse, original) in to_delete {
            self.edges.delete(txn, &original.to_string()).unwrap();
            self.in_edges.delete(txn, &inverse.to_string()).unwrap();
        }
    }
    pub fn add_edge(&self, txn: &mut RwTxn, edge: Edge) -> bool {
        let edge_fmt = edge.to_string();
        let in_edge_fmt = edge.inverse().to_string();
        let mut had_effect = false;
        if self.edges.get(txn, &edge_fmt).unwrap().is_none() {
            self.edges.put(txn, &edge_fmt, &()).unwrap();
            self.in_edges.put(txn, &in_edge_fmt, &()).unwrap();
            had_effect = true;
        }
        had_effect
    }
    pub fn get_id(&self, txn: &RoTxn, value: &str) -> Option<NodeId> {
        self.keys
            .get(txn, value)
            .unwrap()
            .map(NodeId::from_byte_rpr)
    }
    pub fn get_node(&self, txn: &RoTxn, id: NodeId) -> Option<String> {
        self.inv_keys
            .get(txn, &id.as_byte_rpr())
            .unwrap()
            .map(String::from)
    }
    pub fn match_edges<'a>(&'a self, txn: &'a RoTxn, query: PartialEdge) -> QueryIter<'a> {
        self.match_edges_with_db(&self.edges, txn, query)
    }
    pub fn delete_matches(&self, txn: &mut RwTxn, query: PartialEdge) {
        let edges: Vec<_> = self
            .match_edges(txn, query)
            .map(|edge| (edge.clone(), edge.inverse()))
            .collect();
        for (edge, inverse) in edges {
            self.edges.delete(txn, &edge.to_string()).unwrap();
            self.in_edges.delete(txn, &inverse.to_string()).unwrap();
        }
    }
    pub fn get_keys<'a>(&self, txn: &'a RoTxn) -> NodeIter<'a> {
        NodeIter {
            iter: self.keys.iter(txn).unwrap(),
        }
    }
    pub fn no_nodes(&self, txn: &RoTxn) -> u64 {
        self.keys.len(txn).unwrap()
    }
    fn get_fresh_node_id(&self, txn: &mut RwTxn) -> NodeId {
        let mut fresh = self
            .state
            .get(txn, state_fields::FRESH_NODE)
            .unwrap()
            .map(NodeId::from_byte_rpr)
            .unwrap_or_else(NodeId::new);
        let current = fresh.next();
        self.state
            .put(txn, state_fields::FRESH_NODE, &fresh.as_byte_rpr())
            .unwrap();
        current
    }
    fn match_edges_with_db<'a>(
        &self,
        db: &'a Database<Str, Unit>,
        txn: &'a RoTxn,
        query: PartialEdge,
    ) -> QueryIter<'a> {
        let query_formated = query.to_string();
        let iter = db.prefix_iter(txn, &query_formated).unwrap();
        QueryIter { iter }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn initialize_storage_system() -> StorageSystem {
        let dir = tempfile::tempdir().unwrap();
        StorageSystem::create(dir.path())
    }

    #[test]
    fn open_and_create() {
        let dir = tempfile::tempdir().unwrap();
        StorageSystem::create(dir.path());
        StorageSystem::create(dir.path());
    }

    const CHILD: &str = "child";
    const ABOUT: &str = "about";

    fn child(from: NodeId, to: NodeId) -> Edge {
        Edge::new(from, EdgeType::from(CHILD), to)
    }
    fn about(from: NodeId, to: NodeId) -> Edge {
        Edge::new(from, EdgeType::from(ABOUT), to)
    }
    fn par_child(from: NodeId, to: Option<NodeId>) -> PartialEdge {
        PartialEdge::with_type(from, EdgeType::from(CHILD), to)
    }
    fn par_about(from: NodeId, to: Option<NodeId>) -> PartialEdge {
        PartialEdge::with_type(from, EdgeType::from(ABOUT), to)
    }

    #[test]
    fn add_resource() {
        let system = initialize_storage_system();
        let mut txn = system.rw_txn();
        let resource = "Name".to_string();
        assert!(system.add_node(&mut txn, resource));
        txn.commit().unwrap();
        let txn = system.ro_txn();
        assert!(system.get_id(&txn, "Name").is_some());
        assert!(system.get_id(&txn, "Nonexistent").is_none());
        assert!(system
            .get_node(&txn, system.get_id(&txn, "Name").unwrap())
            .is_some());
        assert_eq!(
            system
                .get_node(&txn, system.get_id(&txn, "Name").unwrap())
                .unwrap(),
            "Name"
        );
    }
    #[test]
    fn same_resource_same_id() {
        let system = initialize_storage_system();
        let mut txn = system.rw_txn();
        let resource0 = "Name".to_string();
        let resource1 = "Name".to_string();
        assert!(system.add_node(&mut txn, resource0));
        assert!(!system.add_node(&mut txn, resource1));
        txn.commit().unwrap();
    }
    #[test]
    fn different_resource_different_id() {
        let system = initialize_storage_system();
        let mut txn = system.rw_txn();
        let resource0 = "Name0".to_string();
        let resource1 = "Name1".to_string();
        assert!(system.add_node(&mut txn, resource0));
        assert!(system.add_node(&mut txn, resource1));
        assert!(system.get_id(&txn, "Name0").is_some());
        assert!(system.get_id(&txn, "Name1").is_some());
        txn.commit().unwrap();
        let txn = system.ro_txn();
        assert_ne!(
            system.get_id(&txn, "Name0").unwrap(),
            system.get_id(&txn, "Name1").unwrap()
        );
        assert_ne!(
            system
                .get_node(&txn, system.get_id(&txn, "Name0").unwrap())
                .unwrap(),
            system
                .get_node(&txn, system.get_id(&txn, "Name1").unwrap())
                .unwrap()
        );
    }
    #[test]
    fn graph_querying() {
        use std::collections::HashSet;
        let system = initialize_storage_system();
        let mut txn = system.rw_txn();
        let r0 = "R0".to_string();
        let r1 = "R1".to_string();
        let l0 = "L0".to_string();
        let l1 = "L1".to_string();
        system.add_node(&mut txn, r0);
        system.add_node(&mut txn, r1);
        system.add_node(&mut txn, l0);
        system.add_node(&mut txn, l1);
        let r0 = system.get_id(&txn, "R0").unwrap();
        let r1 = system.get_id(&txn, "R1").unwrap();
        let l0 = system.get_id(&txn, "L0").unwrap();
        let l1 = system.get_id(&txn, "L1").unwrap();
        assert_eq!(system.no_nodes(&txn), 4);

        // Edges
        let edges_r0 = HashSet::from([child(r0, r1), about(r0, l0)]);
        let edges_r1 = HashSet::from([about(r1, l0), about(r1, l1)]);
        assert!(edges_r0
            .iter()
            .cloned()
            .all(|edge| system.add_edge(&mut txn, edge)));
        assert!(edges_r1
            .iter()
            .cloned()
            .all(|edge| system.add_edge(&mut txn, edge)));
        assert!(edges_r0
            .iter()
            .cloned()
            .all(|edge| !system.add_edge(&mut txn, edge)));
        assert!(edges_r1
            .iter()
            .cloned()
            .all(|edge| !system.add_edge(&mut txn, edge)));
        txn.commit().unwrap();
        let txn = system.ro_txn();
        {
            let result: HashSet<_> = system.match_edges(&txn, PartialEdge::all(r0)).collect();
            assert_eq!(result, edges_r0);
            let result: HashSet<_> = system.match_edges(&txn, PartialEdge::all(r1)).collect();
            assert_eq!(result, edges_r1);
        }
        {
            let result: HashSet<_> = system.match_edges(&txn, par_child(r0, None)).collect();
            assert_eq!(result, HashSet::from([child(r0, r1)]));
            let result: HashSet<_> = system.match_edges(&txn, par_child(r1, None)).collect();
            assert_eq!(result, HashSet::from([]));
        }
        {
            let result: HashSet<_> = system.match_edges(&txn, par_child(r0, None)).collect();
            assert_eq!(result, HashSet::from([child(r0, r1)]));
            let result: HashSet<_> = system.match_edges(&txn, par_child(r1, None)).collect();
            assert_eq!(result, HashSet::from([]));
        }
        {
            let result: HashSet<_> = system.match_edges(&txn, par_about(r0, None)).collect();
            assert_eq!(result, HashSet::from([about(r0, l0)]));
            let result: HashSet<_> = system.match_edges(&txn, par_about(r1, None)).collect();
            assert_eq!(result, HashSet::from([about(r1, l0), about(r1, l1)]))
        }
        {
            let results_r0: HashSet<_> = system
                .match_edges(&txn, par_about(r0, None))
                .map(|edge| edge.to)
                .collect();
            let results_r1: HashSet<_> = system
                .match_edges(&txn, par_about(r1, None))
                .map(|edge| edge.to)
                .collect();
            let intersection = HashSet::from([l0]);
            assert_eq!(results_r0.intersection(&results_r1).count(), 1);
            assert!(results_r0
                .intersection(&results_r1)
                .all(|e| intersection.contains(e)));
        }
        {
            let all_edges: HashSet<_> = edges_r0.union(&edges_r1).cloned().collect();
            let matches: HashSet<_> = system.match_edges(&txn, PartialEdge::wildcard()).collect();
            assert_eq!(all_edges, matches);
        }
    }
    #[test]
    fn query_delete() {
        let system = initialize_storage_system();
        let mut txn = system.rw_txn();
        let r0 = "R0".to_string();
        let r1 = "R1".to_string();
        system.add_node(&mut txn, r0);
        system.add_node(&mut txn, r1);
        let r0_id = system.get_id(&txn, "R0").unwrap();
        let r1_id = system.get_id(&txn, "R1").unwrap();
        system.add_edge(&mut txn, child(r0_id, r1_id));
        system.add_edge(&mut txn, child(r1_id, r0_id));
        system.delete_matches(&mut txn, PartialEdge::all(r0_id));
        assert_eq!(system.match_edges(&txn, PartialEdge::all(r0_id)).count(), 0);
        assert_eq!(system.match_edges(&txn, PartialEdge::all(r1_id)).count(), 1);
    }
    #[test]
    fn delete_full_node() {
        let system = initialize_storage_system();
        let mut txn = system.rw_txn();
        let r0 = "R0".to_string();
        let r1 = "R1".to_string();
        system.add_node(&mut txn, r0);
        system.add_node(&mut txn, r1);
        let r0_id = system.get_id(&txn, "R0").unwrap();
        let r1_id = system.get_id(&txn, "R1").unwrap();
        system.add_edge(&mut txn, child(r0_id, r1_id));
        system.add_edge(&mut txn, child(r1_id, r0_id));
        system.delete_node(&mut txn, r0_id);
        assert_eq!(system.match_edges(&txn, PartialEdge::all(r0_id)).count(), 0);
        assert_eq!(system.match_edges(&txn, PartialEdge::all(r1_id)).count(), 0);
    }
}
