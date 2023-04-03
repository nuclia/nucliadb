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
use std::collections::HashSet;
use std::path::Path;

use heed::flags::Flags;
use heed::types::{SerdeBincode, Str, Unit};
use heed::{Database, Env, EnvOpenOptions, RoTxn, RwTxn};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::errors::{RResult, RelationsErr};
use super::relations_io::{IoEdge, IoEdgeMetadata, IoNode};

pub type RwToken<'a> = RwTxn<'a, 'a>;
pub type RoToken<'a> = RoTxn<'a>;
pub trait GIdProperty: Serialize + DeserializeOwned + 'static {}
impl<T: Serialize + DeserializeOwned + 'static> GIdProperty for T {}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct Entity(Uuid);

mod gid_impl {
    use uuid::Uuid;

    use super::Entity;
    impl Default for Entity {
        fn default() -> Self {
            Entity(Uuid::new_v4())
        }
    }
    impl Entity {
        pub fn new() -> Self {
            Self::default()
        }
    }
    impl std::fmt::Display for Entity {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            self.0.fmt(f)
        }
    }
}

fn encode_connexion(
    from: Option<Entity>,
    to: Option<Entity>,
    with: Option<Entity>,
) -> RResult<String> {
    match (from, to, with) {
        (Some(from), Some(to), Some(with)) => Ok(format!("({from},{to},{with})")),
        (Some(from), Some(to), None) => Ok(format!("({from},{to},")),
        (Some(from), None, None) => Ok(format!("({from},")),
        _ => Err(RelationsErr::UBehaviour),
    }
}
fn decode_connexion(elements: &str) -> (Entity, Entity, Entity) {
    let uuids: Vec<_> = elements
        .strip_prefix('(')
        .unwrap()
        .strip_suffix(')')
        .unwrap()
        .split(',')
        .map(|v| Uuid::parse_str(v).unwrap())
        .collect();
    (Entity(uuids[0]), Entity(uuids[1]), Entity(uuids[2]))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GCnx(Entity, Entity, Entity);
impl GCnx {
    fn decode(elems: &str) -> GCnx {
        let (from, to, with) = decode_connexion(elems);
        GCnx(from, to, with)
    }
    fn decode_inversed(elems: &str) -> GCnx {
        let (to, from, with) = decode_connexion(elems);
        GCnx(from, to, with)
    }
    fn encode(&self) -> RResult<String> {
        encode_connexion(Some(self.0), Some(self.1), Some(self.2))
    }
    pub fn new(from: Entity, to: Entity, with: Entity) -> GCnx {
        GCnx(from, to, with)
    }
    pub fn from(&self) -> Entity {
        self.0
    }
    pub fn to(&self) -> Entity {
        self.1
    }
    pub fn edge(&self) -> Entity {
        self.2
    }
}

pub struct GraphDB {
    env: Env,
    // Given a node enconding returns its Entity
    nodes: Database<Str, SerdeBincode<Entity>>,
    // Encodings for the out edges (source, edge, target)
    outedges: Database<Str, Unit>,
    // Encodings for the in edges (target, edge, source)
    inedges: Database<Str, Unit>,
    // Given an entity, it returns its edge component
    edge_component: Database<SerdeBincode<Entity>, SerdeBincode<IoEdge>>,
    // Given an entity, it returns its edge metadata. It is not stored
    // inside the IoEdge type to avoid the deserialization cost during BFS.
    edge_metadata: Database<SerdeBincode<Entity>, SerdeBincode<IoEdgeMetadata>>,
    // Given an entity, it returns its node component
    node_component: Database<SerdeBincode<Entity>, SerdeBincode<IoNode>>,
}

macro_rules! database_name {
    (const $l:ident) => {
        const $l: &str = stringify!($l);
    };
}
impl GraphDB {
    const MAX_DBS: u32 = 6;
    database_name!(const NODES);
    database_name!(const OUTEDGES);
    database_name!(const INEDGES);
    database_name!(const NODE_COMPONENT);
    database_name!(const EDGE_COMPONENT);
    database_name!(const EDGE_METADATA);
    pub fn new(path: &Path, db_size: usize) -> RResult<Self> {
        let mut env_builder = EnvOpenOptions::new();
        env_builder.max_dbs(Self::MAX_DBS);
        env_builder.map_size(db_size);
        unsafe {
            env_builder.flag(Flags::MdbNoLock);
        }
        let env = env_builder.open(path)?;
        let nodes = env.create_database(Some(Self::NODES))?;
        let outedges = env.create_database(Some(Self::OUTEDGES))?;
        let inedges = env.create_database(Some(Self::INEDGES))?;
        let node_component = env.create_database(Some(Self::NODE_COMPONENT))?;
        let edge_component = env.create_database(Some(Self::EDGE_COMPONENT))?;
        let edge_metadata = env.create_database(Some(Self::EDGE_METADATA))?;
        Ok(GraphDB {
            env,
            nodes,
            outedges,
            inedges,
            node_component,
            edge_component,
            edge_metadata,
        })
    }
    pub fn rw_txn(&self) -> RResult<RwToken<'_>> {
        Ok(self.env.write_txn()?)
    }
    pub fn ro_txn(&self) -> RResult<RoTxn<'_>> {
        Ok(self.env.read_txn()?)
    }
    pub fn add_node(&self, txn: &mut RwTxn, node: &IoNode) -> RResult<Entity> {
        match self.nodes.get(txn, node.hash())? {
            Some(v) => Ok(v),
            None => {
                let id = Entity::new();
                self.nodes.put(txn, node.hash(), &id)?;
                self.node_component.put(txn, &id, node)?;
                Ok(id)
            }
        }
    }
    pub fn delete_node(&self, txn: &mut RwTxn, node_id: Entity) -> RResult<HashSet<Entity>> {
        let node = self.get_node(txn, node_id)?;
        self.nodes.delete(txn, node.hash())?;
        self.node_component.delete(txn, &node_id)?;
        let prefix = encode_connexion(Some(node_id), None, None)?;
        // Deleting out connexions
        let mut affected = HashSet::new();
        let mut in_edges = vec![];
        let mut out_iter = self.outedges.prefix_iter_mut(txn, &prefix)?;
        while let Some((out_edge, _)) = out_iter.next().transpose()? {
            let in_edge = GCnx::decode_inversed(out_edge);
            affected.insert(in_edge.from());
            in_edges.push(in_edge);
            out_iter.del_current()?;
        }
        std::mem::drop(out_iter);
        for edge in in_edges.into_iter().map(|e| e.encode()) {
            let edge = edge?;
            self.inedges.delete(txn, &edge)?;
        }
        // Deleting in connexions
        let mut out_edges = vec![];
        let mut in_iter = self.inedges.prefix_iter_mut(txn, &prefix)?;
        while let Some((out_edge, _)) = in_iter.next().transpose()? {
            let out_edge = GCnx::decode_inversed(out_edge);
            affected.insert(out_edge.from());
            out_edges.push(out_edge);
            in_iter.del_current()?;
        }
        std::mem::drop(in_iter);
        for edge in out_edges.into_iter().map(|e| e.encode()) {
            let edge = edge?;
            self.outedges.delete(txn, &edge)?;
        }
        Ok(affected)
    }
    pub fn connect(
        &self,
        txn: &mut RwTxn,
        from: Entity,
        edge: &IoEdge,
        to: Entity,
        edge_metadata: Option<&IoEdgeMetadata>,
    ) -> RResult<bool> {
        let mut exits = false;
        let mut iter = self.connected_by(txn, from, to)?;
        loop {
            match iter.next() {
                _ if exits => break,
                Some(cnx) => {
                    let cnx = cnx?;
                    let cnx = self.get_edge(txn, cnx.edge())?;
                    exits = *edge == cnx;
                }
                None => {
                    std::mem::drop(iter);
                    let with = Entity::new();
                    let out_edge = GCnx::new(from, to, with).encode()?;
                    let in_edge = GCnx::new(to, from, with).encode()?;
                    self.edge_component.put(txn, &with, edge)?;
                    self.outedges.put(txn, &out_edge, &())?;
                    self.inedges.put(txn, &in_edge, &())?;
                    if let Some(metadata) = edge_metadata {
                        self.edge_metadata.put(txn, &with, metadata)?;
                    }
                    break;
                }
            }
        }
        Ok(!exits)
    }
    pub fn iter_node_ids<'a>(
        &self,
        txn: &'a RoTxn,
    ) -> RResult<impl Iterator<Item = RResult<Entity>> + 'a> {
        Ok(self
            .node_component
            .iter(txn)
            .map_err(RelationsErr::from)?
            .map(|r| r.map(|n| n.0).map_err(RelationsErr::from)))
    }
    pub fn iter_edge_ids<'a>(
        &self,
        txn: &'a RoTxn,
    ) -> RResult<impl Iterator<Item = RResult<Entity>> + 'a> {
        Ok(self
            .edge_component
            .iter(txn)
            .map_err(RelationsErr::from)?
            .map(|r| r.map(|n| n.0).map_err(RelationsErr::from)))
    }
    pub fn get_node(&self, txn: &RoTxn, x: Entity) -> RResult<IoNode> {
        let node = self.node_component.get(txn, &x)?;
        node.map_or_else(|| Err(RelationsErr::UBehaviour), Ok)
    }
    pub fn get_nodeid(&self, txn: &RoTxn, x: &str) -> RResult<Option<Entity>> {
        Ok(self.nodes.get(txn, x)?)
    }
    pub fn get_edge(&self, txn: &RoTxn, x: Entity) -> RResult<IoEdge> {
        let node = self.edge_component.get(txn, &x)?;
        node.map_or_else(|| Err(RelationsErr::UBehaviour), Ok)
    }
    pub fn get_edge_metadata(&self, txn: &RoTxn, x: Entity) -> RResult<Option<IoEdgeMetadata>> {
        Ok(self.edge_metadata.get(txn, &x)?)
    }
    pub fn connected_by<'a>(
        &self,
        txn: &'a RoTxn,
        x: Entity,
        y: Entity,
    ) -> RResult<impl Iterator<Item = RResult<GCnx>> + 'a> {
        let right_arrow = encode_connexion(Some(x), Some(y), None)?;
        let out = self.outedges.prefix_iter(txn, &right_arrow)?;
        let iter = out.map(|r| r.map_err(RelationsErr::from).map(|(v, _)| GCnx::decode(v)));
        Ok(iter)
    }
    pub fn get_outedges<'a>(
        &self,
        txn: &'a RoTxn,
        from: Entity,
    ) -> RResult<impl Iterator<Item = RResult<GCnx>> + 'a> {
        let prefix = encode_connexion(Some(from), None, None)?;
        let out = self.outedges.prefix_iter(txn, &prefix)?;
        let iter = out
            .map(|r| r.map_err(RelationsErr::from))
            .map(|r| r.map(|(v, _)| GCnx::decode(v)));
        Ok(iter)
    }
    pub fn get_inedges<'a>(
        &self,
        txn: &'a RoTxn,
        to: Entity,
    ) -> RResult<impl Iterator<Item = RResult<GCnx>> + 'a> {
        let prefix = encode_connexion(Some(to), None, None)?;
        let xin = self.inedges.prefix_iter(txn, &prefix)?;
        let inv_iter = xin
            .map(|r| r.map_err(RelationsErr::from))
            .map(|r| r.map(|(v, _)| GCnx::decode_inversed(v)));
        Ok(inv_iter)
    }

    pub fn no_nodes(&self, txn: &RoTxn) -> RResult<u64> {
        Ok(self.nodes.len(txn)?)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::graph_test_utils::*;

    #[test]
    fn creation_test() {
        let dir = tempfile::TempDir::new().unwrap();
        let graphdb = GraphDB::new(dir.path(), SIZE).unwrap();
        let node1 = fresh_node();
        let node2 = fresh_node();
        let mut txn = graphdb.rw_txn().unwrap();
        let id1 = graphdb.add_node(&mut txn, &node1).unwrap();
        let id2 = graphdb.add_node(&mut txn, &node2).unwrap();
        let idr = graphdb.add_node(&mut txn, &node1).unwrap();
        txn.commit().unwrap();
        assert_eq!(id1, idr);
        assert_ne!(id1, id2);

        let txn = graphdb.ro_txn().unwrap();
        let gnode1 = graphdb.get_node(&txn, id1).unwrap();
        let gnode2 = graphdb.get_node(&txn, id2).unwrap();
        assert_eq!(gnode1, node1);
        assert_eq!(gnode2, node2);
    }

    #[test]
    fn deletion_test() {
        // N1 -e1-> N2
        // N1 -e2-> N3
        // N2 -e3-> N4
        // N3 -e4-> N4
        // N4 -e5-> N1
        let dir = tempfile::TempDir::new().unwrap();
        let graphdb = GraphDB::new(dir.path(), SIZE).unwrap();
        let mut txn = graphdb.rw_txn().unwrap();
        let n1 = graphdb.add_node(&mut txn, &fresh_node()).unwrap();
        let n2 = graphdb.add_node(&mut txn, &fresh_node()).unwrap();
        let n3 = graphdb.add_node(&mut txn, &fresh_node()).unwrap();
        let n4 = graphdb.add_node(&mut txn, &fresh_node()).unwrap();
        let e1 = fresh_edge();
        let e2 = fresh_edge();
        let e3 = fresh_edge();
        let e4 = fresh_edge();
        let e5 = fresh_edge();
        assert!(graphdb.connect(&mut txn, n1, &e1, n2, None).unwrap());
        assert!(graphdb.connect(&mut txn, n1, &e2, n3, None).unwrap());
        assert!(graphdb.connect(&mut txn, n2, &e3, n4, None).unwrap());
        assert!(graphdb.connect(&mut txn, n3, &e4, n4, None).unwrap());
        assert!(graphdb.connect(&mut txn, n4, &e5, n1, None).unwrap());
        // Deleting N4
        // N1 should have 0 in-edges
        // N3 should have 0 out-edges
        // N2 should have 0 out-edges
        let io_n4 = graphdb.get_node(&txn, n4).unwrap();
        let expected_affected = HashSet::from([n1, n2, n3]);
        let got_affected = graphdb.delete_node(&mut txn, n4).unwrap();
        assert_eq!(expected_affected, got_affected);
        let n1_out = graphdb.get_outedges(&txn, n1).unwrap().count();
        let n1_in = graphdb.get_inedges(&txn, n1).unwrap().count();
        let n2_in = graphdb.get_inedges(&txn, n2).unwrap().count();
        let n2_out = graphdb.get_outedges(&txn, n2).unwrap().count();
        let n3_in = graphdb.get_inedges(&txn, n3).unwrap().count();
        let n3_out = graphdb.get_outedges(&txn, n3).unwrap().count();
        let n4_out = graphdb.get_outedges(&txn, n4).unwrap().count();
        let n4_in = graphdb.get_inedges(&txn, n4).unwrap().count();
        assert_eq!(n4_out, 0);
        assert_eq!(n4_in, 0);
        assert_eq!(n1_out, 2);
        assert_eq!(n1_in, 0);
        assert_eq!(n2_in, 1);
        assert_eq!(n2_out, 0);
        assert_eq!(n3_in, 1);
        assert_eq!(n3_out, 0);
        let n4_id = graphdb.get_nodeid(&txn, io_n4.hash()).unwrap();
        assert_eq!(n4_id, None);
    }
    #[test]
    fn connexions_test() {
        // N1 -e1-> N2
        // N1 -e2-> N3
        // N2 -e3-> N4
        // N3 -e4-> N4
        // N4 -e5-> N1
        let dir = tempfile::TempDir::new().unwrap();
        let graphdb = GraphDB::new(dir.path(), SIZE).unwrap();
        let mut txn = graphdb.rw_txn().unwrap();
        let n1 = graphdb.add_node(&mut txn, &fresh_node()).unwrap();
        let n2 = graphdb.add_node(&mut txn, &fresh_node()).unwrap();
        let n3 = graphdb.add_node(&mut txn, &fresh_node()).unwrap();
        let n4 = graphdb.add_node(&mut txn, &fresh_node()).unwrap();
        let e1 = fresh_edge();
        let e2 = fresh_edge();
        let e3 = fresh_edge();
        let e4 = fresh_edge();
        let e5 = fresh_edge();
        assert!(graphdb.connect(&mut txn, n1, &e1, n2, None).unwrap());
        assert!(graphdb.connect(&mut txn, n1, &e2, n3, None).unwrap());
        assert!(graphdb.connect(&mut txn, n2, &e3, n4, None).unwrap());
        assert!(graphdb.connect(&mut txn, n3, &e4, n4, None).unwrap());
        assert!(graphdb.connect(&mut txn, n4, &e5, n1, None).unwrap());

        // out edges test
        let expected_outn1 = HashSet::from([(n2, e1.clone()), (n3, e2.clone())]);
        let expected_outn2 = HashSet::from([(n4, e3.clone())]);
        let expected_outn3 = HashSet::from([(n4, e4.clone())]);
        let expected_outn4 = HashSet::from([(n1, e5.clone())]);
        let got_outn1 = graphdb
            .get_outedges(&txn, n1)
            .unwrap()
            .collect::<RResult<Vec<_>>>()
            .unwrap();
        let got_outn2 = graphdb
            .get_outedges(&txn, n2)
            .unwrap()
            .collect::<RResult<Vec<_>>>()
            .unwrap();
        let got_outn3 = graphdb
            .get_outedges(&txn, n3)
            .unwrap()
            .collect::<RResult<Vec<_>>>()
            .unwrap();
        let got_outn4 = graphdb
            .get_outedges(&txn, n4)
            .unwrap()
            .collect::<RResult<Vec<_>>>()
            .unwrap();
        assert_eq!(got_outn1.len(), expected_outn1.len());
        assert!(got_outn1.into_iter().all(|cnx| {
            let edge = graphdb.get_edge(&txn, cnx.edge()).unwrap();
            cnx.from() == n1 && expected_outn1.contains(&(cnx.to(), edge))
        }));
        assert_eq!(got_outn2.len(), expected_outn2.len());
        assert!(got_outn2.into_iter().all(|cnx| {
            let edge = graphdb.get_edge(&txn, cnx.edge()).unwrap();
            cnx.from() == n2 && expected_outn2.contains(&(cnx.to(), edge))
        }));
        assert_eq!(got_outn3.len(), expected_outn3.len());
        assert!(got_outn3.into_iter().all(|cnx| {
            let edge = graphdb.get_edge(&txn, cnx.edge()).unwrap();
            cnx.from() == n3 && expected_outn3.contains(&(cnx.to(), edge))
        }));
        assert_eq!(got_outn4.len(), expected_outn4.len());
        assert!(got_outn4.into_iter().all(|cnx| {
            let edge = graphdb.get_edge(&txn, cnx.edge()).unwrap();
            cnx.from() == n4 && expected_outn4.contains(&(cnx.to(), edge))
        }));
        // in edges test
        // N1 -e1-> N2
        // N1 -e2-> N3
        // N2 -e3-> N4
        // N3 -e4-> N4
        // N4 -e5-> N1
        let expected_inn1 = HashSet::from([(n4, e5.clone())]);
        let expected_inn2 = HashSet::from([(n1, e1.clone())]);
        let expected_inn3 = HashSet::from([(n1, e2.clone())]);
        let expected_inn4 = HashSet::from([(n2, e3.clone()), (n3, e4.clone())]);
        let got_inn1 = graphdb
            .get_inedges(&txn, n1)
            .unwrap()
            .collect::<RResult<Vec<_>>>()
            .unwrap();
        let got_inn2 = graphdb
            .get_inedges(&txn, n2)
            .unwrap()
            .collect::<RResult<Vec<_>>>()
            .unwrap();
        let got_inn3 = graphdb
            .get_inedges(&txn, n3)
            .unwrap()
            .collect::<RResult<Vec<_>>>()
            .unwrap();
        let got_inn4 = graphdb
            .get_inedges(&txn, n4)
            .unwrap()
            .collect::<RResult<Vec<_>>>()
            .unwrap();
        assert_eq!(got_inn1.len(), expected_inn1.len());
        assert!(got_inn1.into_iter().all(|cnx| {
            let edge = graphdb.get_edge(&txn, cnx.edge()).unwrap();
            cnx.to() == n1 && expected_inn1.contains(&(cnx.from(), edge))
        }));
        assert_eq!(got_inn2.len(), expected_inn2.len());
        assert!(got_inn2.into_iter().all(|cnx| {
            let edge = graphdb.get_edge(&txn, cnx.edge()).unwrap();
            cnx.to() == n2 && expected_inn2.contains(&(cnx.from(), edge))
        }));
        assert_eq!(got_inn3.len(), expected_inn3.len());
        assert!(got_inn3.into_iter().all(|cnx| {
            let edge = graphdb.get_edge(&txn, cnx.edge()).unwrap();
            cnx.to() == n3 && expected_inn3.contains(&(cnx.from(), edge))
        }));
        assert_eq!(got_inn4.len(), expected_inn4.len());
        assert!(got_inn4.into_iter().all(|cnx| {
            let edge = graphdb.get_edge(&txn, cnx.edge()).unwrap();
            cnx.to() == n4 && expected_inn4.contains(&(cnx.from(), edge))
        }));
    }

    #[test]
    fn connexion_similarity_test() {
        let dir = tempfile::TempDir::new().unwrap();
        let graphdb = GraphDB::new(dir.path(), SIZE).unwrap();
        let mut txn = graphdb.rw_txn().unwrap();
        let n1 = graphdb.add_node(&mut txn, &fresh_node()).unwrap();
        let n2 = graphdb.add_node(&mut txn, &fresh_node()).unwrap();
        let e1 = fresh_edge();
        let e2 = fresh_edge();
        let e3 = fresh_edge();
        assert!(graphdb.connect(&mut txn, n1, &e1, n2, None).unwrap());
        assert!(graphdb.connect(&mut txn, n2, &e2, n1, None).unwrap());
        assert!(graphdb.connect(&mut txn, n1, &e3, n2, None).unwrap());
        assert!(!graphdb.connect(&mut txn, n1, &e1, n2, None).unwrap());
        assert!(!graphdb.connect(&mut txn, n2, &e2, n1, None).unwrap());
        assert!(!graphdb.connect(&mut txn, n1, &e3, n2, None).unwrap());
        let expect = HashSet::from([e1, e3]);
        let n1_n2 = graphdb
            .connected_by(&txn, n1, n2)
            .unwrap()
            .map(|c| c.unwrap())
            .map(|c| c.edge())
            .map(|c| graphdb.get_edge(&txn, c).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(n1_n2.len(), expect.len());
        assert!(n1_n2.iter().all(|v| expect.contains(v)));
        let mut n2_n1 = graphdb
            .connected_by(&txn, n2, n1)
            .unwrap()
            .map(|c| c.unwrap())
            .map(|c| c.edge())
            .map(|c| graphdb.get_edge(&txn, c).unwrap());
        assert_eq!(Some(e2), n2_n1.next());
        assert_eq!(None, n2_n1.next());
    }
}
