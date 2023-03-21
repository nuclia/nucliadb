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

use super::bfs_engine::{BfsEngineBuilder, BfsGuide};
use super::errors::*;
pub use super::graph_db::{Entity, GCnx};
use super::graph_db::{GraphDB, RoToken, RwToken};
use super::node_dictionary::{DReader, DWriter, NodeDictionary};
pub use super::relations_io::{IoEdge, IoEdgeMetadata, IoNode};

pub struct RMode(DReader);
pub struct WMode(DWriter);

pub struct Index {
    graphdb: GraphDB,
    dictionary: NodeDictionary,
}
impl Index {
    const DICTIONARY_PATH: &str = "NodeDictionary";
    const GRAPH_PATH: &str = "GraphDB";
    const GRAPH_SIZE: usize = 1048576 * 100000;
    fn connect(
        &self,
        graph_txn: &mut RwToken,
        dict_writer: &DWriter,
        from: &IoNode,
        to: &IoNode,
        edge: &IoEdge,
        edge_metadata: Option<&IoEdgeMetadata>,
    ) -> RResult<bool> {
        self.dictionary.add_node(dict_writer, from)?;
        self.dictionary.add_node(dict_writer, to)?;
        let from = self.graphdb.add_node(graph_txn, from)?;
        let to = self.graphdb.add_node(graph_txn, to)?;
        self.graphdb
            .connect(graph_txn, from, edge, to, edge_metadata)
    }
    fn graph_search<G: BfsGuide>(
        &self,
        txn: &RoToken,
        guide: G,
        max_depth: usize,
        entry_points: Vec<Entity>,
    ) -> RResult<impl Iterator<Item = GCnx>> {
        BfsEngineBuilder::new()
            .graph(&self.graphdb)
            .txn(txn)
            .max_depth(max_depth)
            .guide(guide)
            .entry_points(entry_points)
            .build()
            .unwrap()
            .search()
    }
    fn delete_node(
        &self,
        graph_txn: &mut RwToken,
        dict_writer: &DWriter,
        node_id: Entity,
    ) -> RResult<HashSet<Entity>> {
        let value = self.graphdb.get_node(graph_txn, node_id)?;
        self.dictionary.delete_node(dict_writer, &value);
        self.graphdb.delete_node(graph_txn, node_id)
    }
    fn no_nodes(&self, txn: &RoToken) -> RResult<u64> {
        self.graphdb.no_nodes(txn)
    }
    fn iter_node_ids<'a>(
        &self,
        txn: &'a RoToken,
    ) -> RResult<impl Iterator<Item = RResult<Entity>> + 'a> {
        self.graphdb.iter_node_ids(txn)
    }
    fn iter_edge_ids<'a>(
        &self,
        txn: &'a RoToken,
    ) -> RResult<impl Iterator<Item = RResult<Entity>> + 'a> {
        self.graphdb.iter_edge_ids(txn)
    }
    pub fn get_outedges<'a>(
        &self,
        txn: &'a RoToken,
        from: Entity,
    ) -> RResult<impl Iterator<Item = RResult<GCnx>> + 'a> {
        self.graphdb.get_outedges(txn, from)
    }
    fn get_edge(&self, txn: &RoToken, id: Entity) -> RResult<IoEdge> {
        self.graphdb.get_edge(txn, id)
    }
    fn get_edge_metadata(&self, txn: &RoToken, id: Entity) -> RResult<Option<IoEdgeMetadata>> {
        self.graphdb.get_edge_metadata(txn, id)
    }
    fn get_node(&self, txn: &RoToken, id: Entity) -> RResult<IoNode> {
        self.graphdb.get_node(txn, id)
    }
    fn get_nodeid(&self, txn: &RoToken, x: &str) -> RResult<Option<Entity>> {
        self.graphdb.get_nodeid(txn, x)
    }
    fn prefix_search(&self, dict_reader: &DReader, prefix: &str) -> RResult<Vec<String>> {
        self.dictionary.search(dict_reader, prefix)
    }
    fn get_inedges<'a>(
        &self,
        txn: &'a RoToken,
        to: Entity,
    ) -> RResult<impl Iterator<Item = RResult<GCnx>> + 'a> {
        self.graphdb.get_inedges(txn, to)
    }
    pub fn new_writer(location: &Path) -> RResult<(Index, WMode)> {
        let dictionary_address = location.join(Self::DICTIONARY_PATH);
        let graph_address = location.join(Self::GRAPH_PATH);
        if !graph_address.exists() {
            std::fs::create_dir(&graph_address)?;
        }
        if !dictionary_address.exists() {
            std::fs::create_dir(&dictionary_address)?;
        }
        let graphdb = GraphDB::new(&graph_address, Self::GRAPH_SIZE)?;
        let (dictionary, mode) = NodeDictionary::new_writer(&dictionary_address)?;
        let index = Index {
            graphdb,
            dictionary,
        };
        Ok((index, WMode(mode)))
    }
    pub fn new_reader(location: &Path) -> RResult<(Index, RMode)> {
        let dictionary_address = location.join(Self::DICTIONARY_PATH);
        let graph_address = location.join(Self::GRAPH_PATH);
        if !graph_address.exists() {
            std::fs::create_dir(&graph_address)?;
        }
        if !dictionary_address.exists() {
            std::fs::create_dir(&dictionary_address)?;
        }
        let graphdb = GraphDB::new(&graph_address, Self::GRAPH_SIZE)?;
        let (dictionary, mode) = NodeDictionary::new_reader(&dictionary_address)?;
        let index = Index {
            graphdb,
            dictionary,
        };
        Ok((index, RMode(mode)))
    }
    pub fn start_reading(&self) -> RResult<GraphReader> {
        Ok(GraphReader {
            graph_txn: self.graphdb.ro_txn()?,
            index: self,
        })
    }
    pub fn start_writing(&self) -> RResult<GraphWriter> {
        Ok(GraphWriter {
            graph_txn: self.graphdb.rw_txn()?,
            index: self,
        })
    }
}

pub struct GraphReader<'a> {
    graph_txn: RoToken<'a>,
    index: &'a Index,
}
impl<'a> GraphReader<'a> {
    pub fn reload(&self, RMode(reader): &RMode) -> RResult<()> {
        Ok(reader.reload()?)
    }
    pub fn get_outedges(
        &'a self,
        from: Entity,
    ) -> RResult<impl Iterator<Item = RResult<GCnx>> + 'a> {
        self.index.get_outedges(&self.graph_txn, from)
    }
    pub fn get_inedges(&'a self, to: Entity) -> RResult<impl Iterator<Item = RResult<GCnx>> + 'a> {
        self.index.get_inedges(&self.graph_txn, to)
    }
    pub fn iter_node_ids(&'a self) -> RResult<impl Iterator<Item = RResult<Entity>> + 'a> {
        self.index.iter_node_ids(&self.graph_txn)
    }
    pub fn iter_edge_ids(&'a self) -> RResult<impl Iterator<Item = RResult<Entity>> + 'a> {
        self.index.iter_edge_ids(&self.graph_txn)
    }
    pub fn get_edge(&self, id: Entity) -> RResult<IoEdge> {
        self.index.get_edge(&self.graph_txn, id)
    }
    pub fn get_edge_metadata(&self, id: Entity) -> RResult<Option<IoEdgeMetadata>> {
        self.index.get_edge_metadata(&self.graph_txn, id)
    }
    pub fn get_node(&self, id: Entity) -> RResult<IoNode> {
        self.index.get_node(&self.graph_txn, id)
    }
    pub fn get_node_id(&self, x: &str) -> RResult<Option<Entity>> {
        self.index.get_nodeid(&self.graph_txn, x)
    }
    pub fn prefix_search(&self, RMode(reader): &RMode, prefix: &str) -> RResult<Vec<String>> {
        self.index.prefix_search(reader, prefix)
    }
    pub fn search<G: BfsGuide>(
        &self,
        guide: G,
        max_depth: usize,
        entry_points: Vec<Entity>,
    ) -> RResult<impl Iterator<Item = GCnx>> {
        self.index
            .graph_search(&self.graph_txn, guide, max_depth, entry_points)
    }
    pub fn no_nodes(&self) -> RResult<u64> {
        self.index.no_nodes(&self.graph_txn)
    }
}

pub struct GraphWriter<'a> {
    graph_txn: RwToken<'a>,
    index: &'a Index,
}
impl<'a> GraphWriter<'a> {
    pub fn commit(self, WMode(writer): &mut WMode) -> RResult<()> {
        writer.commit()?;
        self.graph_txn.commit()?;
        Ok(())
    }
    pub fn get_outedges(
        &'a self,
        from: Entity,
    ) -> RResult<impl Iterator<Item = RResult<GCnx>> + 'a> {
        self.index.get_outedges(&self.graph_txn, from)
    }
    pub fn get_inedges(&'a self, to: Entity) -> RResult<impl Iterator<Item = RResult<GCnx>> + 'a> {
        self.index.get_inedges(&self.graph_txn, to)
    }
    pub fn iter_node_ids(&'a self) -> RResult<impl Iterator<Item = RResult<Entity>> + 'a> {
        self.index.iter_node_ids(&self.graph_txn)
    }
    pub fn iter_edge_ids(&'a self) -> RResult<impl Iterator<Item = RResult<Entity>> + 'a> {
        self.index.iter_edge_ids(&self.graph_txn)
    }
    pub fn get_edge(&self, id: Entity) -> RResult<IoEdge> {
        self.index.get_edge(&self.graph_txn, id)
    }
    pub fn get_edge_metadata(&self, id: Entity) -> RResult<Option<IoEdgeMetadata>> {
        self.index.get_edge_metadata(&self.graph_txn, id)
    }
    pub fn get_node(&self, id: Entity) -> RResult<IoNode> {
        self.index.get_node(&self.graph_txn, id)
    }
    pub fn get_node_id(&self, x: &str) -> RResult<Option<Entity>> {
        self.index.get_nodeid(&self.graph_txn, x)
    }
    pub fn no_nodes(&self) -> RResult<u64> {
        self.index.no_nodes(&self.graph_txn)
    }
    pub fn connect(
        &mut self,
        WMode(writer): &WMode,
        from: &IoNode,
        to: &IoNode,
        edge: &IoEdge,
        edge_metadata: Option<&IoEdgeMetadata>,
    ) -> RResult<bool> {
        self.index
            .connect(&mut self.graph_txn, writer, from, to, edge, edge_metadata)
    }
    pub fn delete_node(
        &mut self,
        WMode(writer): &WMode,
        node_id: Entity,
    ) -> RResult<HashSet<Entity>> {
        self.index.delete_node(&mut self.graph_txn, writer, node_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    pub fn create_and_insert() {
        let dir = tempfile::TempDir::new().unwrap();
        let (index, mut wmode) = Index::new_writer(dir.path()).unwrap();
        let n1 = IoNode::new("N1".to_string(), "T1".to_string(), None);
        let n2 = IoNode::new("N2".to_string(), "T2".to_string(), None);
        let edge = IoEdge::new("T2".to_string(), None);
        let mut writer = index.start_writing().unwrap();
        writer.connect(&wmode, &n1, &n2, &edge, None).unwrap();
        writer.connect(&wmode, &n2, &n1, &edge, None).unwrap();
        writer.commit(&mut wmode).unwrap();
        let (index, _rmode) = Index::new_reader(dir.path()).unwrap();
        let reader = index.start_reading().unwrap();
        std::mem::drop(reader);
    }
}
