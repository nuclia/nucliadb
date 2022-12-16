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

use std::collections::{HashSet, LinkedList};

use super::errors::*;
use crate::relations::graph_db::*;

// BfsGuide allows the user to modify how the search will be performed.
// By default a BfsGuide does not interfere in the search.
pub trait BfsGuide {
    fn matches(&self, node: Entity) -> bool;
    fn free_jump(&self, _cnx: GCnx) -> bool {
        false
    }
    fn node_allowed(&self, _node: Entity) -> bool {
        true
    }
    fn edge_allowed(&self, _edge: Entity) -> bool {
        true
    }
}

// Refers to a node that has been reached by the BFS
// but has not been expanded yet.
#[derive(Clone, Copy, Debug)]
struct BfsNode {
    // GId of the node.
    point: Entity,
    // Depth at which the node was found.
    depth: usize,
}

#[derive(derive_builder::Builder)]
#[builder(name = "BfsEngineBuilder", pattern = "owned")]
pub struct BfsEngine<'a, Guide>
where Guide: BfsGuide
{
    #[builder(setter(skip))]
    #[builder(default = "LinkedList::new()")]
    work_stack: LinkedList<BfsNode>,
    #[builder(setter(skip))]
    #[builder(default = "HashSet::new()")]
    visited: HashSet<Entity>,
    entry_points: Vec<Entity>,
    max_depth: usize,
    guide: Guide,
    txn: &'a RoToken<'a>,
    graph: &'a GraphDB,
}

impl<'a, Guide> BfsEngineBuilder<'a, Guide>
where Guide: BfsGuide
{
    pub fn new() -> BfsEngineBuilder<'a, Guide> {
        BfsEngineBuilder::create_empty()
    }
}
impl<'a, Guide> BfsEngine<'a, Guide>
where Guide: BfsGuide
{
    pub fn search(mut self) -> RResult<Vec<Entity>> {
        std::mem::take(&mut self.entry_points)
            .iter()
            .copied()
            .map(|point| (BfsNode { point, depth: 0 }, self.visited.insert(point)))
            .filter(|(_, v)| *v)
            .for_each(|(e, _)| self.work_stack.push_back(e));
        let mut results = vec![];
        while let Some(node) = self.work_stack.pop_front() {
            self.expand(node)?;
            if self.guide.matches(node.point) {
                results.push(node.point);
            }
        }
        Ok(results)
    }
    fn expand(&mut self, node: BfsNode) -> RResult<()> {
        self.graph
            .get_outedges(self.txn, node.point)?
            .flat_map(|a| a.ok().into_iter())
            .filter(|edge| node.depth < self.max_depth || self.guide.free_jump(*edge))
            .filter(|edge| self.guide.edge_allowed(edge.edge()))
            .filter(|edge| self.guide.node_allowed(edge.to()))
            .for_each(|edge| {
                if !self.visited.contains(&edge.to()) {
                    let node = BfsNode {
                        point: edge.to(),
                        depth: node.depth + (!self.guide.free_jump(edge) as usize),
                    };
                    self.visited.insert(node.point);
                    self.work_stack.push_back(node);
                }
            });
        self.graph
            .get_inedges(self.txn, node.point)?
            .flat_map(|a| a.ok().into_iter())
            .filter(|edge| node.depth < self.max_depth || self.guide.free_jump(*edge))
            .filter(|edge| self.guide.edge_allowed(edge.edge()))
            .filter(|edge| self.guide.node_allowed(edge.from()))
            .for_each(|edge| {
                if !self.visited.contains(&edge.from()) {
                    let node = BfsNode {
                        point: edge.from(),
                        depth: node.depth + (!self.guide.free_jump(edge) as usize),
                    };
                    self.visited.insert(node.point);
                    self.work_stack.push_back(node);
                }
            });
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::path::Path;

    use super::*;
    use crate::relations::graph_test_utils::*;
    fn graph(dir: &Path) -> (Vec<Entity>, GraphDB) {
        let graphdb = GraphDB::new(dir, SIZE).unwrap();
        let mut txn = graphdb.rw_txn().unwrap();
        let ids: Vec<_> = UNodes
            .take(4)
            .map(|node| graphdb.add_node(&mut txn, &node).unwrap())
            .collect();
        UEdges
            .take(ids.len() - 1)
            .enumerate()
            .for_each(|(i, edge)| {
                graphdb
                    .connect(&mut txn, ids[i], &edge, ids[i + 1])
                    .unwrap();
            });
        let backedge = UEdges.next().unwrap();
        graphdb
            .connect(&mut txn, ids[3], &backedge, ids[0])
            .unwrap();
        txn.commit().unwrap();
        (ids, graphdb)
    }

    #[test]
    fn full_search() {
        let dir = tempfile::TempDir::new().unwrap();
        let (nodes, graphdb) = graph(dir.path());
        let txn = graphdb.ro_txn().unwrap();
        let bfs = BfsEngineBuilder::new()
            .entry_points(vec![nodes[0]])
            .graph(&graphdb)
            .txn(&txn)
            .guide(AllGuide)
            .max_depth(usize::MAX)
            .build()
            .unwrap();
        let expected = &nodes;
        let result = bfs.search().unwrap();
        assert_eq!(result.len(), expected.len());
        assert!(result.iter().copied().all(|n| expected.contains(&n)));
    }

    #[test]
    fn full_reverse_search() {
        let dir = tempfile::TempDir::new().unwrap();
        let (nodes, graphdb) = graph(dir.path());
        let txn = graphdb.ro_txn().unwrap();
        let bfs = BfsEngineBuilder::new()
            .entry_points(vec![nodes[3]])
            .graph(&graphdb)
            .txn(&txn)
            .guide(AllGuide)
            .max_depth(usize::MAX)
            .build()
            .unwrap();
        let expected = &nodes;
        let result = bfs.search().unwrap();
        assert_eq!(result.len(), expected.len());
        assert!(result.iter().copied().all(|n| expected.contains(&n)));
    }

    #[test]
    fn limit_depth_search() {
        let dir = tempfile::TempDir::new().unwrap();
        let (nodes, graphdb) = graph(dir.path());
        let txn = graphdb.ro_txn().unwrap();
        let bfs = BfsEngineBuilder::new()
            .entry_points(vec![nodes[0]])
            .graph(&graphdb)
            .txn(&txn)
            .guide(AllGuide)
            .max_depth(1)
            .build()
            .unwrap();
        let expected = vec![nodes[0], nodes[1], nodes[3]];
        let result = bfs.search().unwrap();
        assert_eq!(result.len(), expected.len());
        assert!(result.iter().copied().all(|n| expected.contains(&n)));
    }

    #[test]
    fn always_jump() {
        let dir = tempfile::TempDir::new().unwrap();
        let (nodes, graphdb) = graph(dir.path());
        let txn = graphdb.ro_txn().unwrap();
        let bfs = BfsEngineBuilder::new()
            .entry_points(vec![nodes[0]])
            .graph(&graphdb)
            .txn(&txn)
            .guide(FreeJumps)
            .max_depth(0)
            .build()
            .unwrap();
        let expected = &nodes;
        let result = bfs.search().unwrap();
        assert_eq!(result.len(), expected.len());
        assert!(result.iter().copied().all(|n| expected.contains(&n)));
    }
}
