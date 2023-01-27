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
use std::fmt::Debug;

use nucliadb_core::tracing::*;

use crate::bfs_engine::*;
use crate::errors::*;
use crate::index::*;

pub struct GrpcGuide<'a> {
    pub reader: &'a GraphReader<'a>,
    pub node_filters: HashSet<(&'a str, Option<&'a str>)>,
    pub edge_filters: HashSet<(&'a str, Option<&'a str>)>,
    pub jump_always: &'a str,
}
impl<'a> GrpcGuide<'a> {
    fn treat_bfs_error<A, B, F>(&self, default: B, input: A, f: F) -> B
    where
        F: Fn(A) -> RResult<B>,
        A: Debug + Copy,
        B: Default,
    {
        match f(input) {
            Err(e) => {
                info!("{e:?} during BFS looking at {input:?}");
                default
            }
            Ok(result) => result,
        }
    }
}
impl<'a> BfsGuide for GrpcGuide<'a> {
    fn edge_allowed(&self, edge: Entity) -> bool {
        self.treat_bfs_error(false, edge, |edge: Entity| {
            self.reader.get_edge(edge).map(|edge| {
                self.edge_filters.is_empty()
                    || self.edge_filters.contains(&(edge.xtype(), edge.subtype()))
                    || self.edge_filters.contains(&(edge.xtype(), None))
            })
        })
    }
    fn node_allowed(&self, node: Entity) -> bool {
        self.treat_bfs_error(false, node, |node: Entity| {
            self.reader.get_node(node).map(|node| {
                self.node_filters.is_empty()
                    || self.node_filters.contains(&(node.xtype(), node.subtype()))
                    || self.node_filters.contains(&(node.xtype(), None))
            })
        })
    }
    fn free_jump(&self, cnx: GCnx) -> bool {
        self.treat_bfs_error(false, cnx.edge(), |edge: Entity| {
            self.reader
                .get_edge(edge)
                .map(|edge| edge.xtype() == self.jump_always)
        })
    }
}
