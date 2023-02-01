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

use uuid::Uuid;

use super::bfs_engine::BfsGuide;
use super::graph_db::*;
use super::relations_io::{IoEdge, IoNode};

pub const SIZE: usize = 1048576 * 100000;
pub struct AllGuide;
impl BfsGuide for AllGuide {}

pub struct FreeJumps;
impl BfsGuide for FreeJumps {
    fn free_jump(&self, _cnx: GCnx) -> bool {
        true
    }
}
pub struct UNodes;
impl Iterator for UNodes {
    type Item = IoNode;
    fn next(&mut self) -> Option<Self::Item> {
        let seed = Uuid::new_v4();
        Some(IoNode::new(
            seed.to_string(),
            seed.to_string(),
            Some(seed.to_string()),
        ))
    }
}

pub struct UEdges;
impl Iterator for UEdges {
    type Item = IoEdge;
    fn next(&mut self) -> Option<Self::Item> {
        let seed = Uuid::new_v4();
        Some(IoEdge::new(seed.to_string(), Some(seed.to_string())))
    }
}

pub fn fresh_node() -> IoNode {
    UNodes.next().unwrap()
}
pub fn fresh_edge() -> IoEdge {
    UEdges.next().unwrap()
}
