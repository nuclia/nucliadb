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
use super::*;

fn exits_in_layer(l_id: usize, layer: &WriteIndexLayer, node: NodeId) -> bool {
    let mut index = 0;
    let mut found = false;
    while index < layer.node_stack.len() && !found {
        if let Some(i) = layer.node_stack[index] {
            found = found || i == node;
            assert!(!found, "found in {}", l_id);
        }
        index += 1;
    }
    found
}

pub fn exits(index: &WriteIndex, node: NodeId) -> bool {
    index
        .layers
        .iter()
        .enumerate()
        .fold(false, |p, (id, layer)| p || exits_in_layer(id, layer, node))
}
