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

use crate::index::Index;
use crate::memory_system::elements::*;

fn check_layer(index: &Index, layer: usize) -> HashSet<Node> {
    let mut visited = HashSet::new();
    for (node, edges) in &index.layers_out[layer].cnx {
        let _ = index.get_node_key(*node);
        let _ = index.get_node_vector(*node);
        let in_layer = &index.layers_in[layer].cnx;
        for (edge, value) in edges {
            let in_edge = &in_layer[edge][node];
            let _ = index.get_node_key(*edge);
            let _ = index.get_node_vector(*edge);
            assert_eq!(*node, value.from);
            assert_eq!(*edge, value.to);
            assert_eq!(value.to, in_edge.from);
            assert_eq!(value.from, in_edge.to);
        }
        visited.insert(*node);
    }
    visited
}

pub fn check(index: &Index) {
    let in_layers = index.layers_in.len();
    let out_layers = index.layers_out.len();
    assert_eq!(in_layers, out_layers);
    let nodes_per_layer: Vec<_> = (0..out_layers).map(|i| check_layer(index, i)).collect();
    for (layer, visited) in nodes_per_layer.iter().enumerate().rev() {
        assert!((0..layer).all(|i| visited.is_subset(&nodes_per_layer[i])));
    }
}
