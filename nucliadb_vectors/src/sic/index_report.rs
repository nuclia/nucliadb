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

use std::collections::HashSet;
use std::fs::File;
use std::io::*;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::index::Index;
use crate::memory_system::elements::*;

#[derive(Serialize, Deserialize)]
pub struct LayerInfo {
    id: usize,
    no_nodes: usize,
    max_in: usize,
    max_out: usize,
    min_in: usize,
    min_out: usize,
    no_scc_in: usize,
    no_scc_out: usize,
}

fn dfs(layer: &GraphLayer, node: Node, visited: &mut HashSet<Node>) {
    if !visited.contains(&node) {
        visited.insert(node);
        let mut edges: Vec<_> = layer[node].values().copied().collect();
        edges.sort_by(|a, b| f32::partial_cmp(&a.dist, &b.dist).unwrap());
        for edge in edges {
            dfs(layer, edge.to, visited);
        }
        visited.insert(node);
    }
}

fn layer_report(id: usize, layer_in: &GraphLayer, layer_out: &GraphLayer) -> LayerInfo {
    let mut report = LayerInfo {
        id,
        no_nodes: layer_out.no_nodes(),
        max_in: 0,
        max_out: 0,
        min_in: usize::MAX,
        min_out: usize::MAX,
        no_scc_in: 0,
        no_scc_out: 0,
    };
    let mut visited = HashSet::new();
    let mut nodes: Vec<_> = layer_out.cnx.keys().copied().collect();
    nodes.sort_by_key(|n| n.vector.start);
    for node in nodes {
        if !visited.contains(&node) {
            report.max_out = std::cmp::max(report.max_out, layer_out[node].len());
            report.min_out = std::cmp::min(report.min_out, layer_out[node].len());
            report.no_scc_out += 1;
            dfs(layer_out, node, &mut visited);
        }
    }
    let mut visited = HashSet::new();
    let mut nodes: Vec<_> = layer_in.cnx.keys().copied().collect();
    nodes.sort_by_key(|n| n.vector.start);
    for node in nodes {
        if !visited.contains(&node) {
            report.max_in = std::cmp::max(report.max_in, layer_in[node].len());
            report.min_in = std::cmp::min(report.min_in, layer_in[node].len());
            report.no_scc_in += 1;
            dfs(layer_in, node, &mut visited);
        }
    }
    report
}

pub fn generate_report(index: &Index, output: &Path) {
    for i in 0..index.no_layers() {
        let file = output.join(&format!("layer{i}.json"));
        let report = layer_report(i, &index.layers_in[i], &index.layers_out[i]);
        let mut writer = File::create(&file).unwrap();
        write!(writer, "{}", serde_json::to_string_pretty(&report).unwrap()).unwrap();
    }
}
