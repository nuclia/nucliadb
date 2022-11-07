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

use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashSet};

use ram_hnsw::*;
use rand::distributions::Uniform;
use rand::{thread_rng, Rng};
use rayon::prelude::*;

use super::*;

pub mod params {
    pub fn level_factor() -> f64 {
        1.0 / (m() as f64).ln()
    }
    pub const fn m_max() -> usize {
        30
    }
    pub const fn m() -> usize {
        30
    }
    pub const fn ef_construction() -> usize {
        100
    }
    pub const fn k_neighbours() -> usize {
        10
    }
}
pub trait DataRetriever: std::marker::Sync {
    fn is_deleted(&self, _: Address) -> bool;
    fn has_label(&self, _: Address, _: &[u8]) -> bool;
    fn consine_similarity(&self, _: Address, _: Address) -> f32;
}

pub trait Layer {
    type EdgeIt: Iterator<Item = (Address, Edge)>;
    fn get_out_edges(&self, node: Address) -> Self::EdgeIt;
}

pub trait Hnsw {
    type L: Layer;
    fn get_entry_point(&self) -> Option<EntryPoint>;
    fn get_layer(&self, i: usize) -> Self::L;
}

#[derive(Clone, Copy)]
struct Cnx(pub Address, pub f32);
impl Eq for Cnx {}
impl Ord for Cnx {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}
impl PartialEq for Cnx {
    fn eq(&self, other: &Self) -> bool {
        f32::eq(&self.1, &other.1)
    }
}
impl PartialOrd for Cnx {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        f32::partial_cmp(&self.1, &other.1)
    }
}

pub type Neighbours = Vec<(Address, f32)>;

pub struct HnswOps<'a, DR> {
    pub tracker: &'a DR,
}

impl<'a, DR: DataRetriever> HnswOps<'a, DR> {
    fn select_neighbours_heuristic(
        &self,
        k_neighbours: usize,
        mut candidates: Vec<(Address, Edge)>,
    ) -> Vec<(Address, Edge)> {
        candidates.sort_unstable_by_key(|(n, d)| std::cmp::Reverse(Cnx(*n, d.dist)));
        candidates.dedup_by_key(|(addr, _)| *addr);
        candidates.truncate(k_neighbours);
        candidates
    }
    fn get_random_layer(&self) -> usize {
        let mut rng = thread_rng();
        let distribution = Uniform::new(0.0, 1.0);
        let sample: f64 = rng.sample(distribution);
        let picked_level = -sample.ln() * params::level_factor();
        picked_level.round() as usize
    }
    fn cosine_similarity(&self, x: Address, y: Address) -> f32 {
        self.tracker.consine_similarity(x, y)
    }
    fn closest_up_node<L: Layer>(
        &'a self,
        solution: &mut HashSet<Address>,
        filters: &[&[u8]],
        layer: L,
        x: Address,
        y: Address,
    ) -> Option<(Address, f32)> {
        solution.remove(&x);
        let mut candidates = BinaryHeap::from([Cnx(x, self.cosine_similarity(x, y))]);
        let mut visited_nodes = HashSet::new();
        loop {
            match candidates.pop() {
                None => break None,
                Some(Cnx(n, _))
                    if !self.tracker.is_deleted(n)
                        && !solution.contains(&n)
                        && filters.iter().all(|label| self.tracker.has_label(n, label)) =>
                {
                    solution.insert(n);
                    break Some((n, self.cosine_similarity(n, y)));
                }
                Some(Cnx(down, _)) => layer.get_out_edges(down).for_each(|(n, _)| {
                    if !visited_nodes.contains(&n) {
                        candidates.push(Cnx(n, self.cosine_similarity(n, y)));
                        visited_nodes.insert(n);
                    }
                }),
            }
        }
    }
    fn layer_search<L: Layer>(
        &self,
        x: Address,
        layer: L,
        k_neighbours: usize,
        entry_points: &[Address],
    ) -> Neighbours {
        let mut visited = HashSet::new();
        let mut candidates = BinaryHeap::new();
        let mut ms_neighbours = BinaryHeap::new();
        for ep in entry_points.iter().copied() {
            visited.insert(ep);
            let similarity = self.cosine_similarity(x, ep);
            candidates.push(Cnx(ep, similarity));
            ms_neighbours.push(Reverse(Cnx(ep, similarity)));
        }
        loop {
            match (candidates.pop(), ms_neighbours.peek().cloned()) {
                (None, _) => break,
                (Some(Cnx(_, cs)), Some(Reverse(Cnx(_, ws)))) if cs < ws => break,
                (Some(Cnx(cn, _)), Some(Reverse(Cnx(_, ws)))) => {
                    for (y, _) in layer.get_out_edges(cn) {
                        if !visited.contains(&y) {
                            visited.insert(y);
                            let similarity = self.cosine_similarity(x, y);
                            if similarity > ws || ms_neighbours.len() < k_neighbours {
                                candidates.push(Cnx(y, similarity));
                                ms_neighbours.push(Reverse(Cnx(y, similarity)));
                                if ms_neighbours.len() > k_neighbours {
                                    ms_neighbours.pop();
                                }
                            }
                        }
                    }
                }
                _ => (),
            }
        }
        ms_neighbours
            .into_sorted_vec()
            .into_par_iter()
            .map(|Reverse(Cnx(n, d))| (n, d))
            .collect()
    }
    fn layer_insert(
        &self,
        x: Address,
        layer: &mut RAMLayer,
        entry_points: &[Address],
    ) -> Vec<Address> {
        use params::*;
        let neighbours = self.layer_search::<&RAMLayer>(x, layer, ef_construction(), entry_points);
        let mut needs_repair = HashSet::new();
        let mut result = Vec::with_capacity(neighbours.len());
        layer.add_node(x);
        for (y, dist) in neighbours.iter().copied() {
            result.push(y);
            layer.add_edge(x, Edge { dist }, y);
            layer.add_edge(y, Edge { dist }, x);
            if layer.no_out_edges(y) > 2 * m_max() {
                needs_repair.insert(y);
            }
        }
        for crnt in needs_repair {
            let edges = layer.take_out_edges(crnt);
            let neighbours = self.select_neighbours_heuristic(m_max(), edges);
            neighbours
                .into_iter()
                .for_each(|(y, edge)| layer.add_edge(crnt, edge, y));
        }
        result
    }
    pub fn insert(&self, x: Address, hnsw: &mut RAMHnsw) {
        match hnsw.entry_point {
            None => {
                let top_level = self.get_random_layer();
                hnsw.increase_layers_with(x, top_level).update_entry_point();
            }
            Some(entry_point) => {
                let level = self.get_random_layer();
                hnsw.increase_layers_with(x, level);
                let top_layer = std::cmp::min(entry_point.layer, level);
                let ep = entry_point.node;
                hnsw.layers[0..=top_layer]
                    .iter_mut()
                    .rev()
                    .fold(vec![ep], |eps, layer| self.layer_insert(x, layer, &eps));
                hnsw.update_entry_point();
            }
        }
    }
    pub fn search<H: Hnsw>(
        &self,
        x: Address,
        hnsw: H,
        k_neighbours: usize,
        with_filter: &[&[u8]],
    ) -> Neighbours {
        if let Some(entry_point) = hnsw.get_entry_point() {
            let mut crnt_layer = entry_point.layer;
            let mut neighbours = vec![(entry_point.node, 0.)];
            while crnt_layer != 0 {
                let entry_points: Vec<_> = neighbours.into_iter().map(|(node, _)| node).collect();
                let layer_res = self.layer_search(x, hnsw.get_layer(crnt_layer), 1, &entry_points);
                neighbours = layer_res;
                crnt_layer -= 1;
            }
            let entry_points: Vec<_> = neighbours.into_iter().map(|(node, _)| node).collect();
            let layer = hnsw.get_layer(crnt_layer);
            let result = self.layer_search(x, layer, k_neighbours, &entry_points);
            let mut solution = result.iter().copied().map(|v| v.0).collect();
            result
                .into_iter()
                .flat_map(|(n, _)| {
                    self.closest_up_node(&mut solution, with_filter, hnsw.get_layer(0), n, x)
                })
                .collect()
        } else {
            Neighbours::default()
        }
    }
}
