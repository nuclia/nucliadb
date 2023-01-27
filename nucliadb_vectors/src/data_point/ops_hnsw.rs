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
use std::collections::{BinaryHeap, HashMap, HashSet};

use nucliadb_core::thread::*;
use ram_hnsw::*;
use rand::distributions::Uniform;
use rand::{thread_rng, Rng};

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
    fn get_vector(&self, _: Address) -> &[u8];
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
        x: Address,
        query: Address,
        layer: L,
        filters: &[&[u8]],
        blocked_addresses: &HashSet<Address>,
        vec_counter: &RepCounter,
    ) -> Option<(Address, f32)> {
        let mut visited_nodes = HashSet::new();
        let mut candidates = BinaryHeap::from([Cnx(x, self.cosine_similarity(x, query))]);
        loop {
            match candidates.pop() {
                None => break None,
                Some(Cnx(n, score))
                        // The vector was deleted at some point and will be removed in a future merge
                        if !self.tracker.is_deleted(n)
                        // A score may be invalid if the index contains zero vectors 
                        && !score.is_nan()
                        // The vector is blocked, meaning that it is part of the current version of the solution
                        && !blocked_addresses.contains(&n)
                        // The number of times this vector appears is 0
                        && vec_counter.get(self.tracker.get_vector(n)) == 0
                        // The vector contains all the labels required by the query.
                        && filters.iter().all(|label| self.tracker.has_label(n, label)) =>
                {
                    break Some((n, self.cosine_similarity(n, query)));
                }
                Some(Cnx(down, _)) => layer.get_out_edges(down).for_each(|(n, _)| {
                    if !visited_nodes.contains(&n) {
                        candidates.push(Cnx(n, self.cosine_similarity(n, query)));
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
        query: Address,
        hnsw: H,
        k_neighbours: usize,
        with_filter: &[&[u8]],
        with_duplicates: bool,
    ) -> Neighbours {
        if let Some(entry_point) = hnsw.get_entry_point() {
            let mut crnt_layer = entry_point.layer;
            let mut neighbours = vec![(entry_point.node, 0.)];
            while crnt_layer != 0 {
                let layer = hnsw.get_layer(crnt_layer);
                let entry_points: Vec<_> = neighbours.into_iter().map(|(node, _)| node).collect();
                let layer_res = self.layer_search(query, layer, 1, &entry_points);
                neighbours = layer_res;
                crnt_layer -= 1;
            }
            let entry_points: Vec<_> = neighbours.into_iter().map(|(node, _)| node).collect();
            let layer = hnsw.get_layer(crnt_layer);
            let result = self.layer_search(query, layer, k_neighbours, &entry_points);
            let mut sol_addresses = HashSet::new();
            let mut vec_counter = RepCounter::new(!with_duplicates);
            let mut filtered_result = Vec::new();
            result.iter().copied().for_each(|(addr, _)| {
                sol_addresses.insert(addr);
                vec_counter.add(self.tracker.get_vector(addr));
            });
            result.into_iter().for_each(|(addr, _)| {
                sol_addresses.remove(&addr);
                vec_counter.sub(self.tracker.get_vector(addr));
                if let Some((addr, score)) = self.closest_up_node(
                    addr,
                    query,
                    hnsw.get_layer(0),
                    with_filter,
                    &sol_addresses,
                    &vec_counter,
                ) {
                    filtered_result.push((addr, score));
                    sol_addresses.insert(addr);
                    vec_counter.add(self.tracker.get_vector(addr));
                }
            });
            filtered_result
        } else {
            Neighbours::default()
        }
    }
}

#[derive(Default)]
struct RepCounter<'a> {
    enabled: bool,
    counter: HashMap<&'a [u8], usize>,
}
impl<'a> RepCounter<'a> {
    fn new(enabled: bool) -> RepCounter<'a> {
        RepCounter {
            enabled,
            ..Self::default()
        }
    }
    fn add(&mut self, x: &'a [u8]) {
        *self.counter.entry(x).or_insert(0) += 1;
    }
    fn sub(&mut self, x: &'a [u8]) {
        *self.counter.entry(x).or_insert(1) -= 1;
    }
    fn get(&self, x: &[u8]) -> usize {
        self.counter
            .get(&x)
            .copied()
            .filter(|_| self.enabled)
            .unwrap_or_default()
    }
}
