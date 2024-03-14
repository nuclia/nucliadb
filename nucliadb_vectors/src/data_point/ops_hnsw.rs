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

use bit_set::BitSet;
use ram_hnsw::*;
use rand::distributions::Uniform;
use rand::prelude::*;
use rand::rngs::SmallRng;

use super::*;
use crate::data_point::params;

/// Implementors of this trait can guide the hnsw search
pub trait DataRetriever: std::marker::Sync {
    fn get_key(&self, x: Address) -> &[u8];
    fn is_deleted(&self, x: Address) -> bool;
    fn has_label(&self, x: Address, label: &[u8]) -> bool;
    fn similarity(&self, x: Address, y: Address) -> f32;
    fn get_vector(&self, x: Address) -> &[u8];
    /// Embeddings with smaller similarity should not be considered.
    fn min_score(&self) -> f32;
}

/// Implementors of this trait are layers of an HNSW where a nearest neighbour search can be ran.
pub trait Layer {
    type EdgeIt: Iterator<Item = (Address, Edge)>;
    fn get_out_edges(&self, node: Address) -> Self::EdgeIt;
}

/// Implementors of this trait are an HNSW where search can be ran.
pub trait Hnsw {
    type L: Layer;
    fn get_entry_point(&self) -> Option<EntryPoint>;
    fn get_layer(&self, i: usize) -> Self::L;
}

///  Tuples ([`Address`], [`f32`]) can not be stored in a [`BinaryHeap`] because [`f32`] does not
/// implement [`Ord`]. [`Cnx`] is an application of the new-type pattern that lets us bypass the
/// orphan rules and store such tuples in a [`BinaryHeap`].  
#[derive(Clone, Copy)]
struct Cnx(Address, f32);
impl Eq for Cnx {}
impl Ord for Cnx {
    fn cmp(&self, other: &Self) -> Ordering {
        f32::total_cmp(&self.1, &other.1)
    }
}
impl PartialEq for Cnx {
    fn eq(&self, other: &Self) -> bool {
        f32::eq(&self.1, &other.1)
    }
}
impl PartialOrd for Cnx {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// A list of neighbours containing a pointer to the embedding and their similarity.
pub type Neighbours = Vec<(Address, f32)>;

/// Guides an algorithm to the valid nodes.
struct NodeFilter<'a, DR> {
    tracker: &'a DR,
    filter: &'a FormulaFilter<'a>,
    blocked_addresses: &'a HashSet<Address>,
    vec_counter: &'a mut RepCounter<'a>,
}

impl<'a, DR: DataRetriever> NodeFilter<'a, DR> {
    pub fn passes_formula(&self, n: Address) -> bool {
        // The vector satisfies the given filter
        self.filter.run(n, self.tracker)
    }

    pub fn is_valid(&self, n: Address, score: f32) -> bool {
        !score.is_nan()
        // The vector was deleted at some point and will be removed in a future merge
        && !self.tracker.is_deleted(n)
        // The vector is blocked, meaning that its key is part of the current version of the solution
        && !self.blocked_addresses.contains(&n)
        // The number of times this vector appears is 0
        && self.vec_counter.get(self.tracker.get_vector(n)) == 0
    }
}

pub struct HnswOps<'a, DR> {
    distribution: Uniform<f64>,
    layer_rng: SmallRng,
    tracker: &'a DR,
}

impl<'a, DR: DataRetriever> HnswOps<'a, DR> {
    fn select_neighbours_heuristic(
        &self,
        k_neighbours: usize,
        mut candidates: Vec<(Address, Edge)>,
    ) -> Vec<(Address, Edge)> {
        candidates.sort_unstable_by_key(|(n, d)| std::cmp::Reverse(Cnx(*n, *d)));
        candidates.dedup_by_key(|(addr, _)| *addr);
        candidates.truncate(k_neighbours);
        candidates
    }
    fn similarity(&self, x: Address, y: Address) -> f32 {
        self.tracker.similarity(x, y)
    }
    fn get_random_layer(&mut self) -> usize {
        let sample: f64 = self.layer_rng.sample(self.distribution);
        let picked_level = -sample.ln() * params::level_factor();
        picked_level.round() as usize
    }
    fn closest_up_nodes<L: Layer>(
        &'a self,
        start: Address,
        query: Address,
        layer: L,
        k_neighbours: usize,
        filter: NodeFilter<'a, DR>,
    ) -> Vec<(Address, f32)> {
        // We just need to perform BFS, the replacement is the closest node to the actual
        // best solution. This algorithm takes a lazy approach to computing the similarity of
        // candidates.
        let mut visited = BitSet::new();
        let mut candidates = BinaryHeap::new();
        let mut results = BinaryHeap::new();

        let similarity = self.similarity(start, query);
        candidates.push(Cnx(start, similarity));
        if similarity > self.tracker.min_score() && filter.is_valid(start, similarity) && filter.passes_formula(start) {
            results.push(Reverse(Cnx(start, similarity)))
        }

        loop {
            match (candidates.pop(), results.peek().cloned()) {
                // No more candidates
                (None, _) => break,
                // Our best candidate is worse than the min score
                (Some(Cnx(_, cs)), _) if cs < self.tracker.min_score() => break,
                // Our best candidate is worse than the worst result
                (Some(Cnx(_, cs)), Some(Reverse(Cnx(_, ws)))) if cs < ws => break,
                // Evaluate a candidate
                (Some(Cnx(cn, _)), mut worst_result) => {
                    for (y, _) in layer.get_out_edges(cn) {
                        if !visited.contains(y.0) {
                            visited.insert(y.0);
                            let similarity = self.similarity(query, y);

                            let better = worst_result.map(|Reverse(Cnx(_, ws))| similarity > ws).unwrap_or(true);

                            if better || results.len() < k_neighbours {
                                if similarity > self.tracker.min_score() {
                                    candidates.push(Cnx(y, similarity));

                                    if filter.is_valid(y, similarity) && filter.passes_formula(y) {
                                        results.push(Reverse(Cnx(y, similarity)));
                                        filter.vec_counter.add(self.tracker.get_vector(y));
                                        if results.len() > k_neighbours {
                                            if let Some(Reverse(Cnx(n, _))) = results.pop() {
                                                filter.vec_counter.sub(self.tracker.get_vector(n));
                                            }
                                        }
                                    }
                                }
                                worst_result = results.peek().cloned();
                            }
                        }
                    }
                }
            }
        }

        results.into_sorted_vec().into_iter().map(|Reverse(Cnx(n, d))| (n, d)).collect()
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
            let similarity = self.similarity(x, ep);
            candidates.push(Cnx(ep, similarity));
            ms_neighbours.push(Reverse(Cnx(ep, similarity)));
        }
        loop {
            match (candidates.pop(), ms_neighbours.peek().cloned()) {
                (None, _) => break,
                (Some(Cnx(_, cs)), Some(Reverse(Cnx(_, ws)))) if cs < ws => break,
                (Some(Cnx(cn, _)), Some(Reverse(Cnx(_, mut ws)))) => {
                    for (y, _) in layer.get_out_edges(cn) {
                        if !visited.contains(&y) {
                            visited.insert(y);
                            let similarity = self.similarity(x, y);
                            if similarity > ws || ms_neighbours.len() < k_neighbours {
                                candidates.push(Cnx(y, similarity));
                                ms_neighbours.push(Reverse(Cnx(y, similarity)));
                                if ms_neighbours.len() > k_neighbours {
                                    ms_neighbours.pop();
                                }
                                ws = ms_neighbours.peek().map_or(ws, |Reverse(v)| v.1);
                            }
                        }
                    }
                }
                _ => (),
            }
        }
        ms_neighbours.into_sorted_vec().into_iter().map(|Reverse(Cnx(n, d))| (n, d)).collect()
    }
    fn layer_insert(&self, x: Address, layer: &mut RAMLayer, entry_points: &[Address]) -> Vec<Address> {
        use params::*;
        let neighbours = self.layer_search::<&RAMLayer>(x, layer, ef_construction(), entry_points);
        let neighbours = self.select_neighbours_heuristic(m_max(), neighbours);
        let mut needs_repair = HashSet::new();
        let mut result = Vec::with_capacity(neighbours.len());
        layer.add_node(x);
        for (y, dist) in neighbours.iter().copied() {
            result.push(y);
            layer.add_edge(x, dist, y);
            layer.add_edge(y, dist, x);
            if layer.no_out_edges(y) > m_max() {
                needs_repair.insert(y);
            }
        }
        for crnt in needs_repair {
            let edges = layer.take_out_edges(crnt);
            let neighbours = self.select_neighbours_heuristic(m_max(), edges);
            neighbours.into_iter().for_each(|(y, edge)| layer.add_edge(crnt, edge, y));
        }
        result
    }
    pub fn insert(&mut self, x: Address, hnsw: &mut RAMHnsw) {
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
        with_filter: FormulaFilter,
        with_duplicates: bool,
    ) -> Neighbours {
        if k_neighbours == 0 {
            return Neighbours::with_capacity(0);
        }

        let Some(entry_point) = hnsw.get_entry_point() else {
            return Neighbours::default();
        };

        let mut neighbours = vec![(entry_point.node, 0.)];
        for crnt_layer in (0..=entry_point.layer).rev() {
            let layer = hnsw.get_layer(crnt_layer);
            let entry_points: Vec<_> = neighbours.into_iter().map(|(node, _)| node).collect();
            let layer_res = self.layer_search(query, layer, 1, &entry_points);
            neighbours = layer_res;
        }

        let Some(best_node) = neighbours.first() else {
            return Neighbours::default();
        };

        let mut vec_counter = RepCounter::new(!with_duplicates);
        let filter = NodeFilter {
            filter: &with_filter,
            tracker: self.tracker,
            blocked_addresses: &HashSet::new(),
            vec_counter: &mut vec_counter,
        };
        let mut filtered_result = self.closest_up_nodes(best_node.0, query, hnsw.get_layer(0), k_neighbours, filter);

        // order may be lost
        filtered_result.sort_by(|a, b| b.1.total_cmp(&a.1));
        filtered_result
    }

    pub fn new(tracker: &DR) -> HnswOps<'_, DR> {
        HnswOps {
            tracker,
            distribution: Uniform::new(0.0, 1.0),
            layer_rng: SmallRng::seed_from_u64(2),
        }
    }
}

/// Useful datatype for counting how many times a embedding appears a solution.
/// If it not enabled the count will always be 0.
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
        if self.enabled {
            *self.counter.entry(x).or_insert(0) += 1;
        }
    }
    fn sub(&mut self, x: &'a [u8]) {
        if self.enabled {
            *self.counter.entry(x).or_insert(1) -= 1;
        }
    }
    fn get(&self, x: &[u8]) -> usize {
        if self.enabled {
            self.counter.get(&x).copied().unwrap_or_default()
        } else {
            0
        }
    }
}
