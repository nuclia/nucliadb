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

use bit_set::BitSet;
use ram_hnsw::*;
use rand::distributions::Uniform;
use rand::prelude::*;
use rand::rngs::SmallRng;
use rustc_hash::FxHashSet;
use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};

use crate::inverted_index::FilterBitSet;
use crate::vector_types::rabitq;
use crate::{ParagraphAddr, VectorAddr};

use super::params;
use super::*;

/// Implementors of this trait can guide the hnsw search
pub trait DataRetriever: std::marker::Sync {
    fn similarity(&self, x: VectorAddr, y: &SearchVector) -> f32;
    fn similarity_upper_bound(&self, x: VectorAddr, y: &SearchVector) -> EstimatedScore;
    fn paragraph(&self, x: VectorAddr) -> ParagraphAddr;
    fn get_vector(&self, x: VectorAddr) -> &[u8];
    /// Embeddings with smaller similarity should not be considered.
    fn min_score(&self) -> f32;
    /// Preload all data for a vector + paragraph (needed for similarity + filtering)
    fn will_need(&self, x: VectorAddr);
    /// Preload a vector (only the vector, for similarity comparison)
    fn will_need_vector(&self, x: VectorAddr);
}

pub enum SearchVector {
    Stored(VectorAddr),
    Query(Vec<u8>),
    RabitQ(rabitq::QueryVector),
}

/// Implementors of this trait are layers of an HNSW where a nearest neighbour search can be ran.
pub trait Layer {
    type EdgeIt: Iterator<Item = (VectorAddr, Edge)>;
    fn get_out_edges(&self, node: VectorAddr) -> Self::EdgeIt;
}

/// Implementors of this trait are an HNSW where search can be ran.
pub trait Hnsw {
    type L: Layer;
    fn get_entry_point(&self) -> Option<EntryPoint>;
    fn get_layer(&self, i: usize) -> Self::L;
}

#[derive(Clone, Copy)]
pub struct EstimatedScore {
    pub score: f32,
    pub upper_bound: f32,
}

impl EstimatedScore {
    pub fn new_with_error(score: f32, error: f32) -> Self {
        Self {
            score,
            upper_bound: score + error,
        }
    }

    pub fn new_exact(score: f32) -> Self {
        Self {
            score,
            upper_bound: score,
        }
    }
}

///  Tuples ([`VectorAddr`], [`f32`]) can not be stored in a [`BinaryHeap`] because [`f32`] does not
/// implement [`Ord`]. [`Cnx`] is an application of the new-type pattern that lets us bypass the
/// orphan rules and store such tuples in a [`BinaryHeap`].
#[derive(Clone, Copy)]
pub struct Cnx(pub VectorAddr, pub f32);
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

#[derive(Clone, Copy)]
pub struct CnxWithBound(VectorAddr, EstimatedScore);
impl Eq for CnxWithBound {}
impl Ord for CnxWithBound {
    fn cmp(&self, other: &Self) -> Ordering {
        f32::total_cmp(&self.1.score, &other.1.score)
    }
}
impl PartialEq for CnxWithBound {
    fn eq(&self, other: &Self) -> bool {
        f32::eq(&self.1.score, &other.1.score)
    }
}
impl PartialOrd for CnxWithBound {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// A list of neighbours containing a pointer to the embedding and their similarity.
pub type Neighbours = Vec<(VectorAddr, f32)>;

/// Guides an algorithm to the valid nodes.
struct NodeFilter<'a, DR> {
    retriever: &'a DR,
    filter: &'a FilterBitSet,
    paragraphs: FxHashSet<ParagraphAddr>,
    vec_counter: RepCounter<'a>,
}

impl<DR: DataRetriever> NodeFilter<'_, DR> {
    pub fn passes_formula(&self, n: ParagraphAddr) -> bool {
        self.filter.contains(n)
    }

    pub fn is_valid(&self, n: VectorAddr, score: f32) -> bool {
        !score.is_nan()
        // Reject the candidate if we already have a result for the same paragraph
        && !self.paragraphs.contains(&self.retriever.paragraph(n))
        // Reject the candidate if we already have a result with an identical vector
        && self.vec_counter.get(self.retriever.get_vector(n)) == 0
    }

    /// Adds a result so that further candidates with the same vector
    /// or paragraph will get rejected.
    pub fn add_result(&mut self, n: VectorAddr) {
        self.paragraphs.insert(self.retriever.paragraph(n));
        self.vec_counter.add(self.retriever.get_vector(n));
    }
}

pub struct HnswOps<'a, DR> {
    distribution: Uniform<f64>,
    layer_rng: SmallRng,
    retriever: &'a DR,
    preload_nodes: bool,
}

impl<'a, DR: DataRetriever> HnswOps<'a, DR> {
    fn select_neighbours_heuristic(
        &self,
        k_neighbours: usize,
        candidates: Vec<(VectorAddr, Edge)>,
        layer: &RAMLayer,
    ) -> Vec<(VectorAddr, Edge)> {
        let mut results = Vec::new();
        let mut discarded = BinaryHeap::new();

        // First, select the best candidates to link, trying to connect from all directions
        // i.e: avoid linking to all nodes in a single cluster
        for (x, sim) in candidates.into_iter() {
            if results.len() == k_neighbours {
                break;
            }
            // Keep if x is more similar to the new node than it is similar to other results
            // i.e: similarity(x, new) > similarity(x, y) for all y in result
            let check = results
                .iter()
                .map(|&(y, _)| {
                    // Try to get the similarity from an existing graph edge, fallback to calculating it
                    if let Some((_, edge)) = layer.get_out_edges(x).find(|&(z, _)| z == y) {
                        edge
                    } else {
                        self.retriever.similarity(x, &SearchVector::Stored(y))
                    }
                })
                .all(|inter_sim| sim > inter_sim);
            if check {
                results.push((x, sim));
            } else {
                discarded.push(Cnx(x, sim));
            }
        }

        // keepPrunedConnections: keep some other connections to fill M
        while results.len() < k_neighbours {
            let Some(Cnx(n, d)) = discarded.pop() else {
                return results;
            };
            results.push((n, d));
        }

        results
    }
    fn get_random_layer(&mut self) -> usize {
        let sample: f64 = self.layer_rng.sample(self.distribution);
        let picked_level = -sample.ln() * params::level_factor();
        picked_level.round() as usize
    }
    fn closest_up_nodes<L: Layer>(
        &'a self,
        entry_points: Vec<VectorAddr>,
        query: &SearchVector,
        layer: L,
        number_of_results: usize,
        mut filter: NodeFilter<'a, DR>,
    ) -> Vec<(VectorAddr, f32)> {
        // We just need to perform BFS, the replacement is the closest node to the actual
        // best solution. This algorithm takes a lazy approach to computing the similarity of
        // candidates.

        const MAX_VECTORS_TO_PRELOAD: u32 = 20_000;
        let mut results = Vec::new();
        let inner_entry_points_iter = entry_points.iter().map(|VectorAddr(inner)| *inner as usize);
        let mut visited_nodes: BitSet = BitSet::from_iter(inner_entry_points_iter);
        let mut candidates = VecDeque::from(entry_points);

        let mut preloaded = 0;

        loop {
            let Some(candidate) = candidates.pop_front() else {
                break;
            };

            let candidate_similarity = self.retriever.similarity(candidate, query);

            if candidate_similarity < self.retriever.min_score() {
                break;
            }

            let paragraph_addr = self.retriever.paragraph(candidate);
            if filter.is_valid(candidate, candidate_similarity) && filter.passes_formula(paragraph_addr) {
                filter.add_result(candidate);
                results.push((candidate, candidate_similarity));
            }

            if results.len() == number_of_results {
                return results;
            }

            let mut sorted_out: Vec<_> = layer.get_out_edges(candidate).collect();
            sorted_out.sort_by(|a, b| b.1.total_cmp(&a.1));
            sorted_out.into_iter().for_each(|(new_candidate, _)| {
                if !visited_nodes.contains(new_candidate.0 as usize) {
                    visited_nodes.insert(new_candidate.0 as usize);
                    candidates.push_back(new_candidate);

                    if self.preload_nodes && preloaded < MAX_VECTORS_TO_PRELOAD {
                        self.retriever.will_need(new_candidate);
                        preloaded += 1;
                    }
                }
            });
        }

        results
    }
    fn layer_search<L: Layer>(
        &self,
        query: &SearchVector,
        layer: L,
        k_neighbours: usize,
        entry_points: &[VectorAddr],
    ) -> impl Iterator<Item = (VectorAddr, EstimatedScore)> {
        // Nodes already visited
        let mut visited = FxHashSet::default();
        // Nodes to visit
        let mut candidates = BinaryHeap::new();
        // Best results so far
        let mut ms_neighbours = BinaryHeap::new();

        // The initial candidates are the entry points
        for ep in entry_points.iter().copied() {
            visited.insert(ep);
            let similarity = self.retriever.similarity_upper_bound(ep, query);
            candidates.push(CnxWithBound(ep, similarity));
            ms_neighbours.push(Reverse(CnxWithBound(ep, similarity)));
        }

        loop {
            match (candidates.pop(), ms_neighbours.peek().cloned()) {
                // No more candidates, done
                (None, _) => break,
                // Candidate is worse than worse result, done
                (
                    Some(CnxWithBound(_, EstimatedScore { score: cs, .. })),
                    Some(Reverse(CnxWithBound(_, EstimatedScore { score: ws, .. }))),
                ) if cs < ws => {
                    break;
                }
                // Candidate is better than worse result
                (Some(CnxWithBound(cn, _)), Some(Reverse(CnxWithBound(_, mut ws)))) => {
                    for (y, _) in layer.get_out_edges(cn) {
                        if self.preload_nodes && !visited.contains(&y) {
                            self.retriever.will_need_vector(y);
                        }
                    }
                    for (y, _) in layer.get_out_edges(cn) {
                        if !visited.contains(&y) {
                            visited.insert(y);
                            let similarity = self.retriever.similarity_upper_bound(y, query);
                            if similarity.score > ws.score || ms_neighbours.len() < k_neighbours {
                                candidates.push(CnxWithBound(y, similarity));
                                ms_neighbours.push(Reverse(CnxWithBound(y, similarity)));
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
        ms_neighbours.into_iter().map(|Reverse(CnxWithBound(n, d))| (n, d))
    }

    fn layer_insert(
        &self,
        x: VectorAddr,
        layer: &mut RAMLayer,
        entry_points: &[VectorAddr],
        mmax: usize,
    ) -> Vec<VectorAddr> {
        use params::*;
        let search_neighbours = self
            .layer_search::<&RAMLayer>(&SearchVector::Stored(x), layer, EF_CONSTRUCTION, entry_points)
            .map(|(addr, score)| (addr, score.score))
            .collect();
        let neighbours = self.select_neighbours_heuristic(M, search_neighbours, layer);
        let mut needs_repair = HashSet::new();
        let mut result = Vec::with_capacity(neighbours.len());
        layer.add_node(x);
        for (y, dist) in neighbours.iter().copied() {
            result.push(y);
            layer.add_edge(x, dist, y);
            layer.add_edge(y, dist, x);
            if layer.no_out_edges(y) > mmax {
                needs_repair.insert(y);
            }
        }
        for crnt in needs_repair {
            let edges = layer.take_out_edges(crnt);
            let neighbours = self.select_neighbours_heuristic(params::prune_m(mmax), edges, layer);
            neighbours
                .into_iter()
                .for_each(|(y, edge)| layer.add_edge(crnt, edge, y));
        }
        result
    }
    pub fn insert(&mut self, x: VectorAddr, hnsw: &mut RAMHnsw) {
        match hnsw.entry_point {
            None => {
                let top_level = self.get_random_layer();
                hnsw.increase_layers_with(x, top_level).update_entry_point();
            }
            Some(entry_point) => {
                let level = self.get_random_layer();
                hnsw.increase_layers_with(x, level);
                let top_layer = std::cmp::max(entry_point.layer, level);
                let mut eps = vec![entry_point.node];

                for l in (0..=top_layer).rev() {
                    if l > level {
                        // Above insertion point, just search
                        let new_ep = self
                            .layer_search(&SearchVector::Stored(x), &hnsw.layers[l], 1, &eps)
                            .next()
                            .unwrap();
                        eps[0] = new_ep.0;
                    } else {
                        eps = self.layer_insert(x, &mut hnsw.layers[l], &eps, params::m_max_for_layer(l));
                    }
                }
                hnsw.update_entry_point();
            }
        }
    }

    pub fn search<H: Hnsw>(
        &self,
        query: &SearchVector,
        hnsw: H,
        k_neighbours: usize,
        with_filter: &FilterBitSet,
        with_duplicates: bool,
    ) -> Neighbours {
        if k_neighbours == 0 {
            return Neighbours::with_capacity(0);
        }

        let Some(entry_point) = hnsw.get_entry_point() else {
            return Neighbours::default();
        };

        let mut crnt_layer = entry_point.layer;
        let mut entry_points = vec![entry_point.node];

        // Traverse upper layer finding the best match on each one
        while crnt_layer != 0 {
            let layer = hnsw.get_layer(crnt_layer);
            let layer_res = self.layer_search(query, layer, 1, &entry_points);
            entry_points = layer_res.map(|(addr, _)| addr).collect();
            crnt_layer -= 1;
        }

        // If using RabitQ, request more vectors to rerank later
        let original_query = if let SearchVector::RabitQ(rq) = query {
            Some(&SearchVector::Query(rq.original().to_vec()))
        } else {
            None
        };
        let last_layer_k = if original_query.is_some() {
            std::cmp::min(k_neighbours * rabitq::RERANKING_FACTOR, rabitq::RERANKING_LIMIT)
        } else {
            k_neighbours
        };

        // Find the best k nodes in the last layer
        let layer = hnsw.get_layer(crnt_layer);
        let neighbours = self.layer_search(query, layer, last_layer_k, &entry_points);

        let entry_points = if let Some(query) = original_query {
            // If using RabitQ, rerank using the original vectors
            rabitq::rerank_top(neighbours.collect(), k_neighbours, self.retriever, query)
                .into_iter()
                .map(|Reverse(Cnx(addr, _))| addr)
                .collect()
        } else {
            neighbours.map(|(addr, _)| addr).collect()
        };

        let filter = NodeFilter {
            filter: with_filter,
            retriever: self.retriever,
            paragraphs: Default::default(),
            vec_counter: RepCounter::new(!with_duplicates),
        };
        let layer_zero = hnsw.get_layer(0);

        // Find k nodes that match the filter in the last layer
        let mut filtered_result = self.closest_up_nodes(
            entry_points,
            original_query.unwrap_or(query),
            layer_zero,
            k_neighbours,
            filter,
        );

        // order may be lost
        filtered_result.sort_by(|a, b| b.1.total_cmp(&a.1));
        filtered_result
    }

    pub fn new(retriever: &DR, preload_nodes: bool) -> HnswOps<'_, DR> {
        HnswOps {
            retriever,
            distribution: Uniform::new(0.0, 1.0),
            layer_rng: SmallRng::seed_from_u64(2),
            preload_nodes,
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
    fn get(&self, x: &[u8]) -> usize {
        if self.enabled {
            self.counter.get(&x).copied().unwrap_or_default()
        } else {
            0
        }
    }
}
