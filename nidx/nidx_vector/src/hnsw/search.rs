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
use rustc_hash::FxHashSet;
use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashMap};
use std::time::Instant;
use tracing::trace;

use crate::config::{VectorCardinality, VectorConfig};
use crate::hnsw::params::EF_SEARCH;
use crate::hnsw::ram_hnsw::EntryPoint;
use crate::inverted_index::FilterBitSet;
use crate::vector_types::rabitq;
use crate::{ParagraphAddr, VectorAddr};

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
pub trait SearchableLayer {
    fn get_out_edges(&self, node: VectorAddr) -> impl Iterator<Item = VectorAddr>;
}

/// Implementors of this trait are an HNSW where search can be ran.
pub trait SearchableHnsw {
    type L: SearchableLayer;
    fn get_entry_point(&self) -> EntryPoint;
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
pub struct NodeFilter<'a> {
    filter: &'a FilterBitSet,
    paragraphs: Option<FxHashSet<ParagraphAddr>>,
    vec_counter: RepCounter<'a>,
}

impl<'a> NodeFilter<'a> {
    pub fn new(filter: &'a FilterBitSet, with_duplicates: bool, config: &VectorConfig) -> NodeFilter<'a> {
        let paragraphs = matches!(config.vector_cardinality, VectorCardinality::Multi).then_some(Default::default());
        let vec_counter = RepCounter::new(!with_duplicates);
        NodeFilter {
            filter,
            paragraphs,
            vec_counter,
        }
    }

    pub fn passes(&mut self, retriever: &'a impl DataRetriever, v: VectorAddr) -> bool {
        let vector = retriever.get_vector(v);
        let paragraph = retriever.paragraph(v);

        // Matches query filters?
        if !self.filter.contains(paragraph) {
            return false;
        }

        // Is a duplicated vector?
        if self.vec_counter.get(vector) > 0 {
            return false;
        };

        // Is a vector for an existing paragraph? (only relevant in multi-vector search)
        if let Some(paragraphs) = &mut self.paragraphs
            && !paragraphs.insert(paragraph)
        {
            return false;
        }

        // Track the vector for duplicates (the paragraphs was tracked in the above check)
        self.vec_counter.add(vector);

        true
    }
}

pub struct HnswSearcher<'a, DR> {
    retriever: &'a DR,
    preload_nodes: bool,
}

impl<'a, DR: DataRetriever> HnswSearcher<'a, DR> {
    pub fn new(retriever: &DR, preload_nodes: bool) -> HnswSearcher<'_, DR> {
        HnswSearcher {
            retriever,
            preload_nodes,
        }
    }

    /// Breadth-first search to find the closest nodes to the entry-points that fulfill the filtering conditions
    fn closest_up_nodes<L: SearchableLayer>(
        &'a self,
        entry_points: Vec<Cnx>,
        query: &SearchVector,
        layer: L,
        number_of_results: usize,
        mut filter: NodeFilter<'a>,
    ) -> Vec<(VectorAddr, f32)> {
        const MAX_VECTORS_TO_PRELOAD: u32 = 20_000;
        let mut results = Vec::new();
        let inner_entry_points_iter = entry_points.iter().map(|Cnx(VectorAddr(inner), _)| *inner as usize);
        let mut visited_nodes: BitSet = BitSet::from_iter(inner_entry_points_iter);
        let mut candidates = entry_points;
        candidates.sort_unstable();

        let mut preloaded = 0;

        loop {
            let Some(Cnx(candidate, candidate_similarity)) = candidates.pop() else {
                break;
            };

            if candidate_similarity < self.retriever.min_score() {
                break;
            }

            if !candidate_similarity.is_nan() && filter.passes(self.retriever, candidate) {
                results.push((candidate, candidate_similarity));
            }

            if results.len() == number_of_results {
                break;
            }

            if self.preload_nodes && preloaded < MAX_VECTORS_TO_PRELOAD {
                for new_candidate in layer.get_out_edges(candidate) {
                    if !visited_nodes.contains(new_candidate.0 as usize) {
                        self.retriever.will_need(new_candidate);
                        preloaded += 1;
                    }
                }
            }

            for new_candidate in layer.get_out_edges(candidate) {
                if visited_nodes.insert(new_candidate.0 as usize) {
                    let new_similarity = self.retriever.similarity(new_candidate, query);

                    if new_similarity > self.retriever.min_score() {
                        candidates.push(Cnx(new_candidate, new_similarity));
                    }
                }
            }
            candidates.sort_unstable();
        }

        results
    }

    pub fn layer_search<L: SearchableLayer>(
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
                    for y in layer.get_out_edges(cn) {
                        if self.preload_nodes && !visited.contains(&y) {
                            self.retriever.will_need_vector(y);
                        }
                    }
                    for y in layer.get_out_edges(cn) {
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
        ms_neighbours
            .into_sorted_vec()
            .into_iter()
            .map(|Reverse(CnxWithBound(n, d))| (n, d))
    }

    pub fn search<H: SearchableHnsw>(
        &self,
        query: &SearchVector,
        hnsw: H,
        k_neighbours: usize,
        filter: NodeFilter,
    ) -> Neighbours {
        if k_neighbours == 0 {
            return Neighbours::with_capacity(0);
        }

        let entry_point = hnsw.get_entry_point();
        let mut crnt_layer = entry_point.layer;
        let mut entry_points = vec![entry_point.node];

        // Traverse upper layer finding the best match on each one
        let t = Instant::now();
        while crnt_layer != 0 {
            let layer = hnsw.get_layer(crnt_layer);
            let layer_res = self.layer_search(query, layer, 1, &entry_points);
            entry_points = layer_res.map(|(addr, _)| addr).collect();
            crnt_layer -= 1;
        }
        let time = t.elapsed();
        trace!(?time, "HNSW search: upper layers");

        // If using RabitQ, request more vectors to rerank later
        let original_query = if let SearchVector::RabitQ(rq) = query {
            Some(&SearchVector::Query(rq.original().to_vec()))
        } else {
            None
        };
        let last_layer_k = if original_query.is_some() {
            std::cmp::min(k_neighbours * rabitq::RERANKING_FACTOR, rabitq::RERANKING_LIMIT)
        } else {
            // We search for at least EF_SEARCH neighbours to guarantee good results
            // In the case k > EF we could use EF search and expand the search with
            // closest_up_nodes()
            std::cmp::max(k_neighbours, EF_SEARCH)
        };

        // Find the best k nodes in the last layer
        let t = Instant::now();
        let layer = hnsw.get_layer(crnt_layer);
        let neighbours = self.layer_search(query, layer, last_layer_k, &entry_points);
        let time = t.elapsed();
        trace!(?time, "HNSW search: last layer");

        let entry_points = if let Some(query) = original_query {
            // If using RabitQ, rerank using the original vectors
            let t = Instant::now();
            let reranked = rabitq::rerank_top(neighbours.collect(), k_neighbours, self.retriever, query)
                .into_iter()
                .map(|Reverse(c)| c)
                .collect();
            let time = t.elapsed();
            trace!(?time, "HNSW search: reranking");
            reranked
        } else {
            neighbours.map(|(addr, score)| Cnx(addr, score.score)).collect()
        };

        // Find k nodes that match the filter in the last layer
        let t = Instant::now();
        let mut filtered_result = self.closest_up_nodes(
            entry_points,
            original_query.unwrap_or(query),
            hnsw.get_layer(0),
            k_neighbours,
            filter,
        );
        let time = t.elapsed();
        trace!(?time, "HNSW search: closest nodes");

        // order may be lost
        filtered_result.sort_by(|a, b| b.1.total_cmp(&a.1));
        filtered_result
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
