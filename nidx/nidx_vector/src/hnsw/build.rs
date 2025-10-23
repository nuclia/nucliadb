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

use std::collections::{BinaryHeap, HashSet};

use rand::{Rng, SeedableRng, distributions::Uniform, rngs::SmallRng};

use crate::{
    VectorAddr,
    hnsw::{
        Cnx, DataRetriever, HnswSearcher, RAMHnsw, SearchVector, params,
        ram_hnsw::{Edge, RAMLayer},
        search::SearchableLayer,
    },
};

pub struct HnswBuilder<'a, DR> {
    distribution: Uniform<f64>,
    layer_rng: SmallRng,
    retriever: &'a DR,
    searcher: HnswSearcher<'a, DR>,
}

impl<'a, DR: DataRetriever> HnswBuilder<'a, DR> {
    pub fn new(retriever: &DR) -> HnswBuilder<'_, DR> {
        HnswBuilder {
            retriever,
            distribution: Uniform::new(0.0, 1.0),
            layer_rng: SmallRng::seed_from_u64(2),
            searcher: HnswSearcher::new(retriever, false),
        }
    }

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

    fn layer_insert(
        &self,
        x: VectorAddr,
        layer: &mut RAMLayer,
        entry_points: &[VectorAddr],
        mmax: usize,
    ) -> Vec<VectorAddr> {
        let search_neighbours = self
            .searcher
            .layer_search::<&RAMLayer>(&SearchVector::Stored(x), layer, params::EF_CONSTRUCTION, entry_points)
            .map(|(addr, score)| (addr, score.score))
            .collect();
        let neighbours = self.select_neighbours_heuristic(params::M, search_neighbours, layer);
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
                            .searcher
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
}
