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

    /// Initialize the graph holding the specified number of vectors
    /// Each vector is assigned to a random amount of layers but no
    /// edges are built yet. If the node is preinitialized, you can skip
    /// an initial amount of nodes so they aren't recreated
    pub fn initialize_graph(&mut self, hnsw: &mut RAMHnsw, skip_nodes: u32, total_nodes: u32) {
        for node_id in skip_nodes..total_nodes {
            let top_layer = self.get_random_layer();
            hnsw.add_node(VectorAddr(node_id), top_layer);
        }
        hnsw.update_entry_point();
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

    /// Insert a node into a layer, calculating all edges
    fn layer_insert(
        &self,
        x: VectorAddr,
        layer: &mut RAMLayer,
        search_neighbours: Vec<(VectorAddr, Edge)>,
        mmax: usize,
    ) -> Vec<VectorAddr> {
        let neighbours = self.select_neighbours_heuristic(params::M, search_neighbours, layer);
        let mut needs_repair = HashSet::new();
        let mut result = Vec::with_capacity(neighbours.len());
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

    /// Insert a node into all corresponding layers.
    /// The node must be created first by calling initialize_graph()
    pub fn insert(&mut self, node: VectorAddr, hnsw: &mut RAMHnsw) {
        debug_assert!(!hnsw.layers.is_empty());
        debug_assert!(node.0 < hnsw.layers[0].out.len() as u32);

        let mut search_ep = vec![hnsw.entry_point.node];
        let vector = SearchVector::Stored(node);

        // The neighbours of the node at each layer, for insertion
        let mut layer_neighbours = Vec::with_capacity(hnsw.no_layers());
        let mut node_in_layer = false;

        // First, find the neighbours for each layer the node appears in.
        for l in (0..hnsw.no_layers()).rev() {
            if !node_in_layer && (l == 0 || hnsw.layers[l].contains(&node)) {
                node_in_layer = true;
            }

            // On upper layers, find 1 neighbour (as entrypoint to next layer)
            // On layers where the inserted node appears, find efC neighbours for creating links
            let k_neighbours = if node_in_layer { params::EF_CONSTRUCTION } else { 1 };

            let search_results: Vec<_> = self
                .searcher
                .layer_search(&vector, &hnsw.layers[l], k_neighbours, &search_ep)
                .map(|x| (x.0, x.1.score))
                .collect();

            search_ep = search_results.iter().map(|x| x.0).collect();
            if node_in_layer {
                // If finding neighbours, store them for insertion later
                layer_neighbours.push(search_results);
            }
        }

        // Insert all neighbours from the bottom layer up. This is done so that it's not possible to
        // find a node on a top layer before the edges are set on the lower layers because this can cause
        // problems during parallel insertion.
        // If edges are inserted from top to bottom, another worker might find the node in layer N and
        // follow it down to layer N-1 where edges are not yet set. Then, it cannot find neighbours in this
        // layer and the search gets stuck (no links to follow) resulting in a node with low connectivity.
        for (layer, neighbours) in layer_neighbours.into_iter().rev().enumerate() {
            self.layer_insert(
                node,
                &mut hnsw.layers[layer],
                neighbours,
                params::m_max_for_layer(layer),
            );
        }
    }
}
