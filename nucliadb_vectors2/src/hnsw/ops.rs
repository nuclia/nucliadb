use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashSet};

use rand::distributions::Uniform;
use rand::{thread_rng, Rng};

use super::*;
use crate::database::*;
use crate::index::NodeTracker;
use crate::vector;

const NO_FILTER: &[String] = &[];
#[derive(Clone, Copy)]
struct StandardElem(pub Node, pub f32);
impl Eq for StandardElem {}
impl Ord for StandardElem {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}
impl PartialEq for StandardElem {
    fn eq(&self, other: &Self) -> bool {
        f32::eq(&self.1, &other.1)
    }
}
impl PartialOrd for StandardElem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        f32::partial_cmp(&self.1, &other.1)
    }
}

#[derive(Default, Clone)]
pub struct SearchValue {
    pub neighbours: Vec<(Node, f32)>,
}

pub struct HnswOps<'a> {
    pub txn: &'a RoTxn<'a>,
    pub tracker: &'a NodeTracker,
    pub vector_db: &'a VectorDB,
}

impl<'a> HnswOps<'a> {
    fn select_neighbours_heuristic(
        &self,
        k_neighbours: usize,
        mut candidates: Vec<(Node, f32)>,
    ) -> Vec<(Node, f32)> {
        candidates.sort_unstable_by_key(|(n, d)| std::cmp::Reverse(StandardElem(*n, *d)));
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
    fn increase_layers_with(&self, hnsw: &mut Hnsw, x: Node, level: usize) {
        while hnsw.layers.len() < level {
            let mut new_layer = GraphLayer::new();
            new_layer.add_node(x);
            hnsw.layers.push(new_layer);
        }
    }
    fn take_out_edges(&self, x: Node, layer: &mut GraphLayer) -> Vec<(Node, f32)> {
        layer
            .lout
            .get_mut(&x)
            .map(std::mem::take)
            .unwrap_or_default()
            .into_iter()
            .fold(vec![], |mut collector, (y, edge)| {
                layer.lin.get_mut(&y).and_then(|edges| edges.remove(&x));
                collector.push((y, edge.dist));
                collector
            })
    }
    fn take_in_edges(&self, x: Node, layer: &mut GraphLayer) -> Vec<(Node, f32)> {
        layer
            .lin
            .get_mut(&x)
            .map(std::mem::take)
            .unwrap_or_default()
            .into_iter()
            .fold(vec![], |mut collector, (y, edge)| {
                layer.lout.get_mut(&y).and_then(|edges| edges.remove(&x));
                collector.push((y, edge.dist));
                collector
            })
    }
    fn layer_search(
        &self,
        x: Node,
        layer: &GraphLayer,
        k_neighbours: usize,
        with_filter: &[String],
        entry_points: &[Node],
    ) -> SearchValue {
        let mut visited = HashSet::new();
        let mut candidates = BinaryHeap::new();
        let mut ms_neighbours = BinaryHeap::new();
        for ep in entry_points.iter().copied() {
            visited.insert(ep);
            let similarity =
                vector::consine_similarity(self.tracker.find(x), self.tracker.find(ep));
            candidates.push(StandardElem(ep, similarity));
            ms_neighbours.push(Reverse(StandardElem(ep, similarity)));
        }
        loop {
            match (candidates.pop(), ms_neighbours.peek().cloned()) {
                (None, _) => break,
                (Some(StandardElem(_, cs)), Some(Reverse(StandardElem(_, ws)))) if cs < ws => break,
                (Some(StandardElem(cn, _)), Some(Reverse(StandardElem(_, ws)))) => {
                    for (node, _) in layer.get_out_edges(cn).map(|(n, e)| (*n, *e)) {
                        if !visited.contains(&node) {
                            visited.insert(node);
                            let similarity = vector::consine_similarity(
                                self.tracker.find(x),
                                self.tracker.find(node),
                            );
                            if similarity > ws || ms_neighbours.len() < k_neighbours {
                                candidates.push(StandardElem(node, similarity));
                                ms_neighbours.push(Reverse(StandardElem(node, similarity)));
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
        let neighbours = ms_neighbours.into_sorted_vec();
        let neighbours: Vec<_> = neighbours
            .into_par_iter()
            .map(|Reverse(StandardElem(n, d))| (n, d))
            .filter(|(node, _)| {
                let key = self.vector_db.get_node_key(self.txn, *node).unwrap();
                with_filter
                    .iter()
                    .all(|label| self.vector_db.has_label(self.txn, key, label))
            })
            .collect();
        SearchValue { neighbours }
    }
    fn layer_insert(&self, x: Node, layer: &mut GraphLayer, entry_points: &[Node]) -> Vec<Node> {
        use params::*;
        let s_result = self.layer_search(x, layer, ef_construction(), NO_FILTER, entry_points);
        let neighbours = s_result.neighbours;
        let mut needs_repair = HashSet::new();
        let mut result = Vec::with_capacity(neighbours.len());
        layer.add_node(x);
        for (y, dist) in neighbours.iter().copied() {
            result.push(y);
            layer.add_edge(x, Edge { dist }, y);
            layer.add_edge(y, Edge { dist }, x);
            if layer.no_out_edges(y) > m_max() {
                needs_repair.insert(y);
            }
        }
        for crnt in needs_repair {
            let edges = self.take_out_edges(crnt, layer);
            let neighbours = self.select_neighbours_heuristic(m_max(), edges);
            neighbours
                .into_iter()
                .for_each(|(y, dist)| layer.add_edge(crnt, Edge { dist }, y));
        }
        result
    }
    fn layer_delete(&self, x: Node, layer: &mut GraphLayer) {
        use params::*;
        let _out_edges = self.take_out_edges(x, layer);
        let in_edges = self.take_in_edges(x, layer);
        layer.lout.remove(&x);
        layer.lin.remove(&x);
        for (crnt, _) in in_edges {
            if layer.no_out_edges(crnt) < (m() / 2) {
                let sresult = self.layer_search(x, layer, ef_construction(), NO_FILTER, &[crnt]);
                let mut candidates = self.take_out_edges(crnt, layer);
                candidates.extend(sresult.neighbours.into_iter());
                let neighbours = self.select_neighbours_heuristic(m_max(), candidates);
                neighbours
                    .into_iter()
                    .filter(|(y, _)| *y != crnt && *y != x)
                    .for_each(|(y, dist)| layer.add_edge(crnt, Edge { dist }, y));
            }
        }
    }
    pub fn delete(&self, x: Node, hnsw: &mut Hnsw) {
        hnsw.layers
            .iter_mut()
            .filter(|layer| layer.has_node(x))
            .for_each(|layer| self.layer_delete(x, layer));
        let mut maybe_empty = hnsw.layers.len();
        let mut new_ep = None;
        while maybe_empty > 0 && new_ep.is_none() {
            let attemp = maybe_empty - 1;
            new_ep = hnsw.layers[attemp].first().map(|node| (node, attemp));
            maybe_empty -= 1;
        }
        hnsw.entry_point = new_ep.map(|(node, layer)| EntryPoint { node, layer });
    }
    pub fn insert(&self, x: Node, hnsw: &mut Hnsw) {
        match hnsw.entry_point {
            None => {
                let top_level = self.get_random_layer();
                let entry_point = EntryPoint {
                    node: x,
                    layer: top_level,
                };
                self.increase_layers_with(hnsw, x, top_level);
                hnsw.entry_point = Some(entry_point)
            }
            Some(entry_point) => {
                let searchv = self.search(x, hnsw, 1, &[]);
                let (ep, _) = searchv.neighbours[0];
                let level = self.get_random_layer();
                self.increase_layers_with(hnsw, x, level);
                let top_layer = std::cmp::min(entry_point.layer, level);
                hnsw.layers[0..=top_layer]
                    .iter_mut()
                    .rev()
                    .fold(vec![ep], |eps, layer| self.layer_insert(x, layer, &eps));
                if level > entry_point.layer {
                    let new_ep = EntryPoint {
                        node: x,
                        layer: level,
                    };
                    hnsw.entry_point = Some(new_ep);
                }
            }
        }
    }
    pub fn search(
        &self,
        x: Node,
        hnsw: &Hnsw,
        k_neighbours: usize,
        with_filter: &[String],
    ) -> SearchValue {
        if let Some(entry_point) = hnsw.entry_point {
            let mut crnt_layer = entry_point.layer;
            let mut neighbours = vec![(entry_point.node, 0.)];
            while crnt_layer != 0 {
                let entry_points: Vec<_> = neighbours.into_iter().map(|(node, _)| node).collect();
                let layer_res =
                    self.layer_search(x, &hnsw.layers[crnt_layer], 1, with_filter, &entry_points);
                neighbours = layer_res.neighbours;
                crnt_layer -= 1;
            }
            let entry_points: Vec<_> = neighbours.into_iter().map(|(node, _)| node).collect();
            self.layer_search(
                x,
                &hnsw.layers[crnt_layer],
                k_neighbours,
                with_filter,
                &entry_points,
            )
        } else {
            SearchValue::default()
        }
    }
}
