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

#[cfg(test)]
mod tests;

use crate::config::{IndexEntity, VectorConfig, flags};
use crate::data_store::{DataStore, DataStoreV1, DataStoreV2, OpenReason, ParagraphRef, VectorRef};
use crate::field_list_metadata::{paragraph_alive_fields, paragraph_is_deleted};
use crate::formula::Formula;
use crate::hnsw::{self, *};
use crate::inverted_index::{self, InvertedIndexes};
use crate::inverted_index::{FilterBitSet, build_indexes};
use crate::utils::FieldKey;
use crate::vector_types::rabitq;
use crate::{ParagraphAddr, VectorAddr, VectorErr, VectorR, VectorSegmentMeta, VectorSegmentMetadata};
use core::f32;
use rayon::prelude::*;

use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};
use std::iter::empty;
use std::path::Path;
use std::time::Instant;
use tracing::{debug, trace};

pub fn open(metadata: VectorSegmentMetadata, config: &VectorConfig) -> VectorR<OpenSegment> {
    // TODO: we should get this flag from the VectorConfig or some other place
    let prewarm = false;

    let path = &metadata.path;
    let data_store: Box<dyn DataStore> = if DataStoreV1::exists(path)? {
        let data_store = DataStoreV1::open(path, &config.vector_type, OpenReason::Search { prewarm })?;
        // Build the index at runtime if they do not exist. This can
        // be removed once we have migrated all existing indexes
        if !InvertedIndexes::exists(path) {
            build_indexes(path, &config.entity, &data_store)?;
        }
        Box::new(data_store)
    } else {
        let data_store = DataStoreV2::open(path, &config.vector_type, OpenReason::Search { prewarm })?;
        // Build the index at runtime if they do not exist. This can
        // be removed once we have migrated all existing indexes
        if !InvertedIndexes::exists(path) {
            build_indexes(path, &config.entity, &data_store)?;
        }
        Box::new(data_store)
    };

    let index = open_disk_hnsw(path, prewarm)?;

    let inverted_indexes = InvertedIndexes::open(
        &config.entity,
        path,
        metadata.records,
        inverted_index::OpenOptions { prewarm },
    )?;
    let alive_bitset = FilterBitSet::new(metadata.records, true);

    Ok(OpenSegment {
        metadata,
        data_store,
        index,
        inverted_indexes,
        alive_bitset,
    })
}

pub fn merge(
    segment_path: &Path,
    mut operants: Vec<(&OpenSegment, &HashSet<FieldKey>)>,
    config: &VectorConfig,
) -> VectorR<OpenSegment> {
    // Sort largest operant first so we reuse as much of the HNSW as possible
    operants.sort_unstable_by_key(|o| std::cmp::Reverse(o.0.metadata.records));

    // Tags for all segments are the same (this should not happen, prepare_merge ensures it)
    let tags = &operants[0].0.metadata.index_metadata.tags;
    for (segment, _) in &operants {
        if &segment.metadata.index_metadata.tags != tags {
            return Err(crate::VectorErr::InconsistentMergeSegmentTags);
        }
    }

    let segments = operants.iter().map(|(s, _)| *s).collect();

    // Creating the node store
    if config.flags.contains(&flags::FORCE_DATA_STORE_V1.to_string()) {
        // V1 can only merge from V1
        let mut node_producers = Vec::new();
        for (segment, _) in &operants {
            node_producers.push((
                segment.alive_paragraphs(),
                segment
                    .data_store
                    .as_any()
                    .downcast_ref::<DataStoreV1>()
                    .ok_or(VectorErr::InconsistentMergeDataStore)?,
            ));
        }

        DataStoreV1::merge(segment_path, node_producers.as_mut_slice(), config)?;
        let data_store = DataStoreV1::open(segment_path, &config.vector_type, OpenReason::Create)?;
        merge_indexes(segment_path, data_store, segments, config)
    } else {
        let node_producers = operants
            .iter()
            .map(|(s, _)| (s.alive_paragraphs(), s.data_store.as_ref()))
            .collect();

        // If the metadata represents the list of fields the paragraph appears in, we need to get the new list as the
        // union of the fields in all segments, taking deletions into account
        let paragraph_deduplicator = if matches!(config.entity, IndexEntity::Relation) {
            let mut per_paragraph_alive_fields: HashMap<String, Vec<FieldKey>> = HashMap::new();
            for (segment, deletions) in &operants {
                for paragraph_id in segment.alive_paragraphs() {
                    let paragraph = segment.data_store.get_paragraph(paragraph_id);
                    for f in paragraph_alive_fields(&paragraph, deletions) {
                        per_paragraph_alive_fields
                            .entry(paragraph.id().to_string())
                            .or_default()
                            .push(f.to_owned());
                    }
                }
            }

            Some(
                per_paragraph_alive_fields
                    .into_iter()
                    .map(|(k, v)| (k, bincode::encode_to_vec(v, bincode::config::standard()).unwrap()))
                    .collect(),
            )
        } else {
            None
        };
        DataStoreV2::merge(segment_path, node_producers, config, paragraph_deduplicator)?;

        let data_store = DataStoreV2::open(segment_path, &config.vector_type, OpenReason::Create)?;
        merge_indexes(segment_path, data_store, segments, config)
    }
}

fn merge_indexes<DS: DataStore + 'static>(
    segment_path: &Path,
    data_store: DS,
    operants: Vec<&OpenSegment>,
    config: &VectorConfig,
) -> VectorR<OpenSegment> {
    // Check if the first segment has deletions. If it doesn't, we can reuse its HNSW index
    let has_deletions =
        operants[0].alive_paragraphs().count() < operants[0].data_store.stored_paragraph_count() as usize;

    let mut index;
    let start_vector_index;
    if has_deletions {
        index = RAMHnsw::new();
        start_vector_index = 0;
    } else {
        // If there are no deletions, we can reuse the first segment
        // HNSW since its indexes will match the the ones in data_store
        index = operants[0].index.deserialize()?;
        index.fix_broken_graph();
        start_vector_index = operants[0].data_store.stored_vector_count();
    }
    let merged_vectors_count = data_store.stored_vector_count();

    // Creating the hnsw for the new node store.
    let retriever = Retriever::new(&data_store, config, -1.0);
    let mut builder = HnswBuilder::new(&retriever);
    builder.initialize_graph(&mut index, start_vector_index, merged_vectors_count);
    (start_vector_index..merged_vectors_count)
        .into_par_iter()
        .for_each(|id| builder.insert(VectorAddr(id), &index));

    DiskHnswV2::serialize_to(segment_path, merged_vectors_count, &index)?;
    let index = open_disk_hnsw(segment_path, false)?;

    let metadata = VectorSegmentMetadata {
        path: segment_path.to_path_buf(),
        records: data_store.stored_paragraph_count() as usize,
        index_metadata: VectorSegmentMeta {
            tags: operants[0].metadata.index_metadata.tags.clone(),
        },
    };

    build_indexes(segment_path, &config.entity, &data_store)?;
    let inverted_indexes = InvertedIndexes::open(
        &config.entity,
        segment_path,
        merged_vectors_count as usize,
        inverted_index::OpenOptions { prewarm: false },
    )?;

    let alive_bitset = FilterBitSet::new(metadata.records, true);

    Ok(OpenSegment {
        metadata,
        data_store: Box::new(data_store),
        index,
        inverted_indexes,
        alive_bitset,
    })
}

pub fn create(path: &Path, elems: Vec<Elem>, config: &VectorConfig, tags: HashSet<String>) -> VectorR<OpenSegment> {
    // Check dimensions
    let dim = config.vector_type.dimension();
    for e in &elems {
        for v in &e.vectors {
            if v.len() != dim {
                return Err(crate::VectorErr::InconsistentDimensions {
                    index_config: dim,
                    vector: v.len(),
                });
            }
        }
    }

    // Serializing nodes on disk
    // Nodes are stored on disk and mmaped.
    // Then trigger the rest of the creation (indexes)
    if config.flags.contains(&flags::FORCE_DATA_STORE_V1.to_string()) {
        // Double check vector cardinality
        if elems.iter().any(|e| e.vectors.len() != 1) {
            return Err(crate::VectorErr::InvalidConfiguration(
                "DataStore v1 not supported with multi-vectors",
            ));
        }
        DataStoreV1::create(path, elems, &config.vector_type)?;
        create_indexes(
            path,
            DataStoreV1::open(path, &config.vector_type, OpenReason::Create)?,
            config,
            tags,
        )
    } else {
        DataStoreV2::create(path, elems, config)?;
        create_indexes(
            path,
            DataStoreV2::open(path, &config.vector_type, OpenReason::Create)?,
            config,
            tags,
        )
    }
}

fn create_indexes<DS: DataStore + 'static>(
    path: &Path,
    data_store: DS,
    config: &VectorConfig,
    tags: HashSet<String>,
) -> VectorR<OpenSegment> {
    let vector_count = data_store.stored_vector_count();

    // Creating the HNSW using the mmaped nodes
    let mut index = RAMHnsw::new();
    let retriever = Retriever::new(&data_store, config, -1.0);
    let mut builder = HnswBuilder::new(&retriever);
    builder.initialize_graph(&mut index, 0, vector_count);
    (0..vector_count)
        .into_par_iter()
        .for_each(|id| builder.insert(VectorAddr(id), &index));

    // The HNSW is on RAM
    // Serializing the HNSW into disk
    DiskHnswV2::serialize_to(path, vector_count, &index)?;

    let index = open_disk_hnsw(path, false)?;

    let metadata = VectorSegmentMetadata {
        path: path.to_path_buf(),
        records: data_store.stored_paragraph_count() as usize,
        index_metadata: VectorSegmentMeta { tags },
    };

    build_indexes(path, &config.entity, &data_store)?;
    let inverted_indexes = InvertedIndexes::open(
        &config.entity,
        path,
        vector_count as usize,
        inverted_index::OpenOptions { prewarm: false },
    )?;
    let alive_bitset = FilterBitSet::new(metadata.records, true);

    Ok(OpenSegment {
        metadata,
        data_store: Box::new(data_store),
        index,
        inverted_indexes,
        alive_bitset,
    })
}

pub struct Retriever<'a, DS: DataStore> {
    similarity_function: fn(&[u8], &[u8]) -> f32,
    data_store: &'a DS,
    min_score: f32,
}
impl<'a, DS: DataStore> Retriever<'a, DS> {
    pub fn new(data_store: &'a DS, config: &VectorConfig, min_score: f32) -> Retriever<'a, DS> {
        Retriever {
            data_store,
            similarity_function: config.similarity_function(),
            min_score,
        }
    }
}

impl<DS: DataStore> DataRetriever for Retriever<'_, DS> {
    fn will_need(&self, x: VectorAddr) {
        self.data_store.will_need(x);
    }

    fn will_need_vector(&self, x: VectorAddr) {
        if self.data_store.has_quantized() {
            self.data_store.will_need_quantized(x);
        } else {
            self.data_store.will_need(x);
        }
    }

    fn get_vector(&self, x: VectorAddr) -> &[u8] {
        self.data_store.get_vector(x).vector()
    }

    fn similarity(&self, x: VectorAddr, y: &SearchVector) -> f32 {
        match y {
            SearchVector::Stored(vector_addr) => {
                let x = self.data_store.get_vector(x).vector();
                let y = self.data_store.get_vector(*vector_addr).vector();
                (self.similarity_function)(x, y)
            }
            SearchVector::Query(query) => {
                let x = self.data_store.get_vector(x).vector();
                (self.similarity_function)(x, query)
            }
            SearchVector::RabitQ(query) => {
                let x = self.data_store.get_quantized_vector(x);
                let (est, _err) = query.similarity(x);
                est
            }
        }
    }

    fn similarity_upper_bound(&self, x: VectorAddr, y: &SearchVector) -> EstimatedScore {
        match y {
            SearchVector::RabitQ(query) => {
                let x = self.data_store.get_quantized_vector(x);
                let (est, err) = query.similarity(x);
                EstimatedScore::new_with_error(est, err)
            }
            _ => EstimatedScore::new_exact(self.similarity(x, y)),
        }
    }

    fn min_score(&self) -> f32 {
        self.min_score
    }

    fn paragraph(&self, x: VectorAddr) -> ParagraphAddr {
        self.data_store.get_vector(x).paragraph()
    }
}

#[derive(Clone, Debug)]
pub struct Elem {
    pub key: String,
    pub vectors: Vec<Vec<f32>>,
    pub metadata: Option<Vec<u8>>,
    pub labels: Vec<String>,
}
impl Elem {
    pub fn new(key: String, vector: Vec<f32>, labels: Vec<String>, metadata: Option<Vec<u8>>) -> Elem {
        Elem {
            labels,
            metadata,
            key,
            vectors: vec![vector],
        }
    }

    pub fn new_multivector(
        key: String,
        vectors: Vec<Vec<f32>>,
        labels: Vec<String>,
        metadata: Option<Vec<u8>>,
    ) -> Elem {
        Elem {
            labels,
            metadata,
            key,
            vectors,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ScoredVector<'a> {
    score: f32,
    vector: VectorRef<'a>,
}

impl ScoredVector<'_> {
    fn new(addr: VectorAddr, data_store: &dyn DataStore, score: f32) -> ScoredVector<'_> {
        let node = data_store.get_vector(addr);
        ScoredVector { score, vector: node }
    }
    pub fn score(&self) -> f32 {
        self.score
    }
    pub fn vector(&self) -> &[u8] {
        self.vector.vector()
    }
    pub fn paragraph(&self) -> ParagraphAddr {
        self.vector.paragraph()
    }
}

pub struct OpenSegment {
    metadata: VectorSegmentMetadata,
    data_store: Box<dyn DataStore>,
    index: Box<dyn DiskHnsw>,
    inverted_indexes: InvertedIndexes,
    alive_bitset: FilterBitSet,
}

impl AsRef<OpenSegment> for OpenSegment {
    fn as_ref(&self) -> &OpenSegment {
        self
    }
}

impl OpenSegment {
    pub fn apply_deletions(&mut self, keys: &HashSet<FieldKey>) {
        match &self.inverted_indexes {
            InvertedIndexes::Paragraph(indexes) => {
                for key in keys {
                    for id in indexes.ids_for_deletion_key(key) {
                        self.alive_bitset.remove(id);
                    }
                }
            }
            InvertedIndexes::Relation(indexes) => {
                let affected_paragraphs = keys.iter().flat_map(|k| indexes.ids_for_field_key(k));
                for paragraph_id in affected_paragraphs {
                    let paragraph = self.data_store.get_paragraph(paragraph_id);
                    if paragraph_is_deleted(&paragraph, keys) {
                        self.alive_bitset.remove(paragraph_id);
                    }
                }
            }
        }
    }

    pub fn into_metadata(self) -> VectorSegmentMetadata {
        self.metadata
    }

    pub fn tags(&self) -> &HashSet<String> {
        &self.metadata.index_metadata.tags
    }

    pub fn alive_paragraphs(&self) -> impl Iterator<Item = ParagraphAddr> + '_ {
        self.alive_bitset.iter()
    }

    pub fn space_usage(&self) -> usize {
        self.data_store.size_bytes() + self.index.size() + self.inverted_indexes.space_usage()
    }

    pub fn get_paragraph(&self, id: ParagraphAddr) -> ParagraphRef<'_> {
        self.data_store.get_paragraph(id)
    }

    pub fn get_vector(&self, id: VectorAddr) -> VectorRef<'_> {
        self.data_store.get_vector(id)
    }

    pub fn search(
        &self,
        query: &[f32],
        filter: &Formula,
        with_duplicates: bool,
        results: usize,
        config: &VectorConfig,
        min_score: f32,
    ) -> Box<dyn Iterator<Item = ScoredVector<'_>> + '_> {
        if let Some(v1) = self.data_store.as_any().downcast_ref::<DataStoreV1>() {
            self._search(v1, query, filter, with_duplicates, results, config, min_score)
        } else if let Some(v2) = self.data_store.as_any().downcast_ref::<DataStoreV2>() {
            self._search(v2, query, filter, with_duplicates, results, config, min_score)
        } else {
            unreachable!()
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn _search<'a, DS: DataStore>(
        &'a self,
        data_store: &'a DS,
        query: &[f32],
        filter: &Formula,
        with_duplicates: bool,
        top_k: usize,
        config: &VectorConfig,
        min_score: f32,
    ) -> Box<dyn Iterator<Item = ScoredVector<'a>> + '_> {
        let raw_query = SearchVector::Query(config.vector_type.encode(query));
        let rabitq = data_store.has_quantized() && !config.flags.contains(&flags::DISABLE_RABITQ_SEARCH.to_string());
        let encoded_query = if rabitq {
            let rabitq = rabitq::QueryVector::from_vector(query, &config.vector_type);
            &SearchVector::RabitQ(rabitq)
        } else {
            &raw_query
        };
        let retriever = Retriever::new(data_store, config, min_score);

        let filter_bitset = if filter.non_empty() {
            let InvertedIndexes::Paragraph(inverted_indexes) = &self.inverted_indexes else {
                unreachable!("Cannot filter without paragraph indexes");
            };
            let mut filter_bitset = inverted_indexes.filter(filter);
            if let Some(ref mut bitset) = filter_bitset {
                bitset.intersect_with(&self.alive_bitset);
            }
            filter_bitset
        } else {
            None
        };
        // If we have no filters, just the deletions
        let bitset = filter_bitset.as_ref().unwrap_or(&self.alive_bitset);

        let matching = bitset.iter().count();
        if matching == 0 {
            return Box::new(empty());
        }

        let t = Instant::now();
        let method;
        let results = if use_hnsw(self.metadata.records, matching, top_k, rabitq) {
            method = "hnsw";
            let ops = HnswSearcher::new(&retriever, true);
            let filter = NodeFilter::new(bitset, with_duplicates, config);
            let neighbours = if let Some(v1) = self.index.as_any().downcast_ref::<DiskHnswV1>() {
                ops.search(encoded_query, v1, top_k, filter)
            } else if let Some(v2) = self.index.as_any().downcast_ref::<DiskHnswV2>() {
                ops.search(encoded_query, v2, top_k, filter)
            } else {
                unreachable!()
            };

            Box::new(
                neighbours
                    .into_iter()
                    .map(|(address, dist)| ScoredVector::new(address, self.data_store.as_ref(), dist))
                    .take(top_k),
            )
        } else {
            method = "brute force";
            self.brute_force_search(bitset, top_k, retriever, encoded_query, &raw_query)
        };

        let time = t.elapsed();
        let segment_path = &self.metadata.path;
        let records = self.metadata.records;
        debug!(?time, ?segment_path, records, matching, "Segment search using {method}");

        results
    }

    fn brute_force_search<'a, DS: DataStore>(
        &'a self,
        bitset: &FilterBitSet,
        top_k: usize,
        retriever: Retriever<'a, DS>,
        encoded_query: &SearchVector,
        raw_query: &SearchVector,
    ) -> Box<dyn Iterator<Item = ScoredVector<'a>> + '_> {
        let mut scored_results = Vec::new();

        let t = Instant::now();
        for paragraph_addr in bitset.iter() {
            let paragraph = retriever.data_store.get_paragraph(paragraph_addr);

            // Only return the best vector match per paragraph (only relevant with multi-vectors)
            let best_vector_score = paragraph
                .vectors(&paragraph_addr)
                .map(|va| {
                    let score = retriever.similarity_upper_bound(va, encoded_query);
                    (va, score)
                })
                .max_by(|v, w| v.1.score.total_cmp(&w.1.score))
                .unwrap();

            if best_vector_score.1.upper_bound >= retriever.min_score {
                scored_results.push(best_vector_score);
            }
        }
        let time = t.elapsed();
        trace!(?time, "Brute force search: retrieve");

        let t = Instant::now();
        let results: Box<dyn Iterator<Item = _>> = if matches!(encoded_query, SearchVector::RabitQ(_)) {
            // If using RabitQ, rerank top results using the raw vectors
            Box::new(
                rabitq::rerank_top(scored_results, top_k, &retriever, raw_query)
                    .into_iter()
                    .map(|Reverse(Cnx(addr, score))| ScoredVector::new(addr, self.data_store.as_ref(), score)),
            )
        } else {
            // If using raw vectors, sort by score and take top_k
            scored_results.sort_unstable_by(|a, b| b.1.score.total_cmp(&a.1.score));
            Box::new(
                scored_results
                    .into_iter()
                    .map(|a| ScoredVector::new(a.0, self.data_store.as_ref(), a.1.score))
                    .take(top_k),
            )
        };
        let time = t.elapsed();
        trace!(?time, "Brute force search: rerank");

        results
    }
}

fn use_hnsw(total_nodes: usize, matching_nodes: usize, top_k: usize, has_rabitq: bool) -> bool {
    // Cost of a full vector comparison compared to a quantized vector comparison
    let full_cost: usize;
    // How many more vectors are evaluated in layer_search for quantized vectors
    let search_mult: usize;
    // How many vectors are reranked
    let rerank_mult: usize;
    if has_rabitq {
        full_cost = 16;
        search_mult = rabitq::RERANKING_FACTOR * 3 / 4;
        rerank_mult = rabitq::RERANKING_FACTOR / 2;
    } else {
        full_cost = 1;
        search_mult = 1;
        rerank_mult = 0;
    }

    // Estimated vectors visited during hnsw search (quantized vectors)
    let hnsw_rq = ((total_nodes as f32).ln() - 2.0).powi(2) * (top_k as f32).ln() * search_mult as f32;
    // Estimated vectors visited in closest_up_nodes (full vectors)
    let hnsw_full = (top_k * rerank_mult) + (top_k * hnsw::M * total_nodes / matching_nodes);

    // Quantized vectors evaluated with brute-force
    let bf_rq = matching_nodes;
    // Estimated full vectors to be evaluated during rerank in brute-force
    let bf_full = top_k * rerank_mult;

    let hnsw_cost = hnsw_rq as usize + hnsw_full * full_cost;
    let bf_cost = bf_rq + bf_full * full_cost;

    let use_hnsw = hnsw_cost < bf_cost;
    debug!(hnsw_cost, bf_cost, use_hnsw, "Estimated search costs");

    use_hnsw
}

#[cfg(test)]
mod test {
    use std::collections::{BTreeMap, HashSet};

    use nidx_protos::{Position, Representation, SentenceMetadata};
    use rand::{Rng, SeedableRng, rngs::SmallRng};
    use tempfile::tempdir;

    use crate::{
        ParagraphAddr, VectorAddr,
        config::VectorConfig,
        formula::Formula,
        vector_types::dense_f32::{dot_similarity, encode_vector},
    };

    use super::{Elem, create, merge};
    use nidx_protos::prost::*;

    const DIMENSION: usize = 256;

    fn random_vector(rng: &mut impl Rng) -> Vec<f32> {
        let v: Vec<f32> = (0..DIMENSION).map(|_| rng.gen_range(-1.0..1.0)).collect();
        normalize(v)
    }

    fn normalize(v: Vec<f32>) -> Vec<f32> {
        let mut modulus = 0.0;
        for w in &v {
            modulus += w * w;
        }
        modulus = modulus.powf(0.5);

        v.into_iter().map(|w| w / modulus).collect()
    }

    fn random_nearby_vector(rng: &mut impl Rng, close_to: &[f32], distance: f32) -> Vec<f32> {
        // Create a random vector of low modulus
        let fuzz = random_vector(rng);
        let v = close_to
            .iter()
            .zip(fuzz.iter())
            .map(|(v, fuzz)| v + fuzz * distance)
            .collect();
        normalize(v)
    }

    fn random_key(rng: &mut impl Rng) -> String {
        format!("{:032x?}/f/file/0-100", rng.r#gen::<u128>())
    }

    fn random_string(rng: &mut impl Rng) -> String {
        String::from_utf8_lossy(&[rng.gen_range(40..110)].repeat(rng.gen_range(1..16))).to_string()
    }

    fn similarity(x: &[f32], y: &[f32]) -> f32 {
        dot_similarity(&encode_vector(x), &encode_vector(y))
    }

    fn random_elem(rng: &mut impl Rng) -> (Elem, Vec<String>) {
        let labels: Vec<_> = (0..rng.gen_range(0..=2)).map(|_| random_string(rng)).collect();
        let metadata = SentenceMetadata {
            position: Some(Position {
                index: 1,
                start: 2,
                end: 3,
                page_number: 4,
                in_page: true,
                start_seconds: vec![],
                end_seconds: vec![],
            }),
            page_with_visual: false,
            representation: Some(Representation {
                is_a_table: false,
                file: random_string(rng),
            }),
        };
        (
            Elem::new(
                random_key(rng),
                random_vector(rng),
                labels.clone(),
                Some(metadata.encode_to_vec()),
            ),
            labels,
        )
    }

    #[test]
    fn test_save_recall_aligned_data() -> anyhow::Result<()> {
        let config = VectorConfig::for_paragraphs(crate::config::VectorType::DenseF32 { dimension: DIMENSION });
        let mut rng = SmallRng::seed_from_u64(1234567890);
        let temp_dir = tempdir()?;

        // Create a segment with random data of different length
        let path = temp_dir.path();
        let elems = (0..100).map(|_| random_elem(&mut rng)).collect::<Vec<_>>();
        let dp = create(
            path,
            elems.iter().cloned().map(|x| x.0).collect(),
            &config,
            HashSet::new(),
        )?;

        for (i, (elem, mut labels)) in elems.into_iter().enumerate() {
            let vector = dp.data_store.get_vector(VectorAddr(i as u32));
            assert_eq!(config.vector_type.encode(&elem.vectors[0]), vector.vector());

            let paragraph = dp.data_store.get_paragraph(ParagraphAddr(i as u32));
            assert_eq!(elem.key, paragraph.id());

            // Compare metadata as the decoded protobug. Tthe absolute stored value may have trailing padding
            // from vectors, but the decoding step should ignore it
            assert_eq!(
                SentenceMetadata::decode(elem.metadata.as_ref().unwrap().as_slice()),
                SentenceMetadata::decode(paragraph.metadata())
            );

            // Compare labels
            labels.sort();
            let mut paragraph_labels = paragraph.labels();
            paragraph_labels.sort();
            assert_eq!(labels, paragraph_labels);
        }

        Ok(())
    }

    #[test]
    fn test_save_recall_aligned_data_after_merge() -> anyhow::Result<()> {
        let config = VectorConfig::for_paragraphs(crate::config::VectorType::DenseF32 { dimension: DIMENSION });
        let mut rng = SmallRng::seed_from_u64(1234567890);

        // Create two segments with random data of different length
        let path1 = tempdir()?;
        let elems1 = (0..10).map(|_| random_elem(&mut rng)).collect::<Vec<_>>();
        let dp1 = create(
            path1.path(),
            elems1.iter().cloned().map(|x| x.0).collect(),
            &config,
            HashSet::new(),
        )?;

        let path2 = tempdir()?;
        let elems2 = (0..10).map(|_| random_elem(&mut rng)).collect::<Vec<_>>();
        let dp2 = create(
            path2.path(),
            elems2.iter().cloned().map(|x| x.0).collect(),
            &config,
            HashSet::new(),
        )?;

        let no_dels = HashSet::new();
        let work = vec![(&dp1, &no_dels), (&dp2, &no_dels)];
        let path_merged = tempdir()?;
        let merged_dp = merge(path_merged.path(), work, &config)?;

        for (i, (elem, mut labels)) in elems1.into_iter().chain(elems2.into_iter()).enumerate() {
            let vector = merged_dp.data_store.get_vector(VectorAddr(i as u32));
            assert_eq!(config.vector_type.encode(&elem.vectors[0]), vector.vector());

            let paragraph = merged_dp.data_store.get_paragraph(ParagraphAddr(i as u32));
            assert_eq!(elem.key, paragraph.id());

            // Compare metadata as the decoded protobug. Tthe absolute stored value may have trailing padding
            // from vectors, but the decoding step should ignore it
            assert_eq!(
                SentenceMetadata::decode(elem.metadata.as_ref().unwrap().as_slice()),
                SentenceMetadata::decode(paragraph.metadata())
            );

            // Compare labels
            labels.sort();
            let mut paragraph_labels = paragraph.labels();
            paragraph_labels.sort();
            assert_eq!(labels, paragraph_labels);
        }

        Ok(())
    }

    #[test]
    fn test_recall_clustered_data() -> anyhow::Result<()> {
        // This test is a simplified version of the synthetic_recall_benchmark, with smaller data for faster runs
        // It's run here as a sanity check to get a big warning in case we mess up recall too badly
        // You can play with the benchmark version in order to get more information, tweak parameters, etc.
        let mut rng = SmallRng::seed_from_u64(1234567890);
        let mut elems = BTreeMap::new();

        // Create some clusters
        let mut center = random_vector(&mut rng);
        for _ in 0..4 {
            // 80 tightly clustered vectors, ideally more than Mmax0
            for _ in 0..80 {
                elems.insert(random_key(&mut rng), random_nearby_vector(&mut rng, &center, 0.01));
            }
            // 80 tightly clustered vectors
            for _ in 0..80 {
                elems.insert(random_key(&mut rng), random_nearby_vector(&mut rng, &center, 0.03));
            }
            // Next cluster is nearby
            center = random_nearby_vector(&mut rng, &center, 0.1);
        }

        let config = VectorConfig::for_paragraphs(crate::config::VectorType::DenseF32 { dimension: DIMENSION });

        // Create a segment
        let temp_dir = tempdir()?;
        let dp = create(
            temp_dir.path(),
            elems
                .iter()
                .map(|(k, v)| Elem::new(k.clone(), v.clone(), Default::default(), None))
                .collect(),
            &config,
            HashSet::new(),
        )?;

        // Search a few times
        let correct: f32 = (0..100)
            .map(|_| {
                // Search near an existing segment (simulates that the query is related to the data)
                let base_v = elems.values().nth(rng.gen_range(0..elems.len())).unwrap();
                let query = random_nearby_vector(&mut rng, base_v, 0.05);

                let mut similarities: Vec<_> = elems.iter().map(|(k, v)| (k, similarity(v, &query))).collect();
                similarities.sort_unstable_by(|a, b| a.1.total_cmp(&b.1).reverse());

                let results: Vec<_> = dp.search(&query, &Formula::new(), false, 5, &config, 0.0).collect();

                let search: Vec<_> = results
                    .iter()
                    .map(|r| dp.data_store.get_paragraph(r.paragraph()).id().to_string())
                    .collect();

                let brute_force: Vec<_> = similarities.iter().take(5).map(|r| r.0.clone()).collect();
                let mut r = 0.0;
                for b in brute_force {
                    if search.contains(&b) {
                        r += 0.2;
                    }
                }
                r
            })
            .sum();

        let recall = correct / 100.0;
        println!("Assessed recall = {recall}");
        // Expected ~0.98, has a little margin because HNSW can be non-deterministic
        assert!(recall >= 0.95);

        Ok(())
    }
}
