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

use crate::config::{VectorConfig, flags};
use crate::data_store::{DataStore, DataStoreV1, DataStoreV2, OpenReason, ParagraphRef, VectorRef};
use crate::formula::Formula;
use crate::hnsw::*;
use crate::inverted_index::{FilterBitSet, InvertedIndexes, build_indexes};
use crate::{ParagraphAddr, VectorAddr, VectorErr, VectorR, VectorSegmentMeta, VectorSegmentMetadata};
use core::f32;
use io::{BufWriter, Write};
use memmap2::Mmap;
use std::cmp::Reverse;
use std::collections::HashSet;
use std::fs::File;
use std::io;
use std::iter::empty;
use std::path::Path;

/// How much expensive is to find a node via HNSW compared to a simple brute force scan
const HNSW_COST_FACTOR: usize = 200;

mod file_names {
    pub const HNSW: &str = "index.hnsw";
}

pub fn open(metadata: VectorSegmentMetadata, config: &VectorConfig) -> VectorR<OpenSegment> {
    let path = &metadata.path;
    let data_store: Box<dyn DataStore> = if DataStoreV1::exists(path)? {
        let data_store = DataStoreV1::open(path, &config.vector_type, OpenReason::Search)?;
        // Build the index at runtime if they do not exist. This can
        // be removed once we have migrated all existing indexes
        if !InvertedIndexes::exists(path) {
            build_indexes(path, &data_store)?;
        }
        Box::new(data_store)
    } else {
        let data_store = DataStoreV2::open(path, &config.vector_type, OpenReason::Search)?;
        // Build the index at runtime if they do not exist. This can
        // be removed once we have migrated all existing indexes
        if !InvertedIndexes::exists(path) {
            build_indexes(path, &data_store)?;
        }
        Box::new(data_store)
    };
    let hnsw_file = File::open(path.join(file_names::HNSW))?;

    let index = unsafe { Mmap::map(&hnsw_file)? };

    // Telling the OS our expected access pattern
    #[cfg(not(target_os = "windows"))]
    {
        index.advise(memmap2::Advice::Random)?;
    }

    let inverted_indexes = InvertedIndexes::open(path, metadata.records)?;
    let alive_bitset = FilterBitSet::new(metadata.records, true);

    Ok(OpenSegment {
        metadata,
        data_store,
        index,
        inverted_indexes,
        alive_bitset,
    })
}

pub fn merge(segment_path: &Path, operants: &[&OpenSegment], config: &VectorConfig) -> VectorR<OpenSegment> {
    // Sort largest operant first so we reuse as much of the HNSW as possible
    let mut operants = operants.to_vec();
    operants.sort_unstable_by_key(|o| std::cmp::Reverse(o.metadata.records));

    // Tags for all segments are the same (this should not happen, prepare_merge ensures it)
    let tags = &operants[0].metadata.index_metadata.tags;
    for dp in &operants {
        if &dp.metadata.index_metadata.tags != tags {
            return Err(crate::VectorErr::InconsistentMergeSegmentTags);
        }
    }

    // Creating the node store
    if config.flags.contains(&flags::FORCE_DATA_STORE_V1.to_string()) {
        // V1 can only merge from V1
        let mut node_producers = Vec::new();
        for dp in &operants {
            node_producers.push((
                dp.alive_paragraphs(),
                dp.data_store
                    .as_any()
                    .downcast_ref::<DataStoreV1>()
                    .ok_or(VectorErr::InconsistentMergeDataStore)?,
            ));
        }

        DataStoreV1::merge(segment_path, node_producers.as_mut_slice(), config)?;
        let data_store = DataStoreV1::open(segment_path, &config.vector_type, OpenReason::Create)?;
        merge_indexes(segment_path, data_store, operants, config)
    } else {
        let node_producers = operants
            .iter()
            .map(|dp| (dp.alive_paragraphs(), dp.data_store.as_ref()))
            .collect();
        DataStoreV2::merge(segment_path, node_producers, &config.vector_type)?;
        let data_store = DataStoreV2::open(segment_path, &config.vector_type, OpenReason::Create)?;
        merge_indexes(segment_path, data_store, operants, config)
    }
}

fn merge_indexes<DS: DataStore + 'static>(
    segment_path: &Path,
    data_store: DS,
    operants: Vec<&OpenSegment>,
    config: &VectorConfig,
) -> VectorR<OpenSegment> {
    // Check if the first segment has deletions. If it doesn't, we can reuse its HNSW index
    let has_deletions = operants[0].alive_paragraphs().count() < operants[0].data_store.stored_paragraph_count();

    let mut index;
    let start_vector_index;
    if has_deletions {
        index = RAMHnsw::new();
        start_vector_index = 0;
    } else {
        // If there are no deletions, we can reuse the first segment
        // HNSW since its indexes will match the the ones in data_store
        index = DiskHnsw::deserialize(&operants[0].index);
        start_vector_index = operants[0].data_store.stored_vector_count();
    }
    let merged_vectors_count = data_store.stored_vector_count();

    // Creating the hnsw for the new node store.
    let retriever = Retriever::new(&[], &data_store, config, -1.0);
    let mut ops = HnswOps::new(&retriever, false);
    for id in start_vector_index..merged_vectors_count {
        ops.insert(Address(id), &mut index);
    }

    let hnsw_path = segment_path.join(file_names::HNSW);
    let mut hnsw_file = File::options()
        .read(true)
        .write(true)
        .create_new(true)
        .open(hnsw_path)?;
    {
        let mut hnswf_buffer = BufWriter::new(&mut hnsw_file);
        DiskHnsw::serialize_into(&mut hnswf_buffer, merged_vectors_count, index)?;
        hnswf_buffer.flush()?;
    }

    let index = unsafe { Mmap::map(&hnsw_file)? };

    // Telling the OS our expected access pattern
    #[cfg(not(target_os = "windows"))]
    {
        index.advise(memmap2::Advice::Random)?;
    }

    build_indexes(segment_path, &data_store)?;

    let metadata = VectorSegmentMetadata {
        path: segment_path.to_path_buf(),
        records: data_store.stored_paragraph_count(),
        index_metadata: VectorSegmentMeta {
            tags: operants[0].metadata.index_metadata.tags.clone(),
        },
    };

    build_indexes(segment_path, &data_store)?;
    let inverted_indexes = InvertedIndexes::open(segment_path, merged_vectors_count)?;
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
        DataStoreV2::create(path, elems, &config.vector_type)?;
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
    let mut hnsw_file = File::options()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path.join(file_names::HNSW))?;

    let vector_count = data_store.stored_vector_count();

    // Creating the HNSW using the mmaped nodes
    let mut index = RAMHnsw::new();
    let retriever = Retriever::new(&[], &data_store, config, -1.0);
    let mut ops = HnswOps::new(&retriever, false);
    for id in 0..vector_count {
        ops.insert(Address(id), &mut index)
    }

    {
        // The HNSW is on RAM
        // Serializing the HNSW into disk
        let mut hnsw_file_buffer = BufWriter::new(&mut hnsw_file);
        DiskHnsw::serialize_into(&mut hnsw_file_buffer, vector_count, index)?;
        hnsw_file_buffer.flush()?;
    }
    let index = unsafe { Mmap::map(&hnsw_file)? };

    // Telling the OS our expected access pattern
    #[cfg(not(target_os = "windows"))]
    {
        index.advise(memmap2::Advice::Random)?;
    }

    build_indexes(path, &data_store)?;

    let metadata = VectorSegmentMetadata {
        path: path.to_path_buf(),
        records: data_store.stored_paragraph_count(),
        index_metadata: VectorSegmentMeta { tags },
    };

    build_indexes(path, &data_store)?;
    let inverted_indexes = InvertedIndexes::open(path, vector_count)?;
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
    vector_count: usize,
    temp: &'a [u8],
    data_store: &'a DS,
    min_score: f32,
}
impl<'a, DS: DataStore> Retriever<'a, DS> {
    pub fn new(temp: &'a [u8], data_store: &'a DS, config: &VectorConfig, min_score: f32) -> Retriever<'a, DS> {
        Retriever {
            temp,
            data_store,
            similarity_function: config.similarity_function(),
            vector_count: data_store.stored_vector_count(),
            min_score,
        }
    }
    fn find_vector(&'a self, x: Address) -> VectorRef<'a> {
        self.data_store.get_vector(x.into())
    }
}

impl<DS: DataStore> DataRetriever for Retriever<'_, DS> {
    fn will_need(&self, x: Address) {
        self.data_store.will_need(x.into());
    }

    fn get_vector(&self, x @ Address(addr): Address) -> &[u8] {
        if addr == self.vector_count {
            self.temp
        } else {
            self.find_vector(x).vector()
        }
    }
    fn similarity(&self, x @ Address(a0): Address, y @ Address(a1): Address) -> f32 {
        if a0 == self.vector_count {
            let y = self.find_vector(y).vector();
            (self.similarity_function)(self.temp, y)
        } else if a1 == self.vector_count {
            let x = self.find_vector(x).vector();
            (self.similarity_function)(self.temp, x)
        } else {
            let x = self.find_vector(x).vector();
            let y = self.find_vector(y).vector();
            (self.similarity_function)(x, y)
        }
    }

    fn min_score(&self) -> f32 {
        self.min_score
    }

    fn paragraph(&self, x: Address) -> ParagraphAddr {
        self.find_vector(x).paragraph()
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
    fn new(addr: Address, data_store: &dyn DataStore, score: f32) -> ScoredVector {
        let node = data_store.get_vector(addr.into());
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
    index: Mmap,
    inverted_indexes: InvertedIndexes,
    alive_bitset: FilterBitSet,
}

impl AsRef<OpenSegment> for OpenSegment {
    fn as_ref(&self) -> &OpenSegment {
        self
    }
}

impl OpenSegment {
    pub fn apply_deletion(&mut self, key: &str) {
        if let Some(deleted_ids) = self.inverted_indexes.ids_for_deletion_key(key) {
            for id in deleted_ids {
                self.alive_bitset.remove(id);
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
        self.data_store.size_bytes() + self.index.len() + self.inverted_indexes.space_usage()
    }

    pub fn get_paragraph(&self, id: ParagraphAddr) -> ParagraphRef {
        self.data_store.get_paragraph(id)
    }

    pub fn get_vector(&self, id: VectorAddr) -> VectorRef {
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
    ) -> Box<dyn Iterator<Item = ScoredVector> + '_> {
        if let Some(v1) = self.data_store.as_any().downcast_ref::<DataStoreV1>() {
            self._search(v1, query, filter, with_duplicates, results, config, min_score)
        } else if let Some(v2) = self.data_store.as_any().downcast_ref::<DataStoreV2>() {
            self._search(v2, query, filter, with_duplicates, results, config, min_score)
        } else {
            unreachable!()
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn _search<DS: DataStore>(
        &self,
        data_store: &DS,
        query: &[f32],
        filter: &Formula,
        with_duplicates: bool,
        results: usize,
        config: &VectorConfig,
        min_score: f32,
    ) -> Box<dyn Iterator<Item = ScoredVector> + '_> {
        let encoded_query = config.vector_type.encode(query);
        let retriever = Retriever::new(&encoded_query, data_store, config, min_score);
        let query_address = Address(self.metadata.records);

        let mut filter_bitset = self.inverted_indexes.filter(filter);
        if let Some(ref mut bitset) = filter_bitset {
            bitset.intersect_with(&self.alive_bitset);
        }
        // If we have no filters, just the deletions
        let bitset = filter_bitset.as_ref().unwrap_or(&self.alive_bitset);

        let count = bitset.iter().count();
        if count == 0 {
            return Box::new(empty());
        }
        let expected_traversal_scan = results * self.metadata.records / count;

        if count < expected_traversal_scan * HNSW_COST_FACTOR {
            let mut scored_results = Vec::new();
            for paragraph_addr in bitset.iter() {
                let paragraph = data_store.get_paragraph(paragraph_addr);

                // Only return the best vector match per paragraph
                let best_vector_score = paragraph
                    .vectors(&paragraph_addr)
                    .map(|va| {
                        let address = va.into();
                        let score = retriever.similarity(query_address, address);
                        Cnx(address, score)
                    })
                    .max_by(|v, w| v.1.total_cmp(&w.1))
                    .unwrap();

                if best_vector_score.1 >= min_score {
                    scored_results.push(Reverse(best_vector_score));
                }
            }
            scored_results.sort();
            return Box::new(
                scored_results
                    .into_iter()
                    .map(|Reverse(a)| ScoredVector::new(a.0, self.data_store.as_ref(), a.1))
                    .take(results),
            );
        }

        let ops = HnswOps::new(&retriever, true);
        let neighbours = ops.search(query_address, self.index.as_ref(), results, bitset, with_duplicates);
        Box::new(
            neighbours
                .into_iter()
                .map(|(address, dist)| (ScoredVector::new(address, self.data_store.as_ref(), dist)))
                .take(results),
        )
    }
}

#[cfg(test)]
mod test {
    use std::collections::{BTreeMap, HashSet};

    use nidx_protos::{Position, Representation, SentenceMetadata};
    use rand::{Rng, SeedableRng, rngs::SmallRng};
    use tempfile::tempdir;

    use crate::{
        ParagraphAddr, VectorAddr,
        config::{Similarity, VectorCardinality, VectorConfig},
        formula::Formula,
        vector_types::dense_f32::{dot_similarity, encode_vector},
    };

    use super::{Elem, create, merge};
    use nidx_protos::prost::*;

    const DIMENSION: usize = 128;

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
        let config = VectorConfig {
            similarity: Similarity::Dot,
            vector_type: crate::config::VectorType::DenseF32 { dimension: DIMENSION },
            normalize_vectors: false,
            flags: vec![],
            vector_cardinality: VectorCardinality::Single,
        };
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
        let config = VectorConfig {
            similarity: Similarity::Dot,
            vector_type: crate::config::VectorType::DenseF32 { dimension: DIMENSION },
            normalize_vectors: false,
            flags: vec![],
            vector_cardinality: VectorCardinality::Single,
        };
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

        let path_merged = tempdir()?;
        let merged_dp = merge(path_merged.path(), &[&dp1, &dp2], &config)?;

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

        let config = VectorConfig {
            similarity: Similarity::Dot,
            vector_type: crate::config::VectorType::DenseF32 { dimension: DIMENSION },
            normalize_vectors: false,
            flags: vec![],
            vector_cardinality: VectorCardinality::Single,
        };

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
        let correct = (0..100)
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
                search == brute_force
            })
            .filter(|x| *x)
            .count();

        let recall = correct as f32 / 100.0;
        println!("Assessed recall = {recall}");
        // Expected 0.90-0.92, has a little margin because HNSW can be non-deterministic
        assert!(recall >= 0.88);

        Ok(())
    }
}
