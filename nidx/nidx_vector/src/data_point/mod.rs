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

pub mod disk_hnsw;
pub mod ops_hnsw;
pub mod ram_hnsw;

mod params;
#[cfg(test)]
mod tests;

use crate::config::VectorConfig;
use crate::data_store::{DataStore, Node};
use crate::data_types::{trie, trie_ram};
use crate::formula::Formula;
use crate::inverted_index::{InvertedIndexes, build_indexes};
use crate::{VectorR, VectorSegmentMeta, VectorSegmentMetadata};
use bit_set::BitSet;
use bit_vec::BitVec;
use disk_hnsw::DiskHnsw;
use io::{BufWriter, Write};
use memmap2::Mmap;
use ops_hnsw::{Cnx, HnswOps};
use ram_hnsw::RAMHnsw;
use std::cmp::Reverse;
use std::collections::HashSet;
use std::fs::File;
use std::io;
use std::iter::empty;
use std::path::Path;

pub use ops_hnsw::DataRetriever;

/// How much expensive is to find a node via HNSW compared to a simple brute force scan
const HNSW_COST_FACTOR: usize = 200;

mod file_names {
    pub const HNSW: &str = "index.hnsw";
}

pub fn open(metadata: VectorSegmentMetadata) -> VectorR<OpenDataPoint> {
    let path = &metadata.path;
    let data_store = DataStore::open(path)?;
    let hnsw_file = File::open(path.join(file_names::HNSW))?;

    let index = unsafe { Mmap::map(&hnsw_file)? };

    // Telling the OS our expected access pattern
    #[cfg(not(target_os = "windows"))]
    {
        index.advise(memmap2::Advice::Sequential)?;
    }

    // Build the index at runtime if they do not exist. This can
    // be removed once we have migrated all existing indexes
    if !InvertedIndexes::exists(path) {
        build_indexes(path, &data_store)?;
    }
    let inverted_indexes = InvertedIndexes::open(path, metadata.records)?;
    let alive_bitset = BitSet::from_bit_vec(BitVec::from_elem(metadata.records, true));

    Ok(OpenDataPoint {
        metadata,
        data_store,
        index,
        inverted_indexes,
        alive_bitset,
    })
}

pub fn merge(data_point_path: &Path, operants: &[&OpenDataPoint], config: &VectorConfig) -> VectorR<OpenDataPoint> {
    let hnsw_path = data_point_path.join(file_names::HNSW);
    let mut hnsw_file = File::options()
        .read(true)
        .write(true)
        .create_new(true)
        .open(hnsw_path)?;

    // Sort largest operant first so we reuse as much of the HNSW as possible
    let mut operants = operants.iter().collect::<Vec<_>>();
    operants.sort_unstable_by_key(|o| std::cmp::Reverse(o.metadata.records));

    // Tags for all segments are the same (this should not happen, prepare_merge ensures it)
    let tags = &operants[0].metadata.index_metadata.tags;
    for dp in &operants {
        if &dp.metadata.index_metadata.tags != tags {
            return Err(crate::VectorErr::InconsistentMergeSegmentTags);
        }
    }

    // Creating the node store
    let mut node_producers: Vec<_> = operants.iter().map(|dp| (dp.alive_nodes(), &dp.data_store)).collect();
    let has_deletions = DataStore::merge(data_point_path, node_producers.as_mut_slice(), config)?;
    let data_store = DataStore::open(data_point_path)?;
    let no_nodes = data_store.stored_elements();

    let mut index;
    let start_node_index;
    if has_deletions {
        index = RAMHnsw::new();
        start_node_index = 0;
    } else {
        // If there are no deletions, we can reuse the first segment
        // HNSW since its indexes will match the the ones in data_store
        index = DiskHnsw::deserialize(&operants[0].index);
        start_node_index = operants[0].data_store.stored_elements();
    }

    // Creating the hnsw for the new node store.
    let retriever = Retriever::new(&[], &data_store, config, -1.0);
    let mut ops = HnswOps::new(&retriever, false);
    for id in start_node_index..no_nodes {
        ops.insert(Address(id), &mut index);
    }

    {
        let mut hnswf_buffer = BufWriter::new(&mut hnsw_file);
        DiskHnsw::serialize_into(&mut hnswf_buffer, no_nodes, index)?;
        hnswf_buffer.flush()?;
    }

    let index = unsafe { Mmap::map(&hnsw_file)? };

    // Telling the OS our expected access pattern
    #[cfg(not(target_os = "windows"))]
    {
        index.advise(memmap2::Advice::Sequential)?;
    }

    build_indexes(data_point_path, &data_store)?;

    let metadata = VectorSegmentMetadata {
        path: data_point_path.to_path_buf(),
        records: no_nodes,
        index_metadata: VectorSegmentMeta { tags: tags.clone() },
    };

    build_indexes(data_point_path, &data_store)?;
    let inverted_indexes = InvertedIndexes::open(data_point_path, no_nodes)?;
    let alive_bitset = BitSet::from_bit_vec(BitVec::from_elem(metadata.records, true));

    Ok(OpenDataPoint {
        metadata,
        data_store,
        index,
        inverted_indexes,
        alive_bitset,
    })
}

pub fn create(path: &Path, elems: Vec<Elem>, config: &VectorConfig, tags: HashSet<String>) -> VectorR<OpenDataPoint> {
    // Check dimensions
    if let Some(dim) = config.vector_type.dimension() {
        if let Some(elem) = elems.iter().find(|elem| elem.vector.len() != dim) {
            return Err(crate::VectorErr::InconsistentDimensions {
                index_config: dim,
                vector: elem.vector.len(),
            });
        }
    }

    let mut hnsw_file = File::options()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path.join(file_names::HNSW))?;

    // Serializing nodes on disk
    // Nodes are stored on disk and mmaped.
    DataStore::create(path, elems, &config.vector_type)?;
    let data_store = DataStore::open(path)?;
    let no_nodes = data_store.stored_elements();

    // Creating the HNSW using the mmaped nodes
    let mut index = RAMHnsw::new();
    let retriever = Retriever::new(&[], &data_store, config, -1.0);
    let mut ops = HnswOps::new(&retriever, false);
    for id in 0..no_nodes {
        ops.insert(Address(id), &mut index)
    }

    {
        // The HNSW is on RAM
        // Serializing the HNSW into disk
        let mut hnsw_file_buffer = BufWriter::new(&mut hnsw_file);
        DiskHnsw::serialize_into(&mut hnsw_file_buffer, no_nodes, index)?;
        hnsw_file_buffer.flush()?;
    }
    let index = unsafe { Mmap::map(&hnsw_file)? };

    // Telling the OS our expected access pattern
    #[cfg(not(target_os = "windows"))]
    {
        index.advise(memmap2::Advice::Sequential)?;
    }

    build_indexes(path, &data_store)?;

    let metadata = VectorSegmentMetadata {
        path: path.to_path_buf(),
        records: no_nodes,
        index_metadata: VectorSegmentMeta { tags },
    };

    build_indexes(path, &data_store)?;
    let inverted_indexes = InvertedIndexes::open(path, no_nodes)?;
    let alive_bitset = BitSet::from_bit_vec(BitVec::from_elem(metadata.records, true));

    Ok(OpenDataPoint {
        metadata,
        data_store,
        index,
        inverted_indexes,
        alive_bitset,
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Address(usize);

pub struct Retriever<'a> {
    similarity_function: fn(&[u8], &[u8]) -> f32,
    no_nodes: usize,
    temp: &'a [u8],
    data_store: &'a DataStore,
    min_score: f32,
    vector_len_bytes: usize,
}
impl<'a> Retriever<'a> {
    pub fn new(temp: &'a [u8], data_store: &'a DataStore, config: &VectorConfig, min_score: f32) -> Retriever<'a> {
        let no_nodes = data_store.stored_elements();
        let vector_len_bytes = config.vector_len_bytes();
        Retriever {
            temp,
            data_store,
            similarity_function: config.similarity_function(),
            no_nodes,
            min_score,
            vector_len_bytes,
        }
    }
    fn find_node(&'a self, Address(x): Address) -> Node<'a> {
        self.data_store.get_value(x)
    }
}

impl DataRetriever for Retriever<'_> {
    fn will_need(&self, Address(x): Address) {
        self.data_store.will_need(x, self.vector_len_bytes);
    }

    fn get_vector(&self, x @ Address(addr): Address) -> &[u8] {
        if addr == self.no_nodes {
            self.temp
        } else {
            self.find_node(x).vector()
        }
    }
    fn similarity(&self, x @ Address(a0): Address, y @ Address(a1): Address) -> f32 {
        if a0 == self.no_nodes {
            let y = self.find_node(y).vector();
            (self.similarity_function)(self.temp, y)
        } else if a1 == self.no_nodes {
            let x = self.find_node(x).vector();
            (self.similarity_function)(self.temp, x)
        } else {
            let x = self.find_node(x).vector();
            let y = self.find_node(y).vector();
            (self.similarity_function)(x, y)
        }
    }

    fn min_score(&self) -> f32 {
        self.min_score
    }
}

#[derive(Clone, Debug)]
pub struct LabelDictionary(pub Vec<u8>);
impl Default for LabelDictionary {
    fn default() -> Self {
        LabelDictionary::new(vec![])
    }
}
impl LabelDictionary {
    pub fn new(mut labels: Vec<String>) -> LabelDictionary {
        labels.sort();
        let ram_trie = trie_ram::create_trie(&labels);
        LabelDictionary(trie::serialize(ram_trie))
    }
}
#[derive(Clone, Debug)]
pub struct Elem {
    pub key: Vec<u8>,
    pub vector: Vec<f32>,
    pub metadata: Option<Vec<u8>>,
    pub labels: LabelDictionary,
}
impl Elem {
    pub fn new(key: String, vector: Vec<f32>, labels: LabelDictionary, metadata: Option<Vec<u8>>) -> Elem {
        Elem {
            labels,
            metadata,
            key: key.as_bytes().to_vec(),
            vector,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Neighbour<'a> {
    score: f32,
    node: Node<'a>,
}
impl Eq for Neighbour<'_> {}
impl std::hash::Hash for Neighbour<'_> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id().hash(state)
    }

    fn hash_slice<H: std::hash::Hasher>(data: &[Self], state: &mut H)
    where
        Self: Sized,
    {
        for piece in data {
            piece.hash(state)
        }
    }
}
impl Ord for Neighbour<'_> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.node.cmp(&other.node)
    }
}
impl PartialOrd for Neighbour<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for Neighbour<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.node == other.node
    }
}

impl Neighbour<'_> {
    fn new(Address(addr): Address, data_store: &DataStore, score: f32) -> Neighbour {
        let node = data_store.get_value(addr);
        Neighbour { score, node }
    }
    pub fn score(&self) -> f32 {
        self.score
    }
    pub fn id(&self) -> &[u8] {
        Node::key(&self.node)
    }
    pub fn vector(&self) -> &[u8] {
        Node::vector(&self.node)
    }
    pub fn labels(&self) -> Vec<String> {
        Node::labels(&self.node)
    }
    pub fn metadata(&self) -> Option<&[u8]> {
        let metadata = Node::metadata(&self.node);
        (!metadata.is_empty()).then_some(metadata)
    }
}

pub struct OpenDataPoint {
    metadata: VectorSegmentMetadata,
    data_store: DataStore,
    index: Mmap,
    inverted_indexes: InvertedIndexes,
    alive_bitset: BitSet,
}

impl AsRef<OpenDataPoint> for OpenDataPoint {
    fn as_ref(&self) -> &OpenDataPoint {
        self
    }
}

impl OpenDataPoint {
    pub fn apply_deletion(&mut self, key: &str) {
        if let Some(deleted_ids) = self.inverted_indexes.ids_for_deletion_key(key) {
            for id in deleted_ids {
                self.alive_bitset.remove(id as usize);
            }
        }
    }

    pub fn into_metadata(self) -> VectorSegmentMetadata {
        self.metadata
    }

    pub fn tags(&self) -> &HashSet<String> {
        &self.metadata.index_metadata.tags
    }

    pub fn alive_nodes(&self) -> impl Iterator<Item = usize> + '_ {
        self.alive_bitset.iter()
    }

    pub fn space_usage(&self) -> usize {
        self.data_store.size_bytes() + self.index.len() + self.inverted_indexes.space_usage()
    }

    pub fn search(
        &self,
        query: &[f32],
        filter: &Formula,
        with_duplicates: bool,
        results: usize,
        config: &VectorConfig,
        min_score: f32,
    ) -> Box<dyn Iterator<Item = Neighbour> + '_> {
        let encoded_query = config.vector_type.encode(query);
        let retriever = Retriever::new(&encoded_query, &self.data_store, config, min_score);
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
            for address in bitset.iter() {
                let address = Address(address);
                let score = retriever.similarity(query_address, address);
                if score >= min_score {
                    scored_results.push(Reverse(Cnx(address, score)));
                }
            }
            scored_results.sort();
            return Box::new(
                scored_results
                    .into_iter()
                    .map(|Reverse(a)| Neighbour::new(a.0, &self.data_store, a.1))
                    .take(results),
            );
        }

        let ops = HnswOps::new(&retriever, true);
        let neighbours = ops.search(query_address, self.index.as_ref(), results, bitset, with_duplicates);
        Box::new(
            neighbours
                .into_iter()
                .map(|(address, dist)| (Neighbour::new(address, &self.data_store, dist)))
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
        config::{Similarity, VectorConfig},
        formula::Formula,
        vector_types::dense_f32::{dot_similarity, encode_vector},
    };

    use super::{Elem, LabelDictionary, create, merge};
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
                LabelDictionary::new(labels.clone()),
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
        };
        let mut rng = SmallRng::seed_from_u64(1234567890);
        let temp_dir = tempdir()?;

        // Create a data point with random data of different length
        let path = temp_dir.path();
        let elems = (0..100).map(|_| random_elem(&mut rng)).collect::<Vec<_>>();
        let dp = create(
            path,
            elems.iter().cloned().map(|x| x.0).collect(),
            &config,
            HashSet::new(),
        )?;

        for (i, (elem, mut labels)) in elems.into_iter().enumerate() {
            let node = dp.data_store.get_value(i);
            assert_eq!(elem.key, node.key());
            assert_eq!(config.vector_type.encode(&elem.vector), node.vector());

            // Compare metadata as the decoded protobug. Tthe absolute stored value may have trailing padding
            // from vectors, but the decoding step should ignore it
            assert_eq!(
                SentenceMetadata::decode(elem.metadata.as_ref().unwrap().as_slice()),
                SentenceMetadata::decode(node.metadata())
            );

            // Compare labels
            labels.sort();
            let mut node_labels = node.labels();
            node_labels.sort();
            assert_eq!(labels, node_labels);
        }

        Ok(())
    }

    #[test]
    fn test_save_recall_aligned_data_after_merge() -> anyhow::Result<()> {
        let config = VectorConfig {
            similarity: Similarity::Dot,
            vector_type: crate::config::VectorType::DenseF32 { dimension: DIMENSION },
            normalize_vectors: false,
        };
        let mut rng = SmallRng::seed_from_u64(1234567890);

        // Create two data points with random data of different length
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
            let node = merged_dp.data_store.get_value(i);
            assert_eq!(elem.key, node.key());
            assert_eq!(config.vector_type.encode(&elem.vector), node.vector());

            // Compare metadata as the decoded protobug. Tthe absolute stored value may have trailing padding
            // from vectors, but the decoding step should ignore it
            assert_eq!(
                SentenceMetadata::decode(elem.metadata.as_ref().unwrap().as_slice()),
                SentenceMetadata::decode(node.metadata())
            );

            // Compare labels
            labels.sort();
            let mut node_labels = node.labels();
            node_labels.sort();
            assert_eq!(labels, node_labels);
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
        };

        // Create a data point
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
                // Search near an existing datapoint (simulates that the query is related to the data)
                let base_v = elems.values().nth(rng.gen_range(0..elems.len())).unwrap();
                let query = random_nearby_vector(&mut rng, base_v, 0.05);

                let mut similarities: Vec<_> = elems.iter().map(|(k, v)| (k, similarity(v, &query))).collect();
                similarities.sort_unstable_by(|a, b| a.1.total_cmp(&b.1).reverse());

                let results: Vec<_> = dp.search(&query, &Formula::new(), false, 5, &config, 0.0).collect();

                let search: Vec<_> = results
                    .iter()
                    .map(|r| String::from_utf8(r.id().to_vec()).unwrap())
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
