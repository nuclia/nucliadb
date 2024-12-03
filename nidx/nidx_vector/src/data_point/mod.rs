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
pub mod node;
pub mod ops_hnsw;
pub mod ram_hnsw;

mod params;
#[cfg(test)]
mod tests;

use crate::config::{VectorConfig, VectorType};
use crate::data_types::{data_store, trie, trie_ram, DeleteLog};
use crate::formula::Formula;
use crate::vector_types::dense_f32_unaligned;
use crate::{VectorR, VectorSegmentMeta, VectorSegmentMetadata};
use data_store::Interpreter;
use disk_hnsw::DiskHnsw;
use fs::{File, OpenOptions};
use io::{BufWriter, Write};
use memmap2::Mmap;
use node::Node;
use ops_hnsw::HnswOps;
use ram_hnsw::RAMHnsw;
use std::collections::HashSet;
use std::path::Path;
use std::{fs, io};

pub use ops_hnsw::DataRetriever;

mod file_names {
    pub const NODES: &str = "nodes.kv";
    pub const HNSW: &str = "index.hnsw";
}

pub fn open(metadata: VectorSegmentMetadata) -> VectorR<OpenDataPoint> {
    let path = &metadata.path;
    let nodes_file = File::open(path.join(file_names::NODES))?;
    let hnsw_file = File::open(path.join(file_names::HNSW))?;

    let nodes = unsafe { Mmap::map(&nodes_file)? };
    let index = unsafe { Mmap::map(&hnsw_file)? };

    // Telling the OS our expected access pattern
    #[cfg(not(target_os = "windows"))]
    {
        nodes.advise(memmap2::Advice::WillNeed)?;
        index.advise(memmap2::Advice::Sequential)?;
    }

    Ok(OpenDataPoint {
        metadata,
        nodes,
        index,
    })
}

pub fn merge<Dlog>(
    data_point_path: &Path,
    operants: &[(Dlog, &OpenDataPoint)],
    config: &VectorConfig,
) -> VectorR<OpenDataPoint>
where
    Dlog: DeleteLog,
{
    let nodes_path = data_point_path.join(file_names::NODES);
    let mut nodes_file_options = OpenOptions::new();
    nodes_file_options.read(true);
    nodes_file_options.write(true);
    nodes_file_options.create(true);
    let mut nodes_file = nodes_file_options.open(nodes_path)?;

    let hnsw_path = data_point_path.join(file_names::HNSW);
    let mut hnsw_file_options = OpenOptions::new();
    hnsw_file_options.read(true);
    hnsw_file_options.write(true);
    hnsw_file_options.create(true);
    let mut hnsw_file = hnsw_file_options.open(hnsw_path)?;

    // Sort largest operant first so we reuse as much of the HNSW as possible
    let mut operants = operants.iter().collect::<Vec<_>>();
    operants.sort_unstable_by_key(|o| std::cmp::Reverse(o.1.metadata.records));

    // Tags for all segments are the same (this should not happen, prepare_merge ensures it)
    let tags = &operants[0].1.metadata.index_metadata.tags;
    for (_, dp) in &operants {
        if &dp.metadata.index_metadata.tags != tags {
            return Err(crate::VectorErr::InconsistentMergeSegmentTags);
        }
    }

    // Creating the node store
    let node_producers: Vec<_> = operants.iter().map(|dp| ((&dp.0, Node), dp.1.nodes.as_ref())).collect();
    let has_deletions = data_store::merge(&mut nodes_file, &node_producers, config)?;
    let nodes = unsafe { Mmap::map(&nodes_file)? };
    let no_nodes = data_store::stored_elements(&nodes);

    let mut index;
    let start_node_index;
    if has_deletions {
        index = RAMHnsw::new();
        start_node_index = 0;
    } else {
        // If there are no deletions, we can reuse the first segment
        // HNSW since its indexes will match the the ones in data_store
        index = DiskHnsw::deserialize(&operants[0].1.index);
        start_node_index = data_store::stored_elements(&operants[0].1.nodes);
    }

    // Creating the hnsw for the new node store.
    let tracker = Retriever::new(&[], &nodes, &NoDLog, config, -1.0);
    let mut ops = HnswOps::new(&tracker);
    for id in start_node_index..no_nodes {
        ops.insert(Address(id), &mut index)
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
        nodes.advise(memmap2::Advice::WillNeed)?;
        index.advise(memmap2::Advice::Sequential)?;
    }

    let metadata = VectorSegmentMetadata {
        path: data_point_path.to_path_buf(),
        records: no_nodes,
        index_metadata: VectorSegmentMeta {
            tags: tags.clone(),
        },
    };

    Ok(OpenDataPoint {
        metadata,
        nodes,
        index,
    })
}

pub fn create(path: &Path, elems: Vec<Elem>, config: &VectorConfig, tags: HashSet<String>) -> VectorR<OpenDataPoint> {
    // Check dimensions
    if let Some(dim) = config.vector_type.dimension() {
        if elems.iter().any(|elem| elem.vector.len() != dim) {
            return Err(crate::VectorErr::InconsistentDimensions);
        }
    }

    let mut nodes_file_options = OpenOptions::new();
    nodes_file_options.read(true);
    nodes_file_options.write(true);
    nodes_file_options.create(true);
    let mut nodesf = nodes_file_options.open(path.join(file_names::NODES))?;

    let mut hnsw_file_options = OpenOptions::new();
    hnsw_file_options.read(true);
    hnsw_file_options.write(true);
    hnsw_file_options.create(true);
    let mut hnswf = hnsw_file_options.open(path.join(file_names::HNSW))?;

    // Serializing nodes on disk
    // Nodes are stored on disk and mmaped.
    data_store::create_key_value(&mut nodesf, elems, &config.vector_type)?;
    let nodes = unsafe { Mmap::map(&nodesf)? };
    let no_nodes = data_store::stored_elements(&nodes);

    // Creating the HNSW using the mmaped nodes
    let mut index = RAMHnsw::new();
    let tracker = Retriever::new(&[], &nodes, &NoDLog, config, -1.0);
    let mut ops = HnswOps::new(&tracker);
    for id in 0..no_nodes {
        ops.insert(Address(id), &mut index)
    }

    {
        // The HNSW is on RAM
        // Serializing the HNSW into disk
        let mut hnswf_buffer = BufWriter::new(&mut hnswf);
        DiskHnsw::serialize_into(&mut hnswf_buffer, no_nodes, index)?;
        hnswf_buffer.flush()?;
    }
    let index = unsafe { Mmap::map(&hnswf)? };

    // Telling the OS our expected access pattern
    #[cfg(not(target_os = "windows"))]
    {
        nodes.advise(memmap2::Advice::WillNeed)?;
        index.advise(memmap2::Advice::Sequential)?;
    }

    let metadata = VectorSegmentMetadata {
        path: path.to_path_buf(),
        records: no_nodes,
        index_metadata: VectorSegmentMeta {
            tags,
        },
    };

    Ok(OpenDataPoint {
        metadata,
        nodes,
        index,
    })
}

pub struct NoDLog;
impl DeleteLog for NoDLog {
    fn is_deleted(&self, _: &[u8]) -> bool {
        false
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Address(usize);
impl Address {
    #[cfg(test)]
    pub const fn dummy() -> Address {
        Address(0)
    }
}

pub struct FormulaFilter<'a> {
    filter: &'a Formula,
}

impl FormulaFilter<'_> {
    pub fn new(filter: &Formula) -> FormulaFilter {
        FormulaFilter {
            filter,
        }
    }
    pub fn run<DR: DataRetriever>(&self, address: Address, tracker: &DR) -> bool {
        self.filter.run(address, tracker)
    }
}

pub struct Retriever<'a, Dlog> {
    similarity_function: fn(&[u8], &[u8]) -> f32,
    no_nodes: usize,
    temp: &'a [u8],
    nodes: &'a Mmap,
    delete_log: &'a Dlog,
    min_score: f32,
    vector_len_bytes: Option<usize>,
}
impl<'a, Dlog: DeleteLog> Retriever<'a, Dlog> {
    pub fn new(
        temp: &'a [u8],
        nodes: &'a Mmap,
        delete_log: &'a Dlog,
        config: &VectorConfig,
        min_score: f32,
    ) -> Retriever<'a, Dlog> {
        let no_nodes = data_store::stored_elements(nodes);
        let vector_len_bytes = if config.known_dimensions() {
            config.vector_len_bytes()
        } else if data_store::stored_elements(nodes) > 0 {
            let node = data_store::get_value(Node, nodes, 0);
            Some(dense_f32_unaligned::vector_len(Node::vector(node)) as usize)
        } else {
            None
        };
        Retriever {
            temp,
            nodes,
            delete_log,
            similarity_function: config.similarity_function(),
            no_nodes,
            min_score,
            vector_len_bytes,
        }
    }
    fn find_node(&self, Address(x): Address) -> &[u8] {
        if x == self.no_nodes {
            self.temp
        } else {
            data_store::get_value(Node, self.nodes, x)
        }
    }
}

impl<'a, Dlog: DeleteLog> DataRetriever for Retriever<'a, Dlog> {
    fn get_key(&self, x @ Address(addr): Address) -> &[u8] {
        if addr == self.no_nodes {
            &[]
        } else {
            let x = self.find_node(x);
            Node::key(x)
        }
    }

    fn will_need(&self, Address(x): Address) {
        if let Some(len) = self.vector_len_bytes {
            data_store::will_need(self.nodes, x, len);
        }
    }

    fn get_vector(&self, x @ Address(addr): Address) -> &[u8] {
        if addr == self.no_nodes {
            self.temp
        } else {
            let x = self.find_node(x);
            Node::vector(x)
        }
    }
    fn is_deleted(&self, x @ Address(addr): Address) -> bool {
        if addr == self.no_nodes {
            false
        } else {
            let x = self.find_node(x);
            let key = Node::key(x);
            self.delete_log.is_deleted(key)
        }
    }
    fn has_label(&self, Address(x): Address, label: &[u8]) -> bool {
        if x == self.no_nodes {
            false
        } else {
            let x = data_store::get_value(Node, self.nodes, x);
            Node::has_label(x, label)
        }
    }
    fn similarity(&self, x @ Address(a0): Address, y @ Address(a1): Address) -> f32 {
        if a0 == self.no_nodes {
            let y = self.find_node(y);
            let y = Node::vector(y);
            (self.similarity_function)(self.temp, y)
        } else if a1 == self.no_nodes {
            let x = self.find_node(x);
            let x = Node::vector(x);
            (self.similarity_function)(self.temp, x)
        } else {
            let x = self.find_node(x);
            let y = self.find_node(y);
            let x = Node::vector(x);
            let y = Node::vector(y);
            (self.similarity_function)(x, y)
        }
    }

    fn min_score(&self) -> f32 {
        self.min_score
    }
}

#[derive(Clone, Debug)]
pub struct LabelDictionary(Vec<u8>);
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

impl data_store::IntoBuffer for Elem {
    fn serialize_into<W: io::Write>(self, w: W, vector_type: &VectorType) -> io::Result<()> {
        Node::serialize_into(
            w,
            self.key,
            vector_type.encode(&self.vector),
            vector_type.vector_alignment(),
            self.labels.0,
            self.metadata.as_ref(),
        )
    }
}

#[derive(Debug, Clone)]
pub struct Neighbour {
    score: f32,
    node: Vec<u8>,
}
impl Eq for Neighbour {}
impl std::hash::Hash for Neighbour {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id().hash(state)
    }
}
impl Ord for Neighbour {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.node.cmp(&other.node)
    }
}
impl PartialOrd for Neighbour {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for Neighbour {
    fn eq(&self, other: &Self) -> bool {
        self.node == other.node
    }
}

impl Neighbour {
    fn new(Address(addr): Address, data: &[u8], score: f32) -> Neighbour {
        let node = data_store::get_value(Node, data, addr);
        let (exact, _) = Node.read_exact(node);
        Neighbour {
            score,
            node: exact.to_vec(),
        }
    }
    pub fn score(&self) -> f32 {
        self.score
    }
    pub fn id(&self) -> &[u8] {
        Node.get_key(&self.node)
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
    nodes: Mmap,
    index: Mmap,
}

impl AsRef<OpenDataPoint> for OpenDataPoint {
    fn as_ref(&self) -> &OpenDataPoint {
        self
    }
}

impl OpenDataPoint {
    pub fn into_metadata(self) -> VectorSegmentMetadata {
        self.metadata
    }

    pub fn no_nodes(&self) -> usize {
        self.metadata.records
    }

    pub fn tags(&self) -> &HashSet<String> {
        &self.metadata.index_metadata.tags
    }

    pub fn stored_len(&self) -> Option<usize> {
        if data_store::stored_elements(&self.nodes) == 0 {
            return None;
        }
        let node = data_store::get_value(Node, &self.nodes, 0);
        Some(dense_f32_unaligned::vector_len(Node::vector(node)) as usize)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn search<Dlog: DeleteLog>(
        &self,
        delete_log: &Dlog,
        query: &[f32],
        filter: &Formula,
        with_duplicates: bool,
        results: usize,
        config: &VectorConfig,
        min_score: f32,
    ) -> impl Iterator<Item = Neighbour> + '_ {
        let encoded_query = config.vector_type.encode(query);
        let tracker = Retriever::new(&encoded_query, &self.nodes, delete_log, config, min_score);
        let filter = FormulaFilter::new(filter);
        let ops = HnswOps::new(&tracker);
        let neighbours =
            ops.search(Address(self.metadata.records), self.index.as_ref(), results, filter, with_duplicates);

        neighbours.into_iter().map(|(address, dist)| (Neighbour::new(address, &self.nodes, dist))).take(results)
    }
}

#[cfg(test)]
mod test {
    use std::collections::{BTreeMap, HashSet};

    use nidx_protos::{Position, Representation, SentenceMetadata};
    use rand::{rngs::SmallRng, Rng, SeedableRng};
    use tempfile::tempdir;

    use crate::{
        config::{Similarity, VectorConfig},
        data_types::data_store,
        vector_types::dense_f32_unaligned::{dot_similarity, encode_vector},
    };

    use super::{create, merge, node::Node, Elem, LabelDictionary, NoDLog};
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
        let v = close_to.iter().zip(fuzz.iter()).map(|(v, fuzz)| v + fuzz * distance).collect();
        normalize(v)
    }

    fn random_key(rng: &mut impl Rng) -> String {
        format!("{:032x?}", rng.gen::<u128>())
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
            vector_type: crate::config::VectorType::DenseF32 {
                dimension: DIMENSION,
            },
            normalize_vectors: false,
        };
        let mut rng = SmallRng::seed_from_u64(1234567890);
        let temp_dir = tempdir()?;

        // Create a data point with random data of different length
        let path = temp_dir.path();
        let elems = (0..100).map(|_| random_elem(&mut rng)).collect::<Vec<_>>();
        let dp = create(path, elems.iter().cloned().map(|x| x.0).collect(), &config, HashSet::new())?;
        let nodes = dp.nodes;

        for (i, (elem, mut labels)) in elems.into_iter().enumerate() {
            let node = data_store::get_value(Node, &nodes, i);
            assert_eq!(elem.key, Node::key(node));
            assert_eq!(config.vector_type.encode(&elem.vector), Node::vector(node));

            // Compare metadata as the decoded protobug. Tthe absolute stored value may have trailing padding
            // from vectors, but the decoding step should ignore it
            assert_eq!(
                SentenceMetadata::decode(elem.metadata.as_ref().unwrap().as_slice()),
                SentenceMetadata::decode(Node::metadata(node))
            );

            // Compare labels
            labels.sort();
            let mut node_labels = Node::labels(node);
            node_labels.sort();
            assert_eq!(labels, node_labels);
        }

        Ok(())
    }

    #[test]
    fn test_save_recall_aligned_data_after_merge() -> anyhow::Result<()> {
        let config = VectorConfig {
            similarity: Similarity::Dot,
            vector_type: crate::config::VectorType::DenseF32 {
                dimension: DIMENSION,
            },
            normalize_vectors: false,
        };
        let mut rng = SmallRng::seed_from_u64(1234567890);

        // Create two data points with random data of different length
        let path1 = tempdir()?;
        let elems1 = (0..10).map(|_| random_elem(&mut rng)).collect::<Vec<_>>();
        let dp1 = create(path1.path(), elems1.iter().cloned().map(|x| x.0).collect(), &config, HashSet::new())?;

        let path2 = tempdir()?;
        let elems2 = (0..10).map(|_| random_elem(&mut rng)).collect::<Vec<_>>();
        let dp2 = create(path2.path(), elems2.iter().cloned().map(|x| x.0).collect(), &config, HashSet::new())?;

        let path_merged = tempdir()?;
        let merged_dp = merge(path_merged.path(), &[(NoDLog, &dp1), (NoDLog, &dp2)], &config)?;
        let nodes = merged_dp.nodes;

        for (i, (elem, mut labels)) in elems1.into_iter().chain(elems2.into_iter()).enumerate() {
            let node = data_store::get_value(Node, &nodes, i);
            assert_eq!(elem.key, Node::key(node));
            assert_eq!(config.vector_type.encode(&elem.vector), Node::vector(node));

            // Compare metadata as the decoded protobug. Tthe absolute stored value may have trailing padding
            // from vectors, but the decoding step should ignore it
            assert_eq!(
                SentenceMetadata::decode(elem.metadata.as_ref().unwrap().as_slice()),
                SentenceMetadata::decode(Node::metadata(node))
            );

            // Compare labels
            labels.sort();
            let mut node_labels = Node::labels(node);
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
            vector_type: crate::config::VectorType::DenseF32 {
                dimension: DIMENSION,
            },
            normalize_vectors: false,
        };

        // Create a data point
        let temp_dir = tempdir()?;
        let dp = create(
            temp_dir.path(),
            elems.iter().map(|(k, v)| Elem::new(k.clone(), v.clone(), Default::default(), None)).collect(),
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

                let results: Vec<_> = dp.search(&NoDLog, &query, &Default::default(), false, 5, &config, 0.0).collect();

                let search: Vec<_> = results.iter().map(|r| String::from_utf8(r.id().to_vec()).unwrap()).collect();
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
