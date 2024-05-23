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
use crate::VectorR;
use data_store::Interpreter;
use disk_hnsw::DiskHnsw;
use fs::{File, OpenOptions};
use fs2::FileExt;
use io::{BufWriter, ErrorKind, Write};
use memmap2::Mmap;
use node::Node;
use ops_hnsw::HnswOps;
use ram_hnsw::RAMHnsw;
use serde::{Deserialize, Serialize};
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use std::{fs, io};

pub use ops_hnsw::DataRetriever;
pub use uuid::Uuid as DpId;

mod file_names {
    pub const NODES: &str = "nodes.kv";
    pub const HNSW: &str = "index.hnsw";
    pub const JOURNAL: &str = "journal.json";
    pub const DATA_POINT_PIN: &str = ".pin";
}

pub struct DataPointPin {
    data_point_id: DpId,
    data_point_path: PathBuf,
    journal_path: PathBuf,
    #[allow(unused)]
    pin: File,
}
impl DataPointPin {
    pub fn id(&self) -> DpId {
        self.data_point_id
    }

    pub fn path(&self) -> &Path {
        &self.data_point_path
    }

    pub fn is_pinned(path: &Path, id: DpId) -> io::Result<bool> {
        let data_point_path = path.join(id.to_string());
        let pin_path = data_point_path.join(file_names::DATA_POINT_PIN);

        if !pin_path.is_file() {
            return Ok(false);
        }

        let pin_file = File::open(pin_path)?;

        let Err(error) = pin_file.try_lock_exclusive() else {
            return Ok(false);
        };

        if let ErrorKind::WouldBlock = error.kind() {
            Ok(true)
        } else {
            Err(error)
        }
    }

    pub fn read_journal(&self) -> io::Result<Journal> {
        let journal = File::open(&self.journal_path)?;
        let journal: Journal = serde_json::from_reader(BufReader::new(journal))?;
        Ok(journal)
    }

    pub fn create_pin(dir: &Path) -> io::Result<DataPointPin> {
        let id = DpId::new_v4();
        let folder_name = id.to_string();
        let temp_dir = format!("{folder_name}.tmp");
        let data_point_path = dir.join(folder_name);
        let temp_data_point_path = dir.join(temp_dir);
        let journal_path = data_point_path.join(file_names::JOURNAL);

        std::fs::create_dir(&temp_data_point_path)?;

        let temp_pin_path = temp_data_point_path.join(file_names::DATA_POINT_PIN);
        let pin_file = File::create(temp_pin_path)?;
        pin_file.lock_shared()?;

        std::fs::rename(&temp_data_point_path, &data_point_path)?;

        Ok(DataPointPin {
            journal_path,
            data_point_path,
            pin: pin_file,
            data_point_id: id,
        })
    }

    pub fn open_pin(dir: &Path, id: DpId) -> io::Result<DataPointPin> {
        let data_point_path = dir.join(id.to_string());
        let pin_path = data_point_path.join(file_names::DATA_POINT_PIN);
        let journal_path = data_point_path.join(file_names::JOURNAL);
        let pin_file = File::create(pin_path)?;

        pin_file.lock_shared()?;

        Ok(DataPointPin {
            journal_path,
            data_point_path,
            pin: pin_file,
            data_point_id: id,
        })
    }
}

pub fn delete(dir: &Path, uid: DpId) -> VectorR<()> {
    let uid = uid.to_string();
    let id = dir.join(uid);
    fs::remove_dir_all(id)?;
    Ok(())
}

pub fn open(pin: &DataPointPin) -> VectorR<OpenDataPoint> {
    let data_point_path = &pin.data_point_path;
    let nodes_file = File::open(data_point_path.join(file_names::NODES))?;
    let journal_file = File::open(data_point_path.join(file_names::JOURNAL))?;
    let hnsw_file = File::open(data_point_path.join(file_names::HNSW))?;

    let nodes = unsafe { Mmap::map(&nodes_file)? };
    let index = unsafe { Mmap::map(&hnsw_file)? };
    let journal: Journal = serde_json::from_reader(BufReader::new(journal_file))?;

    // Telling the OS our expected access pattern
    #[cfg(not(target_os = "windows"))]
    {
        nodes.advise(memmap2::Advice::WillNeed)?;
        index.advise(memmap2::Advice::Sequential)?;
    }

    Ok(OpenDataPoint {
        journal,
        nodes,
        index,
    })
}

pub fn merge<Dlog>(
    pin: &DataPointPin,
    operants: &[(Dlog, &OpenDataPoint)],
    config: &VectorConfig,
    merge_time: SystemTime,
) -> VectorR<OpenDataPoint>
where
    Dlog: DeleteLog,
{
    let data_point_id = pin.data_point_id;
    let data_point_path = &pin.data_point_path;

    let nodes_path = data_point_path.join(file_names::NODES);
    let mut nodes_file_options = OpenOptions::new();
    nodes_file_options.read(true);
    nodes_file_options.write(true);
    nodes_file_options.create(true);
    let mut nodes_file = nodes_file_options.open(nodes_path)?;

    let journal_path = data_point_path.join(file_names::JOURNAL);
    let mut journal_file_options = OpenOptions::new();
    journal_file_options.read(true);
    journal_file_options.write(true);
    journal_file_options.create(true);
    let mut journal_file = journal_file_options.open(journal_path)?;

    let hnsw_path = data_point_path.join(file_names::HNSW);
    let mut hnsw_file_options = OpenOptions::new();
    hnsw_file_options.read(true);
    hnsw_file_options.write(true);
    hnsw_file_options.create(true);
    let mut hnsw_file = hnsw_file_options.open(hnsw_path)?;

    // Sort largest operant first so we reuse as much of the HNSW as possible
    let mut operants = operants.iter().collect::<Vec<_>>();
    operants.sort_unstable_by_key(|o| std::cmp::Reverse(o.1.journal().no_nodes()));

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

    let journal = Journal {
        nodes: no_nodes,
        uid: data_point_id,
        ctime: merge_time,
    };

    {
        let mut journalf_buffer = BufWriter::new(&mut journal_file);
        journalf_buffer.write_all(&serde_json::to_vec(&journal)?)?;
        journalf_buffer.flush()?;
    }

    // Telling the OS our expected access pattern
    #[cfg(not(target_os = "windows"))]
    {
        nodes.advise(memmap2::Advice::WillNeed)?;
        index.advise(memmap2::Advice::Sequential)?;
    }

    Ok(OpenDataPoint {
        journal,
        nodes,
        index,
    })
}

pub fn create(
    pin: &DataPointPin,
    elems: Vec<Elem>,
    time: Option<SystemTime>,
    config: &VectorConfig,
) -> VectorR<OpenDataPoint> {
    // Check dimensions
    if let Some(dim) = config.vector_type.dimension() {
        if elems.iter().any(|elem| elem.vector.len() != dim) {
            return Err(crate::VectorErr::InconsistentDimensions);
        }
    }

    let data_point_id = pin.data_point_id;
    let data_point_path = &pin.data_point_path;
    let mut nodes_file_options = OpenOptions::new();
    nodes_file_options.read(true);
    nodes_file_options.write(true);
    nodes_file_options.create(true);
    let mut nodesf = nodes_file_options.open(data_point_path.join(file_names::NODES))?;

    let mut journal_file_options = OpenOptions::new();
    journal_file_options.read(true);
    journal_file_options.write(true);
    journal_file_options.create(true);
    let mut journalf = journal_file_options.open(data_point_path.join(file_names::JOURNAL))?;

    let mut hnsw_file_options = OpenOptions::new();
    hnsw_file_options.read(true);
    hnsw_file_options.write(true);
    hnsw_file_options.create(true);
    let mut hnswf = hnsw_file_options.open(data_point_path.join(file_names::HNSW))?;

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

    let journal = Journal {
        nodes: no_nodes,
        uid: data_point_id,
        ctime: time.unwrap_or_else(SystemTime::now),
    };
    {
        // Saving the journal
        let mut journalf_buffer = BufWriter::new(&mut journalf);
        journalf_buffer.write_all(&serde_json::to_vec(&journal)?)?;
        journalf_buffer.flush()?;
    }

    // Telling the OS our expected access pattern
    #[cfg(not(target_os = "windows"))]
    {
        nodes.advise(memmap2::Advice::WillNeed)?;
        index.advise(memmap2::Advice::Sequential)?;
    }

    Ok(OpenDataPoint {
        journal,
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

#[derive(Clone, Copy, Serialize, Deserialize, Debug)]
pub struct Journal {
    uid: DpId,
    nodes: usize,
    ctime: SystemTime,
}
impl Journal {
    pub fn id(&self) -> DpId {
        self.uid
    }
    pub fn no_nodes(&self) -> usize {
        self.nodes
    }
    pub fn time(&self) -> SystemTime {
        self.ctime
    }
    pub fn update_time(&mut self, time: SystemTime) {
        self.ctime = time;
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default)]
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
    #[cfg(test)]
    pub fn dummy_neighbour(key: &[u8], score: f32) -> Neighbour {
        Neighbour {
            score,
            node: Node::serialize(key, [1, 2, 3, 4], 1, [], None as Option<&[u8]>),
        }
    }
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
        metadata.is_empty().then_some(metadata)
    }
}

pub struct OpenDataPoint {
    journal: Journal,
    nodes: Mmap,
    index: Mmap,
}

impl AsRef<OpenDataPoint> for OpenDataPoint {
    fn as_ref(&self) -> &OpenDataPoint {
        self
    }
}

impl OpenDataPoint {
    pub fn stored_len(&self) -> Option<usize> {
        if data_store::stored_elements(&self.nodes) == 0 {
            return None;
        }
        let node = data_store::get_value(Node, &self.nodes, 0);
        Some(dense_f32_unaligned::vector_len(Node::vector(node)) as usize)
    }
    pub fn get_id(&self) -> DpId {
        self.journal.uid
    }
    pub fn journal(&self) -> Journal {
        self.journal
    }
    pub fn get_keys<Dlog: DeleteLog>(&self, delete_log: &Dlog) -> Vec<String> {
        let node_storage = &self.nodes;
        let length = data_store::stored_elements(node_storage);
        let data_iterator = (0..length).map(|i| data_store::get_value(Node, node_storage, i));
        let mut keys = Vec::new();
        for data in data_iterator {
            let raw_key = Node.get_key(data);
            if !delete_log.is_deleted(raw_key) {
                let string_key = String::from_utf8_lossy(raw_key).to_string();
                keys.push(string_key);
            }
        }
        keys
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
        let neighbours = ops.search(Address(self.journal.nodes), self.index.as_ref(), results, filter, with_duplicates);

        neighbours.into_iter().map(|(address, dist)| (Neighbour::new(address, &self.nodes, dist))).take(results)
    }
}

#[cfg(test)]
mod test {
    use std::{collections::BTreeMap, time::SystemTime};

    use nucliadb_core::{
        protos::{Position, Representation, SentenceMetadata},
        NodeResult,
    };
    use rand::{rngs::SmallRng, Rng, SeedableRng};
    use tempfile::tempdir;

    use crate::{
        config::{Similarity, VectorConfig},
        data_types::data_store,
        vector_types::dense_f32_unaligned::{dot_similarity, encode_vector},
    };
    use nucliadb_core::protos::prost::Message;

    use super::{create, merge, node::Node, DataPointPin, Elem, LabelDictionary, NoDLog};

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
    fn test_save_recall_aligned_data() -> NodeResult<()> {
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
        let pin = DataPointPin::create_pin(temp_dir.path())?;
        let elems = (0..100).map(|_| random_elem(&mut rng)).collect::<Vec<_>>();
        let dp = create(&pin, elems.iter().cloned().map(|x| x.0).collect(), None, &config)?;
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
    fn test_save_recall_aligned_data_after_merge() -> NodeResult<()> {
        let config = VectorConfig {
            similarity: Similarity::Dot,
            vector_type: crate::config::VectorType::DenseF32 {
                dimension: DIMENSION,
            },
            normalize_vectors: false,
        };
        let mut rng = SmallRng::seed_from_u64(1234567890);
        let temp_dir = tempdir()?;

        // Create two data points with random data of different length
        let pin1 = DataPointPin::create_pin(temp_dir.path())?;
        let elems1 = (0..10).map(|_| random_elem(&mut rng)).collect::<Vec<_>>();
        let dp1 = create(&pin1, elems1.iter().cloned().map(|x| x.0).collect(), None, &config)?;

        let pin2 = DataPointPin::create_pin(temp_dir.path())?;
        let elems2 = (0..10).map(|_| random_elem(&mut rng)).collect::<Vec<_>>();
        let dp2 = create(&pin2, elems2.iter().cloned().map(|x| x.0).collect(), None, &config)?;

        let pin_merged = DataPointPin::create_pin(temp_dir.path())?;
        let merged_dp = merge(&pin_merged, &[(NoDLog, &dp1), (NoDLog, &dp2)], &config, SystemTime::now())?;
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
    fn test_recall_clustered_data() -> NodeResult<()> {
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
        let pin = DataPointPin::create_pin(temp_dir.path())?;
        let dp = create(
            &pin,
            elems.iter().map(|(k, v)| Elem::new(k.clone(), v.clone(), Default::default(), None)).collect(),
            None,
            &config,
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
