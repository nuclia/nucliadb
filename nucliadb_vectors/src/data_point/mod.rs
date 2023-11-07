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
mod params;
pub mod ram_hnsw;
#[cfg(test)]
mod tests;
use std::collections::{HashMap, HashSet};
use std::time::SystemTime;
use std::{fs, io, path};

use disk_hnsw::DiskHnsw;
use io::{BufWriter, Write};
use key_value::Slot;
use memmap2::Mmap;
use node::Node;
use nucliadb_core::tracing::{debug, error};
use nucliadb_core::Channel;
pub use ops_hnsw::DataRetriever;
use ops_hnsw::HnswOps;
use ram_hnsw::RAMHnsw;
use serde::{Deserialize, Serialize};
pub use uuid::Uuid as DpId;

use crate::data_types::{key_value, trie, trie_ram, vector, DeleteLog};
use crate::formula::Formula;
use crate::fst_index::{KeyIndex, Label, LabelIndex};
use crate::VectorR;

mod file_names {
    pub const NODES: &str = "nodes.kv";
    pub const HNSW: &str = "index.hnsw";
    pub const JOURNAL: &str = "journal.json";
    pub const FST: &str = "fst";
}

pub struct NoDLog;
impl DeleteLog for NoDLog {
    fn is_deleted(&self, _: &[u8]) -> bool {
        false
    }
}

#[derive(Default, Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Similarity {
    Dot,
    #[default]
    Cosine,
}
impl Similarity {
    pub fn compute(&self, x: &[u8], y: &[u8]) -> f32 {
        match self {
            Similarity::Cosine => vector::cosine_similarity(x, y),
            Similarity::Dot => vector::dot_similarity(x, y),
        }
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

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct Address(usize);
impl Address {
    #[cfg(test)]
    pub const fn dummy() -> Address {
        Address(0)
    }
}

pub struct FormulaFilter<'a> {
    pub matching_nodes: HashSet<u64>,
    empty_formula: bool,
    filter: &'a Formula,
    pub use_fst: bool,
    total_nodes: usize,
}

impl FormulaFilter<'_> {
    fn try_new_with_fst<'a>(
        filter: &'a Formula,
        key_index: &'a KeyIndex,
        label_index: &'a LabelIndex,
        total_nodes: usize,
    ) -> VectorR<FormulaFilter<'a>> {
        debug!("Using FST index files");
        let collector = filter.get_atoms();
        let empty_formula = collector.labels.is_empty() && collector.key_prefixes.is_empty();

        // preparing the list of matching nodes
        let mut matching_labels = None;
        let mut matching_keys = None;
        if !collector.labels.is_empty() {
            matching_labels = Some(label_index.get_nodes(&collector.labels)?);
        }
        if !collector.key_prefixes.is_empty() {
            matching_keys = Some(key_index.get_nodes(&collector.key_prefixes)?);
        }

        let matching_nodes: HashSet<u64> = match (matching_keys, matching_labels) {
            (None, labels) => labels.unwrap_or_default(),
            (keys, None) => keys.unwrap_or_default(),
            (Some(keys), Some(labels)) => keys.intersection(&labels).copied().collect(),
        };

        Ok(FormulaFilter {
            matching_nodes,
            empty_formula,
            filter,
            use_fst: true,
            total_nodes,
        })
    }
    pub fn new<'a>(
        filter: &'a Formula,
        key_index: Option<&'a KeyIndex>,
        label_index: Option<&'a LabelIndex>,
        total_nodes: usize,
    ) -> FormulaFilter<'a> {
        let (Some(key_index), Some(label_index)) = (key_index, label_index) else {
            debug!("No FST index files found, using regular filtering");
            return FormulaFilter {
                filter,
                total_nodes,
                matching_nodes: HashSet::new(),
                empty_formula: true,
                use_fst: false,
            };
        };

        match FormulaFilter::try_new_with_fst(filter, key_index, label_index, total_nodes) {
            Ok(successful_fst) => successful_fst,
            Err(err) => {
                error!("No FST due to corrupted index files: {err:?}");
                FormulaFilter {
                    filter,
                    total_nodes,
                    matching_nodes: HashSet::new(),
                    empty_formula: true,
                    use_fst: false,
                }
            }
        }
    }

    /// Returns the ratio of matching nodes
    pub fn matching_ratio(&self) -> Option<f64> {
        if !self.use_fst || self.empty_formula {
            return None;
        }

        if self.matching_nodes.is_empty() {
            return Some(0.0);
        }

        Some(self.matching_nodes.len() as f64 / self.total_nodes as f64)
    }

    pub fn run<DR: DataRetriever>(&self, address: Address, tracker: &DR) -> bool {
        if !self.use_fst {
            // we don't use FST, calling legacy run
            self.filter.run(address, tracker)
        } else {
            // if we did not have any filtering, it's always a match
            if self.empty_formula {
                return true;
            }
            // if we have no matches at all, we can return false
            if self.matching_nodes.is_empty() {
                return false;
            }
            // O(1) on average
            self.matching_nodes.contains(&(address.0 as u64))
        }
    }
}

pub struct Retriever<'a, Dlog> {
    similarity: Similarity,
    no_nodes: usize,
    temp: &'a [u8],
    nodes: &'a Mmap,
    delete_log: &'a Dlog,
    min_score: f32,
}
impl<'a, Dlog: DeleteLog> Retriever<'a, Dlog> {
    pub fn new(
        temp: &'a [u8],
        nodes: &'a Mmap,
        delete_log: &'a Dlog,
        similarity: Similarity,
        min_score: f32,
    ) -> Retriever<'a, Dlog> {
        Retriever {
            temp,
            nodes,
            delete_log,
            similarity,
            no_nodes: key_value::get_no_elems(nodes),
            min_score,
        }
    }
    fn find_node(&self, Address(x): Address) -> &[u8] {
        if x == self.no_nodes {
            self.temp
        } else {
            key_value::get_value(Node, self.nodes, x)
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
            let x = key_value::get_value(Node, self.nodes, x);
            Node::has_label(x, label)
        }
    }
    fn similarity(&self, x @ Address(a0): Address, y @ Address(a1): Address) -> f32 {
        if a0 == self.no_nodes {
            let y = self.find_node(y);
            let y = Node::vector(y);
            self.similarity.compute(self.temp, y)
        } else if a1 == self.no_nodes {
            let x = self.find_node(x);
            let x = Node::vector(x);
            self.similarity.compute(self.temp, x)
        } else {
            let x = self.find_node(x);
            let y = self.find_node(y);
            let x = Node::vector(x);
            let y = Node::vector(y);
            self.similarity.compute(x, y)
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
    pub vector: Vec<u8>,
    pub metadata: Option<Vec<u8>>,
    pub labels: LabelDictionary,
}
impl Elem {
    pub fn new(
        key: String,
        vector: Vec<f32>,
        labels: LabelDictionary,
        metadata: Option<Vec<u8>>,
    ) -> Elem {
        Elem {
            labels,
            metadata,
            key: key.as_bytes().to_vec(),
            vector: vector::encode_vector(&vector),
        }
    }
}

impl key_value::KVElem for Elem {
    fn serialized_len(&self) -> usize {
        Node::serialized_len(
            &self.key,
            &self.vector,
            &self.labels.0,
            self.metadata.as_ref(),
        )
    }
    fn serialize_into<W: io::Write>(self, w: W) -> io::Result<()> {
        Node::serialize_into(
            w,
            self.key,
            self.vector,
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
        self.node.partial_cmp(&other.node)
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
            node: Node::serialize(key, [1, 2, 3, 4], [], None as Option<&[u8]>),
        }
    }
    fn new(Address(addr): Address, data: &[u8], score: f32) -> Neighbour {
        let node = key_value::get_value(Node, data, addr);
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

pub struct DataPoint {
    journal: Journal,
    nodes: Mmap,
    index: Mmap,
    label_index: Option<LabelIndex>,
    key_index: Option<KeyIndex>,
}

impl AsRef<DataPoint> for DataPoint {
    fn as_ref(&self) -> &DataPoint {
        self
    }
}

impl DataPoint {
    pub fn stored_len(&self) -> Option<u64> {
        if key_value::get_no_elems(&self.nodes) == 0 {
            return None;
        }
        let node = key_value::get_value(Node, &self.nodes, 0);
        Some(vector::vector_len(Node::vector(node)))
    }
    pub fn get_id(&self) -> DpId {
        self.journal.uid
    }
    pub fn journal(&self) -> Journal {
        self.journal
    }
    pub fn get_keys<Dlog: DeleteLog>(&self, delete_log: &Dlog) -> Vec<String> {
        key_value::get_keys(Node, &self.nodes)
            .filter(|k| !delete_log.is_deleted(k))
            .map(String::from_utf8_lossy)
            .map(|s| s.to_string())
            .collect()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn search<Dlog: DeleteLog>(
        &self,
        delete_log: &Dlog,
        query: &[f32],
        filter: &Formula,
        with_duplicates: bool,
        results: usize,
        similarity: Similarity,
        min_score: f32,
    ) -> impl Iterator<Item = Neighbour> + '_ {
        let encoded_query = vector::encode_vector(query);
        let tracker = Retriever::new(
            &encoded_query,
            &self.nodes,
            delete_log,
            similarity,
            min_score,
        );

        let no_nodes = key_value::get_no_elems(&self.nodes);

        let filter = FormulaFilter::new(
            filter,
            self.key_index.as_ref(),
            self.label_index.as_ref(),
            no_nodes,
        );

        let ops = HnswOps::new(&tracker);
        let neighbours = ops.search(
            Address(self.journal.nodes),
            self.index.as_ref(),
            results,
            filter,
            with_duplicates,
        );
        neighbours
            .into_iter()
            .map(|(address, dist)| (Neighbour::new(address, &self.nodes, dist)))
            .take(results)
    }
    pub fn merge<Dlog>(
        dir: &path::Path,
        operants: &[(Dlog, DpId)],
        similarity: Similarity,
        channel: Channel,
    ) -> VectorR<DataPoint>
    where
        Dlog: DeleteLog,
    {
        let uid = DpId::new_v4().to_string();
        let id = dir.join(&uid);
        fs::create_dir(&id)?;
        let mut nodes = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(id.join(file_names::NODES))?;
        let mut journalf = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(id.join(file_names::JOURNAL))?;
        let mut hnswf = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(id.join(file_names::HNSW))?;
        let operants = operants
            .iter()
            .map(|(dlog, dp_id)| DataPoint::open(dir, *dp_id).map(|v| (dlog, v)))
            .collect::<VectorR<Vec<_>>>()?;

        // Creating the node store
        let node_producers = operants
            .iter()
            .map(|dp| ((dp.0, Node), dp.1.nodes.as_ref()));
        {
            let mut node_buffer = BufWriter::new(&mut nodes);
            key_value::merge(&mut node_buffer, node_producers.collect())?;
            node_buffer.flush()?;
        }

        let nodes = unsafe { Mmap::map(&nodes)? };
        let no_nodes = key_value::get_no_elems(&nodes);

        // Creating the FSTs with the new nodes
        let (label_index, key_index) = if channel == Channel::EXPERIMENTAL {
            debug!("Indexing with experimental FSTs");
            Self::create_fsts(&id, &nodes)?
        } else {
            (None, None)
        };

        // Creating the hnsw for the new node store.
        let tracker = Retriever::new(&[], &nodes, &NoDLog, similarity, -1.0);
        let mut ops = HnswOps::new(&tracker);
        let mut index = RAMHnsw::new();
        for id in 0..no_nodes {
            ops.insert(Address(id), &mut index)
        }

        {
            let mut hnswf_buffer = BufWriter::new(&mut hnswf);
            DiskHnsw::serialize_into(&mut hnswf_buffer, no_nodes, index)?;
            hnswf_buffer.flush()?;
        }

        let index = unsafe { Mmap::map(&hnswf)? };

        let journal = Journal {
            nodes: no_nodes,
            uid: DpId::parse_str(&uid).unwrap(),
            ctime: SystemTime::now(),
        };

        {
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

        Ok(DataPoint {
            journal,
            nodes,
            index,
            label_index,
            key_index,
        })
    }
    pub fn delete(dir: &path::Path, uid: DpId) -> VectorR<()> {
        let uid = uid.to_string();
        let id = dir.join(uid);
        fs::remove_dir_all(id)?;
        Ok(())
    }
    pub fn open(dir: &path::Path, uid: DpId) -> VectorR<DataPoint> {
        let uid = uid.to_string();
        let id = dir.join(uid);
        let nodes = fs::OpenOptions::new()
            .read(true)
            .open(id.join(file_names::NODES))?;
        let journal = fs::OpenOptions::new()
            .read(true)
            .open(id.join(file_names::JOURNAL))?;
        let hnswf = fs::OpenOptions::new()
            .read(true)
            .open(id.join(file_names::HNSW))?;

        let nodes = unsafe { Mmap::map(&nodes)? };
        let index = unsafe { Mmap::map(&hnswf)? };
        let journal: Journal = serde_json::from_reader(journal)?;

        let fst_dir = id.join(file_names::FST);

        let (label_index, key_index) = if LabelIndex::exists(&fst_dir) && KeyIndex::exists(&fst_dir)
        {
            debug!("Found FSTs on disk");
            (
                Some(LabelIndex::open(&fst_dir)?),
                Some(KeyIndex::open(&fst_dir)?),
            )
        } else {
            (None, None)
        };

        // Telling the OS our expected access pattern
        #[cfg(not(target_os = "windows"))]
        {
            nodes.advise(memmap2::Advice::WillNeed)?;
            index.advise(memmap2::Advice::Sequential)?;
        }

        Ok(DataPoint {
            journal,
            nodes,
            index,
            label_index,
            key_index,
        })
    }

    fn create_fsts(
        root_dir: &path::Path,
        nodes: &[u8],
    ) -> VectorR<(Option<LabelIndex>, Option<KeyIndex>)> {
        let no_nodes = key_value::get_no_elems(nodes);

        // building the KeyIndex and LabelIndex FSTs
        let fst_dir = root_dir.join(file_names::FST);
        fs::create_dir(&fst_dir)?;

        // Memory representations of the FSTs we want to store
        // - node_keys holds (key, record id) tuples
        // - labels_map holds a mapping of (label, record ids)
        // - labels_list holds (label, record ids) tuples
        let mut node_keys: Vec<(String, u64)> = Vec::new();
        let mut labels_map: HashMap<String, Vec<u64>> = HashMap::new();

        // we iterate on each node to fill `keys`
        for node_id in 0..no_nodes {
            let node = key_value::get_value(Node, nodes, node_id);
            let key: String = String::from_utf8(Node::key(node).to_vec())?;
            let node_labels = Node::labels(node);

            for label in &node_labels {
                if let Some(vec) = labels_map.get_mut(label) {
                    vec.push(node_id as u64);
                } else {
                    let new_vec = vec![node_id as u64];
                    labels_map.insert(label.to_string(), new_vec);
                }
            }
            node_keys.push((key.clone(), node_id as u64));
        }
        node_keys.sort();
        // create the KeyIndex
        let key_index = KeyIndex::new(&fst_dir, node_keys.into_iter())?;

        // we convert `labels_map` into a list
        let mut labels_list: Vec<Label> = Vec::new();
        for (key, mut node_addresses) in labels_map {
            node_addresses.sort();
            labels_list.push(Label {
                key,
                node_addresses,
            })
        }
        labels_list.sort_by_key(|label| label.key.clone());

        // creating the LabelIndex
        let label_index = LabelIndex::new(&fst_dir, labels_list.into_iter())?;

        Ok((Some(label_index), Some(key_index)))
    }

    pub fn new(
        dir: &path::Path,
        mut elems: Vec<Elem>,
        time: Option<SystemTime>,
        similarity: Similarity,
        channel: Channel,
    ) -> VectorR<DataPoint> {
        let uid = DpId::new_v4().to_string();
        let id = dir.join(&uid);
        fs::create_dir(&id)?;
        let mut nodesf = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(id.join(file_names::NODES))?;
        let mut journalf = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(id.join(file_names::JOURNAL))?;
        let mut hnswf = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(id.join(file_names::HNSW))?;

        elems.sort_by(|a, b| a.key.cmp(&b.key));
        elems.dedup_by(|a, b| a.key.cmp(&b.key).is_eq());
        {
            // Serializing nodes on disk
            // Nodes are stored on disk and mmaped.
            let mut nodesf_buffer = BufWriter::new(&mut nodesf);
            key_value::create_key_value(&mut nodesf_buffer, elems)?;
            nodesf_buffer.flush()?;
        }
        let nodes = unsafe { Mmap::map(&nodesf)? };
        let no_nodes = key_value::get_no_elems(&nodes);

        // Creating the FSTs
        let (label_index, key_index) = if channel == Channel::EXPERIMENTAL {
            debug!("Indexing with experimental FSTs");
            Self::create_fsts(&id, &nodes)?
        } else {
            (None, None)
        };

        // Creating the HNSW using the mmaped nodes
        let tracker = Retriever::new(&[], &nodes, &NoDLog, similarity, -1.0);
        let mut ops = HnswOps::new(&tracker);
        let mut index = RAMHnsw::new();
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
            uid: DpId::parse_str(&uid).unwrap(),
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

        Ok(DataPoint {
            journal,
            nodes,
            index,
            label_index,
            key_index,
        })
    }
}
