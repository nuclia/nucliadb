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
use std::io::BufReader;
use std::time::SystemTime;
use std::{fs, io, path};

use data_store::Interpreter;
use disk_hnsw::DiskHnsw;
use io::{BufWriter, Write};
use memmap2::Mmap;
use node::Node;
pub use ops_hnsw::DataRetriever;
use ops_hnsw::HnswOps;
use ram_hnsw::RAMHnsw;
use serde::{Deserialize, Serialize};
pub use uuid::Uuid as DpId;

use crate::data_types::{data_store, trie, trie_ram, vector, DeleteLog};
use crate::formula::Formula;
use crate::VectorR;

mod file_names {
    pub const NODES: &str = "nodes.kv";
    pub const HNSW: &str = "index.hnsw";
    pub const JOURNAL: &str = "journal.json";
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
}
impl<'a, Dlog: DeleteLog> Retriever<'a, Dlog> {
    pub fn new(
        temp: &'a [u8],
        nodes: &'a Mmap,
        delete_log: &'a Dlog,
        similarity: Similarity,
        min_score: f32,
    ) -> Retriever<'a, Dlog> {
        let no_nodes = data_store::stored_elements(nodes);
        let similarity_function = match similarity {
            Similarity::Cosine => vector::cosine_similarity,
            Similarity::Dot => vector::dot_similarity,
        };
        Retriever {
            temp,
            nodes,
            delete_log,
            similarity_function,
            no_nodes,
            min_score,
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
    pub vector: Vec<u8>,
    pub metadata: Option<Vec<u8>>,
    pub labels: LabelDictionary,
}
impl Elem {
    pub fn new(key: String, vector: Vec<f32>, labels: LabelDictionary, metadata: Option<Vec<u8>>) -> Elem {
        Elem {
            labels,
            metadata,
            key: key.as_bytes().to_vec(),
            vector: vector::encode_vector(&vector),
        }
    }
}

impl data_store::IntoBuffer for Elem {
    fn serialize_into<W: io::Write>(self, w: W) -> io::Result<()> {
        Node::serialize_into(w, self.key, self.vector, self.labels.0, self.metadata.as_ref())
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
            node: Node::serialize(key, [1, 2, 3, 4], [], None as Option<&[u8]>),
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

pub struct DataPoint {
    journal: Journal,
    nodes: Mmap,
    index: Mmap,
}

impl AsRef<DataPoint> for DataPoint {
    fn as_ref(&self) -> &DataPoint {
        self
    }
}

impl DataPoint {
    pub fn stored_len(&self) -> Option<u64> {
        if data_store::stored_elements(&self.nodes) == 0 {
            return None;
        }
        let node = data_store::get_value(Node, &self.nodes, 0);
        Some(vector::vector_len(Node::vector(node)))
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
        similarity: Similarity,
        min_score: f32,
    ) -> impl Iterator<Item = Neighbour> + '_ {
        let encoded_query = vector::encode_vector(query);
        let tracker = Retriever::new(&encoded_query, &self.nodes, delete_log, similarity, min_score);
        let filter = FormulaFilter::new(filter);
        let ops = HnswOps::new(&tracker);
        let neighbours = ops.search(Address(self.journal.nodes), self.index.as_ref(), results, filter, with_duplicates);

        neighbours.into_iter().map(|(address, dist)| (Neighbour::new(address, &self.nodes, dist))).take(results)
    }
    pub fn merge<Dlog>(dir: &path::Path, operants: &[(Dlog, DpId)], similarity: Similarity) -> VectorR<DataPoint>
    where
        Dlog: DeleteLog,
    {
        let uid = DpId::new_v4().to_string();
        let id = dir.join(&uid);
        fs::create_dir(&id)?;
        let mut nodes = fs::OpenOptions::new().read(true).write(true).create(true).open(id.join(file_names::NODES))?;
        let mut journalf =
            fs::OpenOptions::new().read(true).write(true).create(true).open(id.join(file_names::JOURNAL))?;
        let mut hnswf = fs::OpenOptions::new().read(true).write(true).create(true).open(id.join(file_names::HNSW))?;
        let operants = operants
            .iter()
            .map(|(dlog, dp_id)| DataPoint::open(dir, *dp_id).map(|v| (dlog, v)))
            .collect::<VectorR<Vec<_>>>()?;

        // Creating the node store
        let node_producers: Vec<_> = operants.iter().map(|dp| ((dp.0, Node), dp.1.nodes.as_ref())).collect();
        data_store::merge(&mut nodes, &node_producers)?;
        let nodes = unsafe { Mmap::map(&nodes)? };
        let no_nodes = data_store::stored_elements(&nodes);

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
        let nodes = fs::OpenOptions::new().read(true).open(id.join(file_names::NODES))?;
        let journal = fs::OpenOptions::new().read(true).open(id.join(file_names::JOURNAL))?;
        let hnswf = fs::OpenOptions::new().read(true).open(id.join(file_names::HNSW))?;

        let nodes = unsafe { Mmap::map(&nodes)? };
        let index = unsafe { Mmap::map(&hnswf)? };
        let journal: Journal = serde_json::from_reader(BufReader::new(journal))?;

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
        })
    }

    pub fn new(
        dir: &path::Path,
        elems: Vec<Elem>,
        time: Option<SystemTime>,
        similarity: Similarity,
    ) -> VectorR<DataPoint> {
        let uid = DpId::new_v4().to_string();
        let id = dir.join(&uid);
        fs::create_dir(&id)?;
        let mut nodesf = fs::OpenOptions::new().read(true).write(true).create(true).open(id.join(file_names::NODES))?;
        let mut journalf =
            fs::OpenOptions::new().read(true).write(true).create(true).open(id.join(file_names::JOURNAL))?;
        let mut hnswf = fs::OpenOptions::new().read(true).write(true).create(true).open(id.join(file_names::HNSW))?;

        // Serializing nodes on disk
        // Nodes are stored on disk and mmaped.
        data_store::create_key_value(&mut nodesf, elems)?;
        let nodes = unsafe { Mmap::map(&nodesf)? };
        let no_nodes = data_store::stored_elements(&nodes);

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
        })
    }
}
