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
#[cfg(test)]
mod tests;

use std::time::SystemTime;
use std::{fs, io, path};

use disk_hnsw::DiskHnsw;
use key_value::Slot;
use memmap2::Mmap;
use node::Node;
use ops_hnsw::{DataRetriever, HnswOps};
use ram_hnsw::RAMHnsw;
use serde::{Deserialize, Serialize};
use thiserror::Error;
pub use uuid::Uuid as DpId;

use crate::data_types::{key_value, trie, trie_ram, vector, DeleteLog};

mod file_names {
    pub const NODES: &str = "nodes.kv";
    pub const HNSW: &str = "index.hnsw";
    pub const JOURNAL: &str = "journal.json";
}

#[derive(Error, Debug)]
pub enum DPError {
    #[error("io Error: {0}")]
    IO(#[from] io::Error),
    #[error("bincode error: {0}")]
    BC(#[from] bincode::Error),
    #[error("json error: {0}")]
    SJ(#[from] serde_json::Error),
}
type DPResult<T> = Result<T, DPError>;

pub struct NoDLog;
impl DeleteLog for NoDLog {
    fn is_deleted(&self, _: &str) -> bool {
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

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct Address(usize);

pub struct Retriever<'a, Dlog> {
    no_nodes: usize,
    temp: &'a [u8],
    nodes: &'a Mmap,
    delete_log: &'a Dlog,
}
impl<'a, Dlog: DeleteLog> Retriever<'a, Dlog> {
    pub fn new(temp: &'a [u8], nodes: &'a Mmap, delete_log: &'a Dlog) -> Retriever<'a, Dlog> {
        Retriever {
            temp,
            nodes,
            delete_log,
            no_nodes: key_value::get_no_elems(nodes),
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
            let key = std::str::from_utf8(Node::key(x)).unwrap();
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
    fn consine_similarity(&self, x @ Address(a0): Address, y @ Address(a1): Address) -> f32 {
        if a0 == self.no_nodes {
            let y = self.find_node(y);
            let y = Node::vector(y);
            vector::consine_similarity(self.temp, y)
        } else if a1 == self.no_nodes {
            let x = self.find_node(x);
            let x = Node::vector(x);
            vector::consine_similarity(self.temp, x)
        } else {
            let x = self.find_node(x);
            let y = self.find_node(y);
            let x = Node::vector(x);
            let y = Node::vector(y);
            vector::consine_similarity(x, y)
        }
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
    pub labels: LabelDictionary,
}
impl Elem {
    pub fn new(key: String, vector: Vec<f32>, labels: LabelDictionary) -> Elem {
        Elem {
            labels,
            key: key.as_bytes().to_vec(),
            vector: vector::encode_vector(&vector),
        }
    }
}

impl key_value::KVElem for Elem {
    fn serialized_len(&self) -> usize {
        Node::serialized_len(&self.key, &self.vector, &self.labels.0)
    }
    fn serialize_into<W: io::Write>(self, w: W) -> io::Result<()> {
        Node::serialize_into(w, self.key, self.vector, self.labels.0)
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
    pub fn get_id(&self) -> DpId {
        self.journal.uid
    }
    pub fn meta(&self) -> Journal {
        self.journal
    }
    pub fn get_keys<Dlog: DeleteLog>(&self, delete_log: &Dlog) -> Vec<String> {
        key_value::get_keys(Node, &self.nodes)
            .map(String::from_utf8_lossy)
            .map(|s| s.to_string())
            .filter(|k| !delete_log.is_deleted(k.as_str()))
            .collect()
    }
    pub fn search<Dlog: DeleteLog>(
        &self,
        delete_log: &Dlog,
        query: &[f32],
        labels: &[String],
        with_duplicates: bool,
        results: usize,
    ) -> Vec<(String, f32)> {
        use ops_hnsw::params;
        let labels = labels.iter().map(|l| l.as_bytes()).collect::<Vec<_>>();
        let encoded_query = vector::encode_vector(query);
        let tacker = Retriever::new(&encoded_query, &self.nodes, delete_log);
        let ops = HnswOps { tracker: &tacker };
        let neighbours = ops.search(
            Address(self.journal.nodes),
            self.index.as_ref(),
            params::k_neighbours(),
            &labels,
            with_duplicates,
        );
        neighbours
            .into_iter()
            .map(|(Address(addr), dist)| (addr, dist))
            .map(|(addr, dist)| (key_value::get_value(Node, &self.nodes, addr), dist))
            .map(|(node, dist)| (Node.get_key(node), dist))
            .map(|(node, dist)| (std::str::from_utf8(node), dist))
            .map(|(node, dist)| (node.unwrap().to_string(), dist))
            .take(results)
            .collect()
    }
    pub fn merge<Dlog>(dir: &path::Path, operants: &[(Dlog, DpId)]) -> DPResult<DataPoint>
    where Dlog: DeleteLog {
        use io::{BufWriter, Write};
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
            .collect::<DPResult<Vec<_>>>()?;
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
        let tracker = Retriever::new(&[], &nodes, &NoDLog);
        let ops = HnswOps { tracker: &tracker };
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

        Ok(DataPoint {
            journal,
            nodes,
            index,
        })
    }
    pub fn delete(dir: &path::Path, uid: DpId) -> DPResult<()> {
        let uid = uid.to_string();
        let id = dir.join(uid);
        fs::remove_dir_all(id)?;
        Ok(())
    }
    pub fn open(dir: &path::Path, uid: DpId) -> DPResult<DataPoint> {
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
        Ok(DataPoint {
            journal,
            nodes,
            index,
        })
    }
    pub fn new(
        dir: &path::Path,
        mut elems: Vec<Elem>,
        with_time: Option<SystemTime>,
    ) -> DPResult<DataPoint> {
        use io::{BufWriter, Write};

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

        // Creating the HNSW using the mmaped nodes
        let tracker = Retriever::new(&[], &nodes, &NoDLog);
        let ops = HnswOps { tracker: &tracker };
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
            ctime: with_time.unwrap_or_else(SystemTime::now),
        };
        {
            // Saving the journal
            let mut journalf_buffer = BufWriter::new(&mut journalf);
            journalf_buffer.write_all(&serde_json::to_vec(&journal)?)?;
            journalf_buffer.flush()?;
        }

        Ok(DataPoint {
            journal,
            nodes,
            index,
        })
    }
}
