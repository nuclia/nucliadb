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

use crate::{
    config::{VectorConfig, VectorType},
    data_point::Elem,
};
use memmap2::Mmap;
use node::Node;
use std::{any::Any, fs::File, path::Path};

use super::{DataStore, ParagraphRef, VectorRef};

pub mod node;
pub mod store;

mod trie;
mod trie_ram;

const NODES: &str = "nodes.kv";

pub struct DataStoreV1 {
    nodes: Mmap,
}

impl DataStore for DataStoreV1 {
    fn size_bytes(&self) -> usize {
        self.nodes.len()
    }

    fn stored_elements(&self) -> usize {
        store::stored_elements(&self.nodes)
    }

    fn get_vector(&self, id: usize) -> VectorRef {
        VectorRef {
            vector: store::get_value(&self.nodes, id).vector(),
            paragraph_addr: id as u32,
        }
    }

    fn will_need(&self, id: usize, vector_len: usize) {
        store::will_need(&self.nodes, id, vector_len);
    }
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_paragraph(&self, id: usize) -> ParagraphRef {
        ParagraphRef::V1(store::get_value(&self.nodes, id))
    }
}

impl DataStoreV1 {
    pub fn exists(path: &Path) -> std::io::Result<bool> {
        std::fs::exists(path.join(NODES))
    }

    pub fn open(path: &Path) -> std::io::Result<Self> {
        let nodes_file = File::open(path.join(NODES))?;
        let nodes = unsafe { Mmap::map(&nodes_file)? };

        #[cfg(not(target_os = "windows"))]
        {
            nodes.advise(memmap2::Advice::WillNeed)?;
        }

        Ok(Self { nodes })
    }

    pub fn create(path: &Path, slots: Vec<Elem>, vector_type: &VectorType) -> std::io::Result<()> {
        let mut nodes_file = File::options()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path.join(NODES))?;
        store::create_key_value(&mut nodes_file, slots, vector_type)?;

        Ok(())
    }

    pub fn merge(
        path: &Path,
        segments: &mut [(impl Iterator<Item = usize>, &DataStoreV1)],
        config: &VectorConfig,
    ) -> std::io::Result<bool> {
        let mut nodes_file = File::options()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path.join(NODES))?;
        store::merge(
            &mut nodes_file,
            segments
                .iter_mut()
                .map(|(deletions, store)| (deletions, &store.nodes[..]))
                .collect::<Vec<_>>()
                .as_mut_slice(),
            config,
        )
    }
}

impl store::IntoBuffer for Elem {
    fn serialize_into<W: std::io::Write>(mut self, w: W, vector_type: &VectorType) -> std::io::Result<()> {
        // Serialize labels to trie
        self.labels.sort();
        let ram_trie = trie_ram::create_trie(&self.labels);
        let trie_bytes = trie::serialize(ram_trie);

        Node::serialize_into(
            w,
            self.key,
            vector_type.encode(&self.vector),
            vector_type.vector_alignment(),
            trie_bytes,
            self.metadata.as_ref(),
        )
    }
}
