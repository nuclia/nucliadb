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

use std::{fs::File, path::Path};

use memmap2::Mmap;
pub use node::Node;

use crate::{
    config::{VectorConfig, VectorType},
    data_point::Elem,
};

mod node;
mod store;

const NODES: &str = "nodes.kv";

pub struct DataStore {
    nodes: Mmap,
}

impl DataStore {
    pub fn size_bytes(&self) -> usize {
        self.nodes.len()
    }

    pub fn stored_elements(&self) -> usize {
        store::stored_elements(&self.nodes)
    }

    pub fn get_value(&self, id: usize) -> Node {
        store::get_value(&self.nodes, id)
    }

    pub fn will_need(&self, id: usize, vector_len: usize) {
        store::will_need(&self.nodes, id, vector_len);
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
        segments: &mut [(impl Iterator<Item = usize>, &DataStore)],
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
    fn serialize_into<W: std::io::Write>(self, w: W, vector_type: &VectorType) -> std::io::Result<()> {
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
