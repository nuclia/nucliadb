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

use node::Node;

use crate::{config::VectorType, data_point::Elem};

pub mod node;
pub mod store;

mod trie;
mod trie_ram;

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
