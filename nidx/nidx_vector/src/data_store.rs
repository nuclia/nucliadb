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

mod v1;
mod v2;

use std::any::Any;

pub use v1::DataStoreV1;
pub use v1::node::Node;
pub use v2::DataStoreV2;
use v2::StoredParagraph;

use crate::{ParagraphAddr, VectorAddr};

pub enum OpenReason {
    Search,
    Create,
}

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Ord, Eq)]
pub struct VectorRef<'a> {
    vector: &'a [u8],
    paragraph_addr: ParagraphAddr,
}

impl<'a> VectorRef<'a> {
    pub fn vector(&self) -> &'a [u8] {
        self.vector
    }

    pub fn paragraph(&self) -> ParagraphAddr {
        self.paragraph_addr
    }
}

#[derive(Clone)]
pub enum ParagraphRef<'a> {
    V1(Node<'a>),
    V2(StoredParagraph<'a>),
}

impl ParagraphRef<'_> {
    pub fn id(&self) -> &str {
        match self {
            ParagraphRef::V1(n) => n.key(),
            ParagraphRef::V2(p) => p.key(),
        }
    }

    pub fn labels(&self) -> Vec<String> {
        match self {
            ParagraphRef::V1(n) => n.labels(),
            ParagraphRef::V2(p) => p.labels(),
        }
    }

    pub fn metadata(&self) -> &[u8] {
        match self {
            ParagraphRef::V1(n) => n.metadata(),
            ParagraphRef::V2(p) => p.metadata(),
        }
    }

    pub fn vectors(&self, addr: &ParagraphAddr) -> impl Iterator<Item = VectorAddr> {
        let (start, len) = match self {
            ParagraphRef::V1(_) => (addr.0, 1),
            ParagraphRef::V2(sp) => sp.vector_first_and_len(),
        };
        (start..start + len).map(VectorAddr)
    }
}

pub trait DataStore: Sync + Send {
    fn size_bytes(&self) -> usize;
    fn stored_paragraph_count(&self) -> usize;
    fn stored_vector_count(&self) -> usize;
    fn get_paragraph(&self, id: ParagraphAddr) -> ParagraphRef;
    fn get_vector(&self, id: VectorAddr) -> VectorRef;
    fn will_need(&self, id: VectorAddr);
    fn as_any(&self) -> &dyn Any;
}

pub fn iter_paragraphs(data_store: &impl DataStore) -> impl Iterator<Item = ParagraphAddr> {
    (0..data_store.stored_paragraph_count() as u32).map(ParagraphAddr)
}
