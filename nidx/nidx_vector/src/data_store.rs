// Copyright 2021 Bosutech XXI S.L.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

mod v1;
mod v2;

use std::any::Any;

pub use v1::DataStoreV1;
pub use v1::node::Node;
pub use v2::DataStoreV2;
use v2::StoredParagraph;

use crate::{ParagraphAddr, VectorAddr, vector_types::rabitq};

pub enum OpenReason {
    Search { prewarm: bool },
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
    fn prewarm_size_bytes(&self) -> usize;
    fn stored_paragraph_count(&self) -> u32;
    fn stored_vector_count(&self) -> u32;
    fn get_paragraph(&self, id: ParagraphAddr) -> ParagraphRef<'_>;
    fn get_vector(&self, id: VectorAddr) -> VectorRef<'_>;
    fn get_quantized_vector(&self, id: VectorAddr) -> rabitq::EncodedVector<'_>;
    fn will_need(&self, id: VectorAddr);
    fn will_need_quantized(&self, id: VectorAddr);
    fn as_any(&self) -> &dyn Any;
    fn has_quantized(&self) -> bool;
}

pub fn iter_paragraphs(data_store: &impl DataStore) -> impl Iterator<Item = ParagraphAddr> {
    (0..data_store.stored_paragraph_count()).map(ParagraphAddr)
}
