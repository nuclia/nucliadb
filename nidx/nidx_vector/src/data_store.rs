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

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Ord, Eq)]
pub struct VectorRef<'a> {
    vector: &'a [u8],
    paragraph_addr: u32,
}

impl<'a> VectorRef<'a> {
    pub fn vector(&self) -> &'a [u8] {
        self.vector
    }

    pub fn paragraph(&self) -> u32 {
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
}

pub trait DataStore: Sync + Send {
    fn size_bytes(&self) -> usize;
    fn stored_elements(&self) -> usize;
    fn get_paragraph(&self, id: usize) -> ParagraphRef;
    fn get_vector(&self, id: usize) -> VectorRef;
    fn will_need(&self, id: usize);
    fn as_any(&self) -> &dyn Any;
    // fn open(path: &Path) -> std::io::Result<Self>;
    // fn create(path: &Path, slots: Vec<Elem>, vector_type: &VectorType) -> std::io::Result<()>;
    // fn merge(
    //     path: &Path,
    //     segments: &mut [(impl Iterator<Item = usize>, &Self)],
    //     config: &VectorConfig,
    // ) -> std::io::Result<bool>;
}
