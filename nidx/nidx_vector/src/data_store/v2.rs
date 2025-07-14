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

use crate::config::VectorType;

use super::DataStore;
use paragraph_store::ParagraphStore;
pub use paragraph_store::StoredParagraph;
use std::path::Path;
use vector_store::VectorStore;

mod paragraph_store;
mod vector_store;

pub struct DataStoreV2 {
    paragraphs: ParagraphStore,
    vectors: VectorStore,
}

impl DataStoreV2 {
    pub fn open(path: &Path, vector_type: VectorType) -> std::io::Result<Self> {
        Ok(Self {
            vectors: VectorStore::open(path, &vector_type)?,
            paragraphs: ParagraphStore::open(path)?,
        })
    }
}

impl DataStore for DataStoreV2 {
    fn size_bytes(&self) -> usize {
        self.vectors.size_bytes() + self.paragraphs.size_bytes()
    }

    fn stored_elements(&self) -> usize {
        todo!()
    }

    fn get_paragraph(&self, id: usize) -> super::ParagraphRef {
        todo!()
    }

    fn get_vector(&self, id: usize) -> super::VectorRef {
        todo!()
    }

    fn will_need(&self, id: usize, vector_len: usize) {
        todo!()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        todo!()
    }
}
