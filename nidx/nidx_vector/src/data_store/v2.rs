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

use crate::{VectorR, config::VectorType, segment::Elem};

use super::{DataStore, OpenReason, ParagraphAddr, VectorAddr};
pub use paragraph_store::StoredParagraph;
use paragraph_store::{ParagraphStore, ParagraphStoreWriter};
use std::path::Path;
use vector_store::{VectorStore, VectorStoreWriter};

mod paragraph_store;
mod vector_store;

pub struct DataStoreV2 {
    paragraphs: ParagraphStore,
    vectors: VectorStore,
}

impl DataStoreV2 {
    pub fn open(path: &Path, vector_type: &VectorType, reason: OpenReason) -> std::io::Result<Self> {
        Ok(Self {
            vectors: VectorStore::open(path, vector_type, &reason)?,
            paragraphs: ParagraphStore::open(path, &reason)?,
        })
    }

    pub fn create(path: &Path, entries: Vec<Elem>, vector_type: &VectorType) -> VectorR<()> {
        let mut paragraphs = ParagraphStoreWriter::new(path)?;
        let mut vectors = VectorStoreWriter::new(path, vector_type)?;

        for (idx, elem) in (0..).zip(entries.into_iter()) {
            let (first_vector, _) = vectors.write(idx, elem.vectors.iter().map(|v| vector_type.encode(v)))?;
            paragraphs.write(StoredParagraph::from_elem(&elem, first_vector))?;
        }

        paragraphs.close()?;
        vectors.close()?;

        Ok(())
    }

    pub fn merge(
        path: &Path,
        producers: Vec<(impl Iterator<Item = ParagraphAddr>, &dyn DataStore)>,
        vector_type: &VectorType,
    ) -> VectorR<()> {
        let mut paragraphs = ParagraphStoreWriter::new(path)?;
        let mut vectors = VectorStoreWriter::new(path, vector_type)?;

        let mut p_idx = 0;
        for (alive, store) in producers {
            for paragraph_addr in alive {
                // Retrieve paragraph and vectors
                let paragraph = store.get_paragraph(paragraph_addr);
                let p_vectors = paragraph.vectors(&paragraph_addr).map(|v| store.get_vector(v).vector());

                // Write to new store
                let (first_vector, last_vector) = vectors.write(p_idx, p_vectors)?;
                paragraphs.write_paragraph_ref(paragraph, first_vector, last_vector - first_vector + 1)?;
                p_idx += 1;
            }
        }

        paragraphs.close()?;
        vectors.close()?;

        Ok(())
    }
}

impl DataStore for DataStoreV2 {
    fn size_bytes(&self) -> usize {
        self.vectors.size_bytes() + self.paragraphs.size_bytes()
    }

    fn stored_paragraph_count(&self) -> usize {
        self.paragraphs.stored_elements()
    }

    fn stored_vector_count(&self) -> usize {
        self.vectors.stored_elements()
    }

    fn get_paragraph(&self, id: ParagraphAddr) -> super::ParagraphRef {
        self.paragraphs.get_paragraph(id)
    }

    fn get_vector(&self, id: VectorAddr) -> super::VectorRef {
        self.vectors.get_vector(id)
    }

    fn will_need(&self, id: VectorAddr) {
        self.vectors.will_need(id);
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
