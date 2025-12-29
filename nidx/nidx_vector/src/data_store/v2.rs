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
    VectorR,
    config::{VectorConfig, VectorType},
    data_store::v2::quant_vector_store::{QuantVectorStore, QuantVectorStoreWriter},
    segment::Elem,
    vector_types::rabitq,
};

use super::{DataStore, OpenReason, ParagraphAddr, VectorAddr};
pub use paragraph_store::StoredParagraph;
use paragraph_store::{ParagraphStore, ParagraphStoreWriter};
use std::{collections::HashMap, path::Path};
use vector_store::{VectorStore, VectorStoreWriter};

mod paragraph_store;
mod quant_vector_store;
mod vector_store;

pub struct DataStoreV2 {
    paragraphs: ParagraphStore,
    vectors: VectorStore,
    quantized: Option<QuantVectorStore>,
}

impl DataStoreV2 {
    pub fn open(path: &Path, vector_type: &VectorType, reason: OpenReason) -> std::io::Result<Self> {
        Ok(Self {
            vectors: VectorStore::open(path, vector_type, &reason)?,
            paragraphs: ParagraphStore::open(path, &reason)?,
            quantized: QuantVectorStore::open(
                path,
                rabitq::EncodedVector::encoded_len(vector_type.dimension()),
                &reason,
            )
            .ok(),
        })
    }

    pub fn create(path: &Path, entries: Vec<Elem>, config: &VectorConfig) -> VectorR<()> {
        let mut paragraphs = ParagraphStoreWriter::new(path)?;
        let mut vectors = VectorStoreWriter::new(path, &config.vector_type)?;
        let mut quantized = if config.quantizable_vectors() {
            Some(QuantVectorStoreWriter::new(path)?)
        } else {
            None
        };

        for (idx, elem) in (0..).zip(entries.into_iter()) {
            let (first_vector, _) = vectors.write(idx, elem.vectors.iter().map(|v| config.vector_type.encode(v)))?;
            if let Some(quantized) = &mut quantized {
                for v in &elem.vectors {
                    quantized.write(&rabitq::EncodedVector::encode(v))?;
                }
            }
            paragraphs.write(StoredParagraph::from_elem(&elem, first_vector))?;
        }

        paragraphs.close()?;
        vectors.close()?;
        if let Some(mut quantized) = quantized {
            quantized.close()?;
        }

        Ok(())
    }

    pub fn merge(
        path: &Path,
        producers: Vec<(impl Iterator<Item = ParagraphAddr>, &dyn DataStore)>,
        config: &VectorConfig,
        mut paragraph_deduplicator: Option<HashMap<String, Vec<u8>>>,
    ) -> VectorR<()> {
        let mut paragraphs = ParagraphStoreWriter::new(path)?;
        let mut vectors = VectorStoreWriter::new(path, &config.vector_type)?;
        let mut quantized = if config.quantizable_vectors() {
            Some(QuantVectorStoreWriter::new(path)?)
        } else {
            None
        };

        let mut p_idx = 0;
        for (alive, store) in producers {
            for paragraph_addr in alive {
                // Retrieve paragraph and vectors
                let paragraph = store.get_paragraph(paragraph_addr);
                let p_vectors = paragraph.vectors(&paragraph_addr).map(|v| store.get_vector(v).vector());

                let metadata = if let Some(paragraph_deduplicator) = &mut paragraph_deduplicator {
                    // Entry is removed so if it appears in other segments it is not copied again
                    let metadata = paragraph_deduplicator.remove(paragraph.id());
                    if metadata.is_none() {
                        continue;
                    };
                    metadata
                } else {
                    None
                };

                // Write to new store
                let (first_vector, last_vector) = vectors.write(p_idx, p_vectors)?;
                if let Some(quantized) = &mut quantized {
                    // Copy quantized vectors if they exist, calculate them if not
                    if store.has_quantized() {
                        for vec_addr in paragraph.vectors(&paragraph_addr) {
                            quantized.write(store.get_quantized_vector(vec_addr).bytes())?;
                        }
                    } else {
                        let p_vectors = paragraph.vectors(&paragraph_addr).map(|v| store.get_vector(v).vector());
                        for v in p_vectors {
                            quantized.write(&rabitq::EncodedVector::encode(config.vector_type.decode(v)))?;
                        }
                    }
                }

                paragraphs.write_paragraph_ref(
                    paragraph,
                    first_vector,
                    last_vector - first_vector + 1,
                    metadata.as_deref(),
                )?;

                p_idx += 1;
            }
        }

        paragraphs.close()?;
        vectors.close()?;

        Ok(())
    }
}

impl DataStore for DataStoreV2 {
    fn has_quantized(&self) -> bool {
        self.quantized.is_some()
    }

    fn size_bytes(&self) -> usize {
        self.vectors.size_bytes() + self.paragraphs.size_bytes()
    }

    fn stored_paragraph_count(&self) -> u32 {
        self.paragraphs.stored_elements()
    }

    fn stored_vector_count(&self) -> u32 {
        self.vectors.stored_elements()
    }

    fn get_paragraph(&self, id: ParagraphAddr) -> super::ParagraphRef<'_> {
        self.paragraphs.get_paragraph(id)
    }

    fn get_vector(&self, id: VectorAddr) -> super::VectorRef<'_> {
        self.vectors.get_vector(id)
    }

    fn get_quantized_vector(&self, id: VectorAddr) -> rabitq::EncodedVector<'_> {
        let Some(quantized) = &self.quantized else {
            panic!("Store does not have quantized vectors")
        };
        quantized.get_vector(id)
    }

    fn will_need(&self, id: VectorAddr) {
        self.vectors.will_need(id);
    }

    fn will_need_quantized(&self, id: VectorAddr) {
        self.quantized.as_ref().unwrap().will_need(id);
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
