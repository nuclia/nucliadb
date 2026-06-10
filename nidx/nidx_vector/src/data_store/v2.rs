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
use std::path::Path;
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

        for (idx, elem) in (0..).zip(entries) {
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
    fn has_quantized(&self) -> bool {
        self.quantized.is_some()
    }

    fn size_bytes(&self) -> usize {
        self.vectors.size_bytes()
            + self.paragraphs.size_bytes()
            + self.quantized.as_ref().map_or(0, |quantized| quantized.size_bytes())
    }

    fn prewarm_size_bytes(&self) -> usize {
        if let Some(quantized) = &self.quantized {
            quantized.size_bytes()
        } else {
            0
        }
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
