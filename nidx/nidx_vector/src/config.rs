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

use std::mem::size_of;

use nidx_protos::VectorIndexConfig;
use nidx_protos::{VectorSimilarity, VectorType as ProtoVectorType};
use serde::{Deserialize, Serialize};

use crate::VectorErr;
use crate::vector_types::*;

#[derive(Default, Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Similarity {
    Dot,
    #[default]
    Cosine,
}

impl From<VectorSimilarity> for Similarity {
    fn from(value: VectorSimilarity) -> Self {
        match value {
            VectorSimilarity::Cosine => Self::Cosine,
            VectorSimilarity::Dot => Self::Dot,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum VectorType {
    DenseF32 { dimension: usize },
}

impl VectorType {
    pub fn encode(&self, vector: &[f32]) -> Vec<u8> {
        match self {
            #[rustfmt::skip]
            VectorType::DenseF32 { .. } => dense_f32::encode_vector(vector),
        }
    }

    pub fn vector_alignment(&self) -> usize {
        match self {
            #[rustfmt::skip]
            VectorType::DenseF32 { .. } => size_of::<f32>(),
        }
    }

    pub fn dimension(&self) -> Option<usize> {
        match self {
            #[rustfmt::skip]
            VectorType::DenseF32 { dimension } => Some(*dimension),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct VectorConfig {
    #[serde(default)]
    pub similarity: Similarity,
    #[serde(default)]
    pub normalize_vectors: bool,
    pub vector_type: VectorType,
}

impl VectorConfig {
    /// The length of bytes of each vector
    pub fn vector_len_bytes(&self) -> usize {
        match self.vector_type {
            VectorType::DenseF32 { dimension } => dimension * size_of::<f32>(),
        }
    }

    pub fn similarity_function(&self) -> fn(&[u8], &[u8]) -> f32 {
        match (&self.similarity, &self.vector_type) {
            #[rustfmt::skip]
            (Similarity::Dot, VectorType::DenseF32 { .. }) => dense_f32::dot_similarity,
            #[rustfmt::skip]
            (Similarity::Cosine, VectorType::DenseF32 { .. }) => dense_f32::cosine_similarity,
        }
    }
}

impl TryFrom<VectorIndexConfig> for VectorConfig {
    type Error = VectorErr;

    fn try_from(proto: VectorIndexConfig) -> Result<Self, Self::Error> {
        let vector_type = match (proto.vector_type(), proto.vector_dimension) {
            (ProtoVectorType::DenseF32, Some(0)) => {
                return Err(VectorErr::InvalidConfiguration("Vector dimension cannot be 0"));
            }
            (ProtoVectorType::DenseF32, None) => {
                return Err(VectorErr::InvalidConfiguration("Vector dimension required"));
            }
            (ProtoVectorType::DenseF32, Some(dim)) => VectorType::DenseF32 {
                dimension: dim as usize,
            },
        };
        Ok(VectorConfig {
            similarity: proto.similarity().into(),
            normalize_vectors: proto.normalize_vectors,
            vector_type,
        })
    }
}
