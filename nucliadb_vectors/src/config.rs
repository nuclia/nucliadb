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

use nucliadb_core::protos::VectorIndexConfig;
use nucliadb_core::protos::{VectorSimilarity, VectorType as ProtoVectorType};
use serde::{Deserialize, Serialize};

use crate::VectorErr;

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

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum VectorType {
    #[default]
    DenseF32Unaligned,
    DenseF32 {
        dimension: usize,
    },
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct VectorConfig {
    #[serde(default)]
    pub similarity: Similarity,
    #[serde(default)]
    pub normalize_vectors: bool,
    #[serde(default)]
    pub vector_type: VectorType,
}

impl TryFrom<VectorIndexConfig> for VectorConfig {
    type Error = VectorErr;

    fn try_from(proto: VectorIndexConfig) -> Result<Self, Self::Error> {
        let vector_type = match (proto.vector_type(), proto.vector_dimension) {
            (ProtoVectorType::DenseF32, Some(dim)) => VectorType::DenseF32 {
                dimension: dim as usize,
            },
            (ProtoVectorType::DenseF32, None) => VectorType::DenseF32Unaligned,
        };
        Ok(VectorConfig {
            similarity: proto.similarity().into(),
            normalize_vectors: proto.normalize_vectors,
            vector_type,
        })
    }
}
