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
