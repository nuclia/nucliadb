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

use crate::{VectorErr, VectorR, config::VectorType};

pub fn extract_multi_vectors(vector: &[f32], vector_type: &VectorType) -> VectorR<Vec<Vec<f32>>> {
    let dimension = vector_type.dimension();
    if !vector.len().is_multiple_of(dimension) {
        return Err(VectorErr::InconsistentDimensions {
            index_config: dimension,
            vector: vector.len(),
        });
    }

    let vectors = (0..vector.len() / dimension)
        .map(|i| vector[i * dimension..(i + 1) * dimension].to_vec())
        .collect();
    Ok(vectors)
}

pub fn maxsim_similarity(similarity: fn(&[u8], &[u8]) -> f32, query: &Vec<Vec<u8>>, document: &[&[u8]]) -> f32 {
    let mut summaxsim = 0.0;
    for v_q in query {
        let mut maxsim = 0.0;
        for v_doc in document {
            let sim = similarity(v_doc, v_q);
            if sim > maxsim {
                maxsim = sim
            }
        }
        summaxsim += maxsim;
    }
    summaxsim
}
