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

use crate::{VectorErr, VectorR, config::VectorType};

pub fn extract_multi_vectors(vector: &[f32], vector_type: &VectorType) -> VectorR<Vec<Vec<f32>>> {
    let dimension = vector_type.dimension();
    if vector.len() % dimension != 0 {
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
