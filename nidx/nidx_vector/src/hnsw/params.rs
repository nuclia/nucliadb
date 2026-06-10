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

//! Set of parameters needed by the HNSW algorithm
//! as named and used in the paper.

/// Factor by which the layer distribution should deviate.
pub fn level_factor() -> f64 {
    1.0 / (M as f64).ln()
}

pub const fn m_max_for_layer(layer: usize) -> usize {
    if layer == 0 { M_MAX_0 } else { M_MAX }
}

/// M to use when pruning neighbours
pub const fn prune_m(m: usize) -> usize {
    m * 95 / 100
}

/// Upper limit to the number of out-edges a embedding can have.
pub const M_MAX_0: usize = 60;

/// Upper limit to the number of out-edges a embedding can have.
pub const M_MAX: usize = 30;

/// Number of bi-directional links created for every new element.
pub const M: usize = 30;

/// Number of neighbours that are explored when searching for the insertion place of a new node
pub const EF_CONSTRUCTION: usize = 100;

/// Number of neighbours that are explored when searching for a query
pub const EF_SEARCH: usize = 30;
