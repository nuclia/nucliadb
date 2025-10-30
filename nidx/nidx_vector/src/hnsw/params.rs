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
