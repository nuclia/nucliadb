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

#[derive(Clone, Copy)]
pub struct RandomVectors(usize);

impl RandomVectors {
    pub fn new(dim: usize) -> RandomVectors {
        RandomVectors(dim)
    }
}
impl Iterator for RandomVectors {
    type Item = Vec<f32>;
    fn next(&mut self) -> Option<Self::Item> {
        let new = (0..self.0).map(|_| rand::random::<f32>()).collect();
        Some(new)
    }
}
