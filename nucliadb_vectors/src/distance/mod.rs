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




/*
    WARNING:
    The code in this module was not developed by Bosutech XXI S.L.
    It was derived the 29th of April 2022 from the following Apache licensed project:
    https://github.com/qdrant/qdrant
*/


mod cosine_distance;

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
mod simple_sse;

#[cfg(target_arch = "x86_64")]
mod simple_avx;

#[cfg(target_arch = "aarch64")]
mod simple_neon;

pub use crate::distance::cosine_distance::cosine_distance;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distance::cosine_distance::cosine_distance;

    #[test]
    fn test_cosine_distance() {
        fn power_sqrt(v: &Vec<f32>) -> f32 {
            f32::sqrt(v.iter().cloned().fold(0.0, |acc, x| acc + (x * x)))
        }

        let a = vec![1.0, 2.0, 3.0];
        let b = vec![4.0, 2.0, 6.0];

        assert_eq!(power_sqrt(&a), f32::sqrt(1.0 * 1.0 + 2.0 * 2.0 + 3.0 * 3.0));

        let distance = (1.0 * 4.0 + 2.0 * 2.0 + 3.0 * 6.0) / (power_sqrt(&a) * power_sqrt(&b));
        assert_eq!(distance, cosine_distance(&a, &b));
    }
}
