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

use crate::memory_system::elements::Node;
use crate::utils::StandardElem;

pub fn select_neighbours_heuristic(
    k_neighbours: usize,
    mut candidates: Vec<(Node, f32)>,
) -> Vec<(Node, f32)> {
    candidates.sort_unstable_by_key(|(n, d)| std::cmp::Reverse(StandardElem(*n, *d)));
    candidates.truncate(k_neighbours);
    candidates
}

#[cfg(test)]
mod test_heuristic_simple {
    use crate::memory_system::elements::{FileSegment, Node};
    #[test]
    fn test_heuristic_simple_search() {
        let mut solution = Vec::with_capacity(100);
        let mut candidates = Vec::with_capacity(100);
        let node = Node {
            vector: FileSegment { start: 0, end: 0 },
        };
        for _ in 0..100 {
            let v = rand::random::<f32>();
            solution.push(v);
            candidates.push((node, v));
        }
        solution.sort_by(|a, b| b.partial_cmp(a).unwrap());
        solution.resize(50, 0.0);
        let result = super::select_neighbours_heuristic(50, candidates);
        let result: Vec<_> = result.iter().map(|(_, d)| *d).collect();
        assert_eq!(result, solution)
    }
}
