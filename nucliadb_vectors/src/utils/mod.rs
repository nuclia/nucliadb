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

use std::cmp::Ordering;

use crate::graph_arena::LockArena;
use crate::graph_disk::LockDisk;
use crate::graph_elems::NodeId;

#[derive(Clone, Copy)]
pub struct InverseElem<D>(pub NodeId, pub D);
impl<D> Eq for InverseElem<D> where D: PartialOrd + PartialEq {}
impl<D> Ord for InverseElem<D>
where D: PartialEq + PartialOrd
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}
impl<D> PartialEq for InverseElem<D>
where D: PartialEq
{
    fn eq(&self, other: &Self) -> bool {
        (self.1 == other.1) && (self.0 == other.0)
    }
}

impl<D> PartialOrd for InverseElem<D>
where D: PartialOrd
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.1 > other.1 {
            Some(Ordering::Less)
        } else if self.1 < other.1 {
            Some(Ordering::Greater)
        } else {
            Some(Ordering::Equal)
        }
    }
}
impl<D> From<StandardElem<D>> for InverseElem<D> {
    fn from(StandardElem(n, d): StandardElem<D>) -> Self {
        InverseElem(n, d)
    }
}

#[derive(Clone, Copy)]
pub struct StandardElem<D>(pub NodeId, pub D);
impl<D> Eq for StandardElem<D> where D: PartialOrd + PartialEq {}
impl<D> Ord for StandardElem<D>
where D: PartialEq + PartialOrd
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}
impl<D> PartialEq for StandardElem<D>
where D: PartialEq
{
    fn eq(&self, other: &Self) -> bool {
        (self.1 == other.1) && (self.0 == other.0)
    }
}
impl<D> PartialOrd for StandardElem<D>
where D: PartialOrd
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.1 > other.1 {
            Some(Ordering::Greater)
        } else if self.1 < other.1 {
            Some(Ordering::Less)
        } else {
            Some(Ordering::Equal)
        }
    }
}

impl<D> From<InverseElem<D>> for StandardElem<D> {
    fn from(InverseElem(n, d): InverseElem<D>) -> Self {
        StandardElem(n, d)
    }
}

#[cfg(test)]
mod test_utils {
    use std::collections::{BinaryHeap, LinkedList};

    use super::*;
    #[test]
    #[allow(non_snake_case)]
    fn test_utils__right_order() {
        let mut inverse_heap = BinaryHeap::new();
        let mut standard_heap = BinaryHeap::new();
        let len = 10;
        for i in 0..len {
            inverse_heap.push(InverseElem(NodeId::null(), i as f32));
            standard_heap.push(StandardElem(NodeId::null(), i as f32));
        }
        let mut inverse = LinkedList::new();
        let mut standard = LinkedList::new();
        loop {
            match (inverse_heap.pop(), standard_heap.pop()) {
                (None, None) => break,
                (Some(InverseElem(_, d_i)), Some(StandardElem(_, d_s))) => {
                    inverse.push_back(d_i);
                    standard.push_front(d_s);
                }
                _ => unreachable!(),
            }
        }
        assert_eq!(inverse, standard);
    }
}

pub fn internal_reload_policy(arena: &LockArena, disk: &LockDisk) -> bool {
    let in_disk = disk.no_nodes();
    let in_arena = arena.no_nodes();
    (in_arena as f64 / in_disk as f64) * 100f64 >= 30f64
}
