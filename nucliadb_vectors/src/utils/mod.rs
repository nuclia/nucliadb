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

use crate::memory_system::elements::Node;

#[derive(Clone, Copy)]
pub struct StandardElem<D>(pub Node, pub D);
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
