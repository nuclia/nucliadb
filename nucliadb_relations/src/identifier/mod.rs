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

use std::marker::PhantomData;

use nucliadb_byte_rpr::*;

use crate::node::*;
pub type ResourceID = ID<ResourceData>;
pub type EntityID = ID<EntityData>;
pub type LabelID = ID<LabelData>;
pub type ColaboratorID = ID<ColabData>;

pub struct ID<T> {
    pub raw: u128,
    points_to: PhantomData<T>,
}

impl<T> std::fmt::Debug for ID<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ID({})", self.raw)
    }
}

impl<T> std::fmt::Display for ID<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.raw)
    }
}

impl<T> From<&str> for ID<T> {
    fn from(raw: &str) -> Self {
        ID {
            raw: raw.parse::<u128>().unwrap(),
            points_to: PhantomData,
        }
    }
}

impl<T> Default for ID<T> {
    fn default() -> Self {
        Self::new()
    }
}
impl<T> ID<T> {
    pub fn new() -> ID<T> {
        ID {
            raw: 0,
            points_to: PhantomData,
        }
    }
    pub fn next(&mut self) -> ID<T> {
        let crnt = *self;
        self.raw += 1;
        crnt
    }
}
impl<T> Eq for ID<T> {}

impl<T> Copy for ID<T> {}
impl<T> Clone for ID<T> {
    fn clone(&self) -> Self {
        Self {
            raw: self.raw,
            points_to: self.points_to,
        }
    }
}
impl<T> PartialEq for ID<T> {
    fn eq(&self, other: &Self) -> bool {
        self.raw.eq(&other.raw)
    }
}
impl<T> PartialOrd for ID<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.raw.partial_cmp(&other.raw)
    }
}
impl<T> Ord for ID<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.raw.cmp(&other.raw)
    }
}
impl<T> std::hash::Hash for ID<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.raw.hash(state)
    }
}

impl<T> ByteRpr for ID<T> {
    fn as_byte_rpr(&self) -> Vec<u8> {
        self.raw.as_byte_rpr()
    }

    fn from_byte_rpr(bytes: &[u8]) -> Self {
        let raw_start = 0;
        let raw_end = raw_start + u128::segment_len();
        ID {
            raw: u128::from_byte_rpr(&bytes[raw_start..raw_end]),
            points_to: PhantomData,
        }
    }
}

impl<T> FixedByteLen for ID<T> {
    fn segment_len() -> usize {
        u128::segment_len()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn id_serialization_generation() {
        let mut id_gen: ID<String> = ID::new();
        let id_0 = id_gen.next();
        let id_1 = id_gen.next();
        assert_ne!(id_0, id_1);
        assert_eq!(id_0, ID::from_byte_rpr(&id_0.as_byte_rpr()));
        assert_eq!(id_1, ID::from_byte_rpr(&id_1.as_byte_rpr()));
    }
}
