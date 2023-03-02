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

use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

/// Indicates that a type might be indexable.
pub trait Indexable {
    type Key;

    /// Returns an iterator over all the indexable keys for the current block of data.
    fn keys<'a>(&'a self) -> Box<dyn Iterator<Item = &Self::Key> + 'a>;
}

/// An handle over a reversed index.
pub enum IndexHandle<T: Indexable> {
    /// Indicates that the handle points to an empty index.
    Empty,
    /// Indicates that the handle points to some indexed block.
    Block(Arc<T>),
}

impl<T: Indexable> IndexHandle<T> {
    /// Indicates if the current block of data contains the given key.
    pub fn contains<U>(&self, key: &U) -> bool
    where
        T::Key: Borrow<U>,
        U: PartialEq + Eq + ?Sized,
    {
        match self {
            Self::Empty => false,
            Self::Block(data) => data.keys().any(|other_key| other_key.borrow() == key),
        }
    }
}

/// Represents a reversed index over any kind of indexable data.
#[derive(Debug)]
pub struct ReversedIndex<T: Indexable> {
    // clippy doesn't know that this type is self-referential.
    #[allow(dead_code)]
    blocks: Vec<Arc<T>>,
    inner: HashMap<T::Key, Arc<T>>,
}

impl<T: Indexable> ReversedIndex<T> {
    /// Creates a new reverse index.
    pub fn new(data: Vec<T>) -> Self
    where
        T::Key: Hash + Eq + Clone,
    {
        let (inner, blocks): (Vec<_>, Vec<_>) = data
            .into_iter()
            .map(|block| {
                let block = Arc::new(block);

                (
                    block
                        .keys()
                        .map(|key| (key.clone(), Arc::clone(&block)))
                        .collect::<HashMap<_, _>>(),
                    block,
                )
            })
            .unzip();

        Self {
            blocks,
            inner: inner.into_iter().flatten().collect(),
        }
    }

    /// Gets a handle to the reverse index for the given key.
    pub fn get<U>(&self, key: &U) -> IndexHandle<T>
    where
        T::Key: Borrow<U> + Hash + Eq,
        U: Hash + Eq + ?Sized,
    {
        self.inner.get(key).map_or(IndexHandle::Empty, |replicas| {
            IndexHandle::Block(Arc::clone(replicas))
        })
    }
}
