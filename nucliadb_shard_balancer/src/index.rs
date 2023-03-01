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
    /// The indexed type.
    type Item;
}

/// An handle over a reversed index.
pub enum IndexHandle<T> {
    /// Indicates that the handle points to an empty index.
    Empty,
    /// Indicates that the handle points to some indexed replicas.
    Replicas(Arc<Vec<T>>),
}

impl<T> IndexHandle<T> {
    /// Indicates if the given value is one the of indexed replicas.
    pub fn is_replica_of<U>(&self, value: &U) -> bool
    where
        T: Borrow<U>,
        U: PartialEq + Eq + ?Sized,
    {
        match self {
            Self::Empty => false,
            Self::Replicas(replicas) => replicas.iter().any(|replica| replica.borrow() == value),
        }
    }
}

/// Represents a reversed index over any kind of indexable type.
///
/// Note that this type allows to easily know the replicas of any given value.
#[derive(Debug)]
pub struct ReversedIndex<T: Indexable> {
    // clippy doesn't know that this type is self-referential.
    #[allow(dead_code)]
    replicas: Vec<Arc<Vec<T::Item>>>,
    inner: HashMap<T::Item, Arc<Vec<T::Item>>>,
}

impl<T: Indexable> ReversedIndex<T> {
    /// Creates a new reverse index.
    pub fn new(replicas: Vec<Vec<T::Item>>) -> Self
    where T::Item: Clone + Hash + Eq {
        let (inner, replicas): (Vec<_>, Vec<_>) = replicas
            .into_iter()
            .map(|replicas| {
                let replicas = Arc::new(replicas);

                (
                    replicas
                        .iter()
                        .map(|replica| (replica.clone(), Arc::clone(&replicas)))
                        .collect::<HashMap<_, _>>(),
                    replicas,
                )
            })
            .unzip();

        Self {
            replicas,
            inner: inner.into_iter().flatten().collect(),
        }
    }

    /// Gets a handle to the reverse index for the given key.
    pub fn get<U>(&self, key: &U) -> IndexHandle<T::Item>
    where
        T::Item: Borrow<U> + Hash + Eq,
        U: Hash + Eq + ?Sized,
    {
        self.inner.get(key).map_or(IndexHandle::Empty, |replicas| {
            IndexHandle::Replicas(Arc::clone(replicas))
        })
    }
}
