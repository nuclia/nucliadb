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

//! Shard provider using an unbounded cache.
//!
//! It can contain all shard readers/writers from its index node, so it could be
//! memory expensive. However, it's an easy implementation that speeds up
//! operations.
//!
//! For faster reads at cost of slower initialization and memory consumption,
//! all shards can be loaded at initialization time.

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

use super::resource_cache::{CacheResult, ResourceCache, ResourceLoadGuard};
use nucliadb_core::{node_error, NodeResult};
use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;

/// This cache allows the user to block entities, ensuring that they will not be loaded from disk.
/// Being able to do so is crucial, otherwise the only source of truth will be disk and that would
/// not be thread-safe.
pub struct Cache<Entity> {
    blocked_entities: HashSet<PathBuf>,
    active_entities: ResourceCache<PathBuf, Entity>,
}

impl<Entity> Cache<Entity> {
    pub fn new(capacity: Option<NonZeroUsize>) -> Cache<Entity> {
        let active_entities = match capacity {
            Some(capacity) => ResourceCache::new_with_capacity(capacity),
            None => ResourceCache::new_unbounded(),
        };
        Self {
            active_entities,
            blocked_entities: HashSet::new(),
        }
    }

    pub fn peek(&mut self, id: &PathBuf) -> Option<Arc<Entity>> {
        if self.blocked_entities.contains(id) {
            return None;
        }

        self.active_entities.get_cached(id)
    }

    pub fn get(&mut self, path: &PathBuf) -> NodeResult<CacheResult<PathBuf, Entity>> {
        if self.blocked_entities.contains(path) {
            return Err(node_error!("Entity {path:?} is not on disk"));
        }

        Ok(self.active_entities.get(path))
    }

    pub fn loaded(&mut self, guard: ResourceLoadGuard<PathBuf>, entity: Arc<Entity>) -> NodeResult<()> {
        let entity_key = guard.key();
        if self.blocked_entities.contains(entity_key) {
            return Err(node_error!("Entity {entity_key:?} is not on disk"));
        }

        self.active_entities.loaded(guard, &entity);
        Ok(())
    }

    pub fn set_being_deleted(&mut self, path: PathBuf) {
        self.blocked_entities.insert(path);
    }

    pub fn remove(&mut self, path: &PathBuf) {
        self.blocked_entities.remove(path);
        self.active_entities.remove(path);
    }
}
