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

use std::collections::HashMap;
use std::hash::Hash;
use std::num::NonZeroUsize;
use std::sync::{Arc, Weak};
use std::time::Instant;

use lru::LruCache;
use nidx_paragraph::ParagraphSearcher;
use nidx_relation::RelationSearcher;
use nidx_text::TextSearcher;
use nidx_types::{OpenIndexMetadata, SegmentMetadata, Seq};
use nidx_vector::VectorSearcher;
use serde::Deserialize;
use tokio::sync::{Mutex, Semaphore};
use uuid::Uuid;

use crate::NidxMetadata;
use crate::errors::{NidxError, NidxResult};
use crate::metadata::{IndexId, IndexKind, Segment, SegmentId};
use crate::metrics::IndexKindLabels;
use crate::metrics::searcher::*;

use super::sync::{Operations, ShardIndexes, SyncMetadata};

pub enum IndexSearcher {
    Paragraph(ParagraphSearcher),
    Relation(RelationSearcher),
    Text(TextSearcher),
    Vector(VectorSearcher),
}

impl<'a> From<&'a IndexSearcher> for &'a VectorSearcher {
    fn from(value: &'a IndexSearcher) -> Self {
        match value {
            IndexSearcher::Vector(v) => v,
            _ => unreachable!(),
        }
    }
}

impl<'a> From<&'a IndexSearcher> for &'a TextSearcher {
    fn from(value: &'a IndexSearcher) -> Self {
        match value {
            IndexSearcher::Text(v) => v,
            _ => unreachable!(),
        }
    }
}

impl<'a> From<&'a IndexSearcher> for &'a ParagraphSearcher {
    fn from(value: &'a IndexSearcher) -> Self {
        match value {
            IndexSearcher::Paragraph(v) => v,
            _ => unreachable!(),
        }
    }
}

impl<'a> From<&'a IndexSearcher> for &'a RelationSearcher {
    fn from(value: &'a IndexSearcher) -> Self {
        match value {
            IndexSearcher::Relation(v) => v,
            _ => unreachable!(),
        }
    }
}

impl MemoryUsage for IndexSearcher {
    fn memory_usage(&self) -> usize {
        match self {
            IndexSearcher::Paragraph(paragraph_searcher) => paragraph_searcher.space_usage(),
            IndexSearcher::Relation(relation_searcher) => relation_searcher.space_usage(),
            IndexSearcher::Text(text_searcher) => text_searcher.space_usage(),
            IndexSearcher::Vector(vector_searcher) => vector_searcher.space_usage(),
        }
    }
}

/// This structure (its trait) is passed to the indexes in order to open a searcher.
/// This implementation takes the data in the format available to a searcher node,
/// that is, what is stored in SyncMetadata, a list of `Operations` and a map of
/// extended segment metadata.
struct IndexOperations {
    sync_metadata: Arc<SyncMetadata>,
    segments: HashMap<SegmentId, Segment>,
    operations: Operations,
    index_id: IndexId,
}

impl<T: for<'de> Deserialize<'de>> OpenIndexMetadata<T> for IndexOperations {
    fn segments(&self) -> impl Iterator<Item = (SegmentMetadata<T>, Seq)> {
        self.operations.0.iter().flat_map(|op| {
            op.segment_ids.iter().map(|segment_id| {
                let location = self.sync_metadata.segment_location(&self.index_id, segment_id);
                (self.segments[segment_id].metadata(location), op.seq)
            })
        })
    }

    fn deletions(&self) -> impl Iterator<Item = (&String, Seq)> {
        self.operations
            .0
            .iter()
            .flat_map(|op| op.deleted_keys.iter().map(|key| (key, op.seq)))
    }
}

pub struct IndexCache {
    cache: Mutex<ResourceCache<IndexId, IndexSearcher>>,
    sync_metadata: Arc<SyncMetadata>,
    metadb: NidxMetadata,
}

impl IndexCache {
    pub fn new(metadata: Arc<SyncMetadata>, metadb: NidxMetadata) -> Self {
        IndexCache {
            cache: ResourceCache::new_unbounded().into(),
            sync_metadata: metadata,
            metadb,
        }
    }

    pub async fn get_shard_indexes(&self, shard_id: &Uuid) -> Option<ShardIndexes> {
        self.sync_metadata.get_shard_indexes(shard_id).await
    }

    pub async fn get(&self, id: &IndexId) -> anyhow::Result<Arc<IndexSearcher>> {
        loop {
            let cached = { self.cache.lock().await.get(id) };
            match cached {
                CacheResult::Cached(shard) => return Ok(shard),
                CacheResult::Wait(waiter) => waiter.wait().await,
                CacheResult::Load(guard) => {
                    let loaded = self.load(id).await?;
                    self.cache.lock().await.loaded(guard, &loaded);
                    return Ok(loaded);
                }
            }
        }
    }

    /// Reload an index, returns true if it needs to be retried later
    pub async fn reload(&self, id: &IndexId) -> anyhow::Result<bool> {
        let cached = { self.cache.lock().await.peek(id) };
        match cached {
            CachePeekResult::Cached(_arc) => {
                match self.load(id).await {
                    Ok(new_searcher) => {
                        self.cache.lock().await.insert(id, &new_searcher);
                        Ok(false)
                    }
                    Err(NidxError::NotFound) => {
                        // Index not found, just take it out of the cache
                        self.cache.lock().await.remove(id);
                        Ok(false)
                    }
                    Err(e) => Err(e.into()),
                }
            }
            CachePeekResult::Loading => Ok(true),
            CachePeekResult::NotPresent => Ok(false),
        }
    }

    pub async fn remove(&self, id: &IndexId) {
        self.cache.lock().await.remove(id);
    }

    pub async fn load(&self, id: &IndexId) -> NidxResult<Arc<IndexSearcher>> {
        let t = Instant::now();
        let read = self.sync_metadata.get(id).await;
        let read2 = read.get().await;
        let Some(meta) = read2 else {
            return Err(crate::errors::NidxError::NotFound);
        };

        let segments = Segment::select_many(&self.metadb.pool, &meta.operations.segments().collect::<Vec<_>>())
            .await?
            .into_iter()
            .map(|s| (s.id, s))
            .collect::<HashMap<SegmentId, Segment>>();

        let open_index = IndexOperations {
            segments,
            operations: meta.operations.clone(),
            sync_metadata: self.sync_metadata.clone(),
            index_id: *id,
        };

        let searcher = match meta.index.kind {
            IndexKind::Text => IndexSearcher::Text(TextSearcher::open(meta.index.config()?, open_index)?),
            IndexKind::Paragraph => IndexSearcher::Paragraph(ParagraphSearcher::open(open_index)?),
            IndexKind::Vector => IndexSearcher::Vector(VectorSearcher::open(meta.index.config()?, open_index)?),
            IndexKind::Relation => IndexSearcher::Relation(RelationSearcher::open(meta.index.config()?, open_index)?),
        };
        INDEX_LOAD_TIME
            .get_or_create(&IndexKindLabels::new(meta.index.kind))
            .observe(t.elapsed().as_secs_f64());

        Ok(Arc::new(searcher))
    }
}

// Used to track when an entry is being loaded. Once this is dropped,
// we know the entry finished loading or failed to do so, and we can
// unlock other clients waiting for it.
struct ResourceLoadGuard<K> {
    waiter: Arc<Semaphore>,
    key: K,
}

impl<K> Drop for ResourceLoadGuard<K> {
    fn drop(&mut self) {
        self.waiter.close();
    }
}

// Use to wait until an entry is ready to be used
struct ResourceWaiter(Arc<Semaphore>);
impl ResourceWaiter {
    pub async fn wait(self) {
        let _ = self.0.acquire().await;
    }
}

enum CacheResult<K, V> {
    Cached(Arc<V>),
    Load(ResourceLoadGuard<K>),
    Wait(ResourceWaiter),
}

enum CachePeekResult<V> {
    Cached(Arc<V>),
    Loading,
    NotPresent,
}

trait MemoryUsage {
    fn memory_usage(&self) -> usize;
}

struct ResourceCache<K, V: MemoryUsage> {
    live: LruCache<K, Arc<V>>,
    eviction: HashMap<K, Weak<V>>,
    capacity: Option<NonZeroUsize>,
    loading: HashMap<K, Arc<Semaphore>>,
}

impl<K, V> ResourceCache<K, V>
where
    K: Eq + Hash + Clone + std::fmt::Debug,
    V: MemoryUsage,
{
    #[allow(dead_code)]
    pub fn new_with_capacity(capacity: NonZeroUsize) -> Self {
        ResourceCache {
            capacity: Some(capacity),
            live: LruCache::unbounded(),
            eviction: HashMap::new(),
            loading: HashMap::new(),
        }
    }

    pub fn new_unbounded() -> Self {
        ResourceCache {
            capacity: None,
            live: LruCache::unbounded(),
            eviction: HashMap::new(),
            loading: HashMap::new(),
        }
    }

    /// Try to get an entry from the cache
    /// 1. If it's present, we return Cached(entry). Consumer can use it.
    /// 2. If it's not in the cache, we return Load(guard). Consumer should load it and call
    ///    cache.loaded with the guard.
    /// 3. If it's being loaded concurrently, we return Wait(waiter). Consumer should wait using the
    ///    waiter and then retry the get.
    pub fn get(&mut self, id: &K) -> CacheResult<K, V> {
        if let Some(v) = self.get_cached(id) {
            return CacheResult::Cached(v);
        }

        if let Some(wait) = self.loading.get(id) {
            if wait.is_closed() {
                // The loading process finished but is still in the loading tree
                // This happens when the guard was dropped without saving an object
                // This is an error loading, we can retry by returning a load object
                self.loading.remove(id);
            } else {
                return CacheResult::Wait(ResourceWaiter(wait.clone()));
            }
        }

        let waiter = Arc::new(Semaphore::new(0));
        self.loading.insert(id.clone(), waiter.clone());
        CacheResult::Load(ResourceLoadGuard {
            waiter,
            key: id.clone(),
        })
    }

    /// Check if an entry is present in the cache
    pub fn peek(&mut self, id: &K) -> CachePeekResult<V> {
        if let Some(v) = self.get_cached(id) {
            CachePeekResult::Cached(v)
        } else if self.loading.contains_key(id) {
            CachePeekResult::Loading
        } else {
            CachePeekResult::NotPresent
        }
    }

    pub fn loaded(&mut self, guard: ResourceLoadGuard<K>, v: &Arc<V>) {
        self.loading.remove(&guard.key);
        self.insert(&guard.key, v);
        drop(guard);
    }

    pub fn remove(&mut self, k: &K) {
        if let Some(v) = self.live.pop(k) {
            INDEX_CACHE_COUNT.dec();
            INDEX_CACHE_BYTES.dec_by(v.memory_usage() as i64);
        }
    }

    pub fn get_cached(&mut self, id: &K) -> Option<Arc<V>> {
        if let Some(v) = self.eviction.get(id).and_then(Weak::upgrade) {
            self.insert(id, &v);
            return Some(v);
        }
        self.live.get(id).cloned()
    }

    pub fn insert(&mut self, k: &K, v: &Arc<V>) {
        if self.live.len() >= self.capacity.unwrap_or(NonZeroUsize::MAX).into() && !self.live.contains(k) {
            self.evict();
        }
        INDEX_CACHE_COUNT.inc();
        INDEX_CACHE_BYTES.inc_by(v.memory_usage() as i64);
        if let Some((_, out)) = self.live.push(k.clone(), Arc::clone(v)) {
            // The previous condition ensures this only happens if updating an existing key with a new value
            INDEX_CACHE_COUNT.dec();
            INDEX_CACHE_BYTES.dec_by(out.memory_usage() as i64);
        }
    }

    fn evict(&mut self) {
        if let Some((evicted_k, evicted_v)) = self.live.pop_lru() {
            INDEX_CACHE_COUNT.dec();
            INDEX_CACHE_BYTES.dec_by(evicted_v.memory_usage() as i64);
            self.eviction.insert(evicted_k, Arc::downgrade(&evicted_v));
        }
    }
}

#[cfg(test)]
mod tests {
    mod resource_cache {
        use std::num::NonZeroUsize;
        use std::sync::atomic::AtomicU8;
        use std::sync::mpsc::channel;
        use std::sync::{Arc, Mutex};
        use std::time::Duration;

        use anyhow::anyhow;
        use rand::Rng;
        use tokio::task::JoinSet;

        use crate::searcher::index_cache::MemoryUsage;

        use super::super::{CacheResult, ResourceCache};

        static OBJCOUNTER: [AtomicU8; 8] = [
            AtomicU8::new(0),
            AtomicU8::new(0),
            AtomicU8::new(0),
            AtomicU8::new(0),
            AtomicU8::new(0),
            AtomicU8::new(0),
            AtomicU8::new(0),
            AtomicU8::new(0),
        ];

        #[derive(Debug)]
        struct CacheItem(usize);

        impl CacheItem {
            async fn new(k: usize) -> anyhow::Result<Self> {
                tokio::time::sleep(Duration::from_millis(50)).await;
                if rand::thread_rng().gen_ratio(1, 10) {
                    return Err(anyhow!("patata"));
                }
                let old = OBJCOUNTER[k].fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if old >= 1 {
                    panic!("A resource was opened more than once simultaneously");
                }
                Ok(Self(k))
            }
        }

        impl MemoryUsage for CacheItem {
            fn memory_usage(&self) -> usize {
                123
            }
        }

        impl MemoryUsage for usize {
            fn memory_usage(&self) -> usize {
                8
            }
        }

        impl Drop for CacheItem {
            fn drop(&mut self) {
                OBJCOUNTER[self.0].fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            }
        }

        #[tokio::test]
        async fn test_cache_rng() {
            let cache: ResourceCache<usize, CacheItem> =
                ResourceCache::new_with_capacity(NonZeroUsize::new(3).unwrap());
            let cache = Arc::new(Mutex::new(cache));

            let mut tasks = JoinSet::new();
            for _ in 0..16 {
                let cc = cache.clone();
                tasks.spawn(async move {
                    for _i in 0..50 {
                        // Wait for next request
                        let t = Duration::from_millis(rand::thread_rng().gen_range(0..5));
                        tokio::time::sleep(t).await;

                        // Get a shard
                        let k = rand::thread_rng().gen_range(0..8);
                        let shard = {
                            loop {
                                let cached = { cc.lock().unwrap().get(&k) };
                                match cached {
                                    CacheResult::Cached(shard) => break shard,
                                    CacheResult::Wait(waiter) => waiter.wait().await,
                                    CacheResult::Load(guard) => {
                                        if let Ok(loaded) = CacheItem::new(k).await {
                                            let loaded = Arc::new(loaded);
                                            cc.lock().unwrap().loaded(guard, &loaded);
                                            break loaded;
                                        }
                                    }
                                }
                            }
                        };

                        // Do something
                        let t = Duration::from_millis(rand::thread_rng().gen_range(1..20));
                        tokio::time::sleep(t).await;
                        drop(shard);
                    }
                });
            }
            tasks.join_all().await;
        }

        #[test]
        fn test_lru() {
            let mut cache: ResourceCache<usize, usize> =
                ResourceCache::new_with_capacity(NonZeroUsize::new(2).unwrap());

            let items = [Arc::new(0), Arc::new(1), Arc::new(2), Arc::new(3)];

            // Keeps the latest inserted ones
            cache.insert(&0, &items[0]);
            cache.insert(&1, &items[1]);
            cache.insert(&2, &items[2]);
            cache.insert(&3, &items[3]);

            assert!(!cache.live.contains(&0));
            assert!(!cache.live.contains(&1));
            assert!(cache.live.contains(&2));
            assert!(cache.live.contains(&3));

            // Keeps the recently used
            cache.get(&2);
            cache.insert(&0, &items[0]);

            assert!(cache.live.contains(&0));
            assert!(!cache.live.contains(&1));
            assert!(cache.live.contains(&2));
            assert!(!cache.live.contains(&3));
        }

        #[test]
        fn test_eviction() {
            let mut cache: ResourceCache<usize, usize> =
                ResourceCache::new_with_capacity(NonZeroUsize::new(1).unwrap());

            let item0 = Arc::new(0);
            let item1 = Arc::new(1);

            // Fill the cache
            cache.insert(&0, &item0);

            // Insert a new one, 0 is getting evicted
            cache.insert(&1, &item1);

            assert!(!cache.live.contains(&0));
            assert!(cache.eviction.contains_key(&0));

            // 0 should be evicted, but there are still references to it
            // from this test code. Requesting it again should reuse that
            // instance.
            assert!(matches!(cache.get(&0), CacheResult::Cached(_)));
            assert!(cache.live.contains(&0));

            // Currently the cache contains 0. Let's delete the last reference
            // to 1 (from this test) and try to get it, we should be asked to
            // load it, since it'll be out of the cache.
            drop(item1);
            assert!(matches!(cache.get(&1), CacheResult::Load(_)));
        }

        #[tokio::test]
        async fn test_loading() {
            let cache: ResourceCache<usize, usize> = ResourceCache::new_unbounded();
            let cache = Arc::new(Mutex::new(cache));

            // Item not in cache, we are asked to load it
            let CacheResult::Load(load_guard) = cache.lock().unwrap().get(&0) else {
                panic!("Expected a CacheResult::Load")
            };

            // If we try to get it from elsewhere, we wait a waiter
            let CacheResult::Wait(waiter) = cache.lock().unwrap().get(&0) else {
                panic!("Expected a CacheResult::Wait")
            };

            // We start two threads to load and wait, we expect the wait to block
            // until the load is complete
            let mut tasks = JoinSet::new();

            let (tx, rx) = channel();
            let tx_clone = tx.clone();
            let cache_clone = cache.clone();
            tasks.spawn(async move {
                waiter.wait().await;
                assert!(matches!(cache_clone.lock().unwrap().get(&0), CacheResult::Cached(_)));
                tx_clone.send(1).unwrap();
            });
            tasks.spawn(async move {
                // Sleep a little bit to ensure the waiter actually waits
                tokio::time::sleep(Duration::from_millis(5)).await;
                let mut unlocked_cache = cache.lock().unwrap();
                tx.send(0).unwrap();
                unlocked_cache.loaded(load_guard, &Arc::new(0));
            });

            // Both threads finished without panic/failing assert
            tasks.join_all().await;

            // Load thread stores before wait thread waken up
            assert_eq!(rx.recv().unwrap(), 0);
            assert_eq!(rx.recv().unwrap(), 1);
        }

        #[tokio::test]
        async fn test_loading_failed() {
            let cache: ResourceCache<usize, usize> = ResourceCache::new_unbounded();
            let cache = Arc::new(Mutex::new(cache));

            // Item not in cache, we are asked to load it
            let CacheResult::Load(load_guard) = cache.lock().unwrap().get(&0) else {
                panic!("Expected a CacheResult::Load")
            };

            // If we try to get it from elsewhere, we wait a waiter
            let CacheResult::Wait(waiter) = cache.lock().unwrap().get(&0) else {
                panic!("Expected a CacheResult::Wait")
            };

            // We start two threads to load and wait, we expect the wait to block
            // until the load is complete
            let mut tasks = JoinSet::new();

            let (tx, rx) = channel();
            let tx_clone = tx.clone();
            let cache_clone = cache.clone();
            tasks.spawn(async move {
                waiter.wait().await;
                // The load will fail, so we expect to be asked
                // to load it ourselves
                assert!(matches!(cache_clone.lock().unwrap().get(&0), CacheResult::Load(_)));
                tx_clone.send(1).unwrap();
            });
            tasks.spawn(async move {
                // Sleep a little bit to ensure the waiter actually waits
                tokio::time::sleep(Duration::from_millis(5)).await;
                // Fail to call `loaded`. This should drop the load_guard
                // which will mark the load as failed.
                tx.send(0).unwrap();
                drop(load_guard);
            });

            // Both threads finished without panic/failing assert
            tasks.join_all().await;

            // Load thread finished earlier
            assert_eq!(rx.recv().unwrap(), 0);
            assert_eq!(rx.recv().unwrap(), 1);
        }
    }

    mod index_cache {
        use std::{path::PathBuf, sync::Arc};

        use sqlx::types::JsonValue;
        use sqlx::types::time::PrimitiveDateTime;
        use uuid::Uuid;

        use crate::{
            NidxMetadata,
            metadata::Index,
            searcher::{
                index_cache::IndexCache,
                sync::{Operations, SyncMetadata},
            },
        };

        #[sqlx::test]
        async fn test_reload(pool: sqlx::PgPool) -> anyhow::Result<()> {
            let metadb = NidxMetadata::new_with_pool(pool).await?;
            let sync_meta = Arc::new(SyncMetadata::new(PathBuf::new()));
            let cache = IndexCache::new(sync_meta.clone(), metadb);

            // Empty index
            let index = Index {
                id: 1i64.into(),
                shard_id: Uuid::new_v4(),
                kind: crate::metadata::IndexKind::Text,
                name: "text".to_string(),
                configuration: JsonValue::Null,
                updated_at: PrimitiveDateTime::MIN,
                deleted_at: None,
            };
            let index_id = index.id;
            let shard_id = index.shard_id;
            let operations = Operations(Vec::new());
            sync_meta.set(index, operations).await;

            // Cache is empty, reloading something that does not exists -> Ok (nothing happened)
            assert!(matches!(cache.reload(&2i64.into()).await, Ok(false)));
            assert!(cache.cache.lock().await.live.is_empty());

            // Cache is empty, reloading something that exists -> Ok (nothing happened)
            assert!(matches!(cache.reload(&index_id).await, Ok(false)));
            assert!(cache.cache.lock().await.live.is_empty());

            // Load the index
            cache.get(&index_id).await?;
            assert_eq!(cache.cache.lock().await.live.len(), 1);

            // Cache has an index, reloading it -> Ok
            assert!(matches!(cache.reload(&index_id).await, Ok(false)));
            assert_eq!(cache.cache.lock().await.live.len(), 1);

            // Index deleted from metadata, reload should remove it from cache
            sync_meta.delete(&shard_id, &index_id).await;
            assert!(matches!(cache.reload(&index_id).await, Ok(false)));
            assert!(cache.cache.lock().await.live.is_empty());

            Ok(())
        }
    }
}
