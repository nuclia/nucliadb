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
use std::sync::{Arc, Condvar, Mutex, Weak};

use lru::LruCache;

#[derive(Default)]
struct Waiter {
    mutex: Mutex<bool>,
    condvar: Condvar,
}

impl Waiter {
    fn wait(&self) {
        let mut ready = self.mutex.lock().unwrap();
        while !*ready {
            ready = self.condvar.wait(ready).unwrap();
        }
    }

    fn notify(&self) {
        let mut ready = self.mutex.lock().unwrap();
        *ready = true;
        self.condvar.notify_all();
    }

    fn finished(&self) -> bool {
        *self.mutex.lock().unwrap()
    }
}

pub struct ResourceLoadGuard<K> {
    waiter: Arc<Waiter>,
    key: K,
}

impl<K> Drop for ResourceLoadGuard<K> {
    fn drop(&mut self) {
        self.waiter.notify();
    }
}

pub struct ResourceWaitGuard(Arc<Waiter>);
impl ResourceWaitGuard {
    pub fn wait(self) {
        self.0.wait();
    }
}

pub enum CacheResult<K, V> {
    Cached(Arc<V>),
    Load(ResourceLoadGuard<K>),
    Wait(ResourceWaitGuard),
}

pub struct ResourceCache<K, V> {
    live: LruCache<K, Arc<V>>,
    eviction: HashMap<K, Weak<V>>,
    capacity: Option<NonZeroUsize>,
    loading: HashMap<K, Arc<Waiter>>,
}

impl<K, V> ResourceCache<K, V>
where
    K: Eq + Hash + Clone + std::fmt::Debug,
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

    pub fn get(&mut self, id: &K) -> CacheResult<K, V> {
        if let Some(v) = self.get_cached(id) {
            return CacheResult::Cached(v);
        }

        if let Some(wait) = self.loading.get(id) {
            if wait.finished() {
                // The loading process finished but is still in the loading tree
                // This happens when the guard was dropped without saving an object
                // This is an error loading, we can retry by returning a load object
                self.loading.remove(id);
            } else {
                return CacheResult::Wait(ResourceWaitGuard(Arc::clone(wait)));
            }
        }

        let waiter = Arc::new(Waiter::default());
        self.loading.insert(id.clone(), Arc::clone(&waiter));
        CacheResult::Load(ResourceLoadGuard {
            waiter,
            key: id.clone(),
        })
    }

    pub fn loaded(&mut self, guard: ResourceLoadGuard<K>, v: &Arc<V>) {
        self.loading.remove(&guard.key);
        println!("Stored {:?} {:?}", guard.key, std::thread::current().id());
        self.insert(&guard.key, v);
        drop(guard);
    }

    pub fn remove(&mut self, k: &K) {
        self.live.pop(k);
    }

    fn get_cached(&mut self, id: &K) -> Option<Arc<V>> {
        if let Some(v) = self.eviction.get(id).and_then(Weak::upgrade) {
            self.insert(id, &v);
            return Some(v);
        }
        self.live.get(id).cloned()
    }

    pub fn insert(&mut self, k: &K, v: &Arc<V>) {
        if self.live.len() >= self.capacity.unwrap_or(NonZeroUsize::MAX).into() {
            self.evict();
        }
        self.live.push(k.clone(), Arc::clone(v));
    }

    fn evict(&mut self) {
        if let Some((evicted_k, evicted_v)) = self.live.pop_lru() {
            println!("Evict {evicted_k:?}");
            self.eviction.insert(evicted_k, Arc::downgrade(&evicted_v));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::atomic::AtomicU8;
    use std::sync::{Arc, Mutex};
    use std::thread::sleep;
    use std::time::Duration;

    use anyhow::anyhow;
    use crossbeam_utils::thread;
    use nucliadb_core::NodeResult;
    use rand::Rng;

    use super::{CacheResult, ResourceCache};

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

    struct CacheItem(usize);

    impl CacheItem {
        fn new(k: usize) -> NodeResult<Self> {
            sleep(Duration::from_millis(100));
            if rand::thread_rng().gen_ratio(1, 10) {
                println!("Failed to load {k} {:?}", std::thread::current().id());
                return Err(anyhow!("patata"));
            }
            println!("Created {k} {:?}", std::thread::current().id());
            let old = OBJCOUNTER[k].fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if old >= 1 {
                panic!("A resource was opened more than once simultaneously");
            }
            Ok(Self(k))
        }
    }

    impl Drop for CacheItem {
        fn drop(&mut self) {
            println!("Destroyed {} {:?}", self.0, std::thread::current().id());
            OBJCOUNTER[self.0].fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[test]
    pub fn test_cache_rng() {
        let cache: ResourceCache<usize, CacheItem> =
            ResourceCache::new_with_capacity(NonZeroUsize::new(3).unwrap());
        let cache = Arc::new(Mutex::new(cache));

        thread::scope(|s| {
            let mut tasks = vec![];
            for _ in 0..16 {
                let cc = cache.clone();
                tasks.push(s.spawn(move |_| {
                    let mut rng = rand::thread_rng();
                    for _i in 0..100 {
                        // Wait for next request
                        sleep(Duration::from_millis(rng.gen_range(0..5)));

                        // Get a shard
                        let k = rng.gen_range(0..8);
                        let shard = {
                            loop {
                                let cached = { cc.lock().unwrap().get(&k) };
                                match cached {
                                    CacheResult::Cached(shard) => break shard,
                                    CacheResult::Wait(waiter) => waiter.wait(),
                                    CacheResult::Load(guard) => {
                                        if let Ok(loaded) = CacheItem::new(k) {
                                            let loaded = Arc::new(loaded);
                                            cc.lock().unwrap().loaded(guard, &loaded);
                                            break loaded;
                                        }
                                    }
                                }
                            }
                        };

                        // Do something
                        sleep(Duration::from_millis(rng.gen_range(1..20)));
                        drop(shard);
                    }
                }));
            }
            tasks.into_iter().for_each(|t| {
                t.join().unwrap();
            });
        })
        .unwrap();
    }
}
