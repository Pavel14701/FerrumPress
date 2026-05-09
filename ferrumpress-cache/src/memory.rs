use async_trait::async_trait;
use ferrumpress_core::traits::CacheProvider;
use ferrumpress_core::traits::CacheOptions;
use ferrumpress_core::error::CacheError;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use std::time::Instant;

struct CacheEntry {
    value: Vec<u8>,
    expires_at: Option<Instant>,
    tags: Vec<String>,
}

pub struct MemoryCache {
    store: Arc<Mutex<HashMap<String, CacheEntry>>>,
    max_entries: usize,
}

impl MemoryCache {
    pub fn new(max_entries: usize) -> Self {
        Self {
            store: Arc::new(Mutex::new(HashMap::new())),
            max_entries,
        }
    }

    /// Периодическая очистка истёкших записей (вызывать по таймеру)
    pub async fn evict_expired(&self) {
        let mut store = self.store.lock().await;
        store.retain(|_, entry| {
            if let Some(exp) = entry.expires_at {
                Instant::now() <= exp
            } else {
                true
            }
        });
    }
}

#[async_trait]
impl CacheProvider for MemoryCache {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, CacheError> {
        let store = self.store.lock().await;
        if let Some(entry) = store.get(key) {
            // Атомарная проверка TTL внутри той же блокировки
            if let Some(exp) = entry.expires_at {
                if Instant::now() > exp {
                    return Ok(None); // можно сразу удалить, но удаление оставим для очистки
                }
            }
            return Ok(Some(entry.value.clone()));
        }
        Ok(None)
    }

    async fn set(&self, key: &str, value: Vec<u8>, options: CacheOptions) -> Result<(), CacheError> {
        let mut store = self.store.lock().await;

        // Проверка лимита записей
        if store.len() >= self.max_entries {
            // Простая стратегия: удаляем случайную запись (или первую попавшуюся)
            if let Some(key_to_remove) = store.keys().next().cloned() {
                store.remove(&key_to_remove);
            }
        }

        let expires_at = options.ttl.map(|ttl| Instant::now() + ttl);
        let entry = CacheEntry {
            value,
            expires_at,
            tags: options.tags,
        };
        store.insert(key.to_string(), entry);
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), CacheError> {
        self.store.lock().await.remove(key);
        Ok(())
    }

    async fn invalidate_by_tag(&self, tag: &str) -> Result<u64, CacheError> {
        let mut store = self.store.lock().await;
        let mut count = 0u64;
        store.retain(|_, entry| {
            if entry.tags.contains(&tag.to_string()) {
                count += 1;
                false
            } else {
                true
            }
        });
        Ok(count)
    }

    async fn clear(&self) -> Result<(), CacheError> {
        self.store.lock().await.clear();
        Ok(())
    }
}