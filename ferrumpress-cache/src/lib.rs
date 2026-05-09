pub mod noop;
pub mod memory;
#[cfg(feature = "redis-backend")]
pub mod redis_cache;

pub use noop::NoopCache;
pub use memory::MemoryCache;
#[cfg(feature = "redis-backend")]
pub use redis_cache::RedisCache;

use ferrumpress_core::traits::CacheProvider;
use ferrumpress_core::config::{CacheConfig, CacheBackend};
use std::sync::Arc;

pub fn create_cache_provider(config: &CacheConfig) -> Arc<dyn CacheProvider> {
    match &config.backend {
        CacheBackend::Noop => Arc::new(NoopCache),
        CacheBackend::Memory => Arc::new(MemoryCache::new(10000)),
        CacheBackend::Redis { url } => {
            #[cfg(feature = "redis-backend")]
            {
                Arc::new(RedisCache::new(url).expect("Failed to create Redis cache"))
            }
            #[cfg(not(feature = "redis-backend"))]
            {
                let _ = url;
                panic!("Redis backend is not enabled (feature 'redis-backend' not set)");
            }
        }
    }
}