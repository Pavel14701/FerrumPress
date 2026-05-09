use async_trait::async_trait;
use ferrumpress_core::traits::CacheProvider;
use ferrumpress_core::traits::CacheOptions;
use ferrumpress_core::error::CacheError;

pub struct NoopCache;

#[async_trait]
impl CacheProvider for NoopCache {
    async fn get(&self, _key: &str) -> Result<Option<Vec<u8>>, CacheError> {
        Ok(None)
    }

    async fn set(&self, _key: &str, _value: Vec<u8>, _options: CacheOptions) -> Result<(), CacheError> {
        Ok(())
    }

    async fn delete(&self, _key: &str) -> Result<(), CacheError> {
        Ok(())
    }

    async fn invalidate_by_tag(&self, _tag: &str) -> Result<u64, CacheError> {
        Ok(0)
    }

    async fn clear(&self) -> Result<(), CacheError> {
        Ok(())
    }
}