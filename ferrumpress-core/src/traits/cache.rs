use async_trait::async_trait;
use std::time::Duration;
use crate::error::CacheError;

#[derive(Debug, Clone)]
pub struct CacheOptions {
    pub ttl: Option<Duration>,
    pub tags: Vec<String>,
}

impl Default for CacheOptions {
    fn default() -> Self {
        Self {
            ttl: Some(Duration::from_secs(300)),
            tags: Vec::new(),
        }
    }
}

#[async_trait]
pub trait CacheProvider: Send + Sync {
    /// Получить значение по ключу
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, CacheError>;

    /// Сохранить значение с опциями
    async fn set(&self, key: &str, value: Vec<u8>, options: CacheOptions) -> Result<(), CacheError>;

    /// Удалить ключ
    async fn delete(&self, key: &str) -> Result<(), CacheError>;

    /// Инвалидировать все ключи, связанные с тегом
    async fn invalidate_by_tag(&self, tag: &str) -> Result<u64, CacheError>;

    /// Очистить весь кэш (если поддерживается)
    async fn clear(&self) -> Result<(), CacheError>;
}