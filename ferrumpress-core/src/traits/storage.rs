use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use crate::error::StorageError;

/// Физическое хранилище (один бекенд)
#[async_trait]
pub trait StorageBackend: Send + Sync {
    async fn put(&self, key: &str, data: Vec<u8>, content_type: &str) -> Result<(), StorageError>;
    async fn get(&self, key: &str) -> Result<Vec<u8>, StorageError>;
    async fn delete(&self, key: &str) -> Result<(), StorageError>;
    fn public_url(&self, key: &str) -> Option<String>;
    fn strategy_name(&self) -> &str;
}

/// Задача на обработку медиафайла
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessMediaTask {
    pub media_id: Uuid,
}

/// Вариант сконвертированного файла
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImageVariant {
    pub format: String,
    pub key: String,
    pub size: u64,
    pub width: u32,
    pub height: u32,
}