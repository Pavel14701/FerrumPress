use async_trait::async_trait;
use crate::error::StorageError;

#[async_trait]
pub trait ObjectStore: Send + Sync {
    async fn put_object(&self, key: &str, data: Vec<u8>, content_type: &str) -> Result<(), StorageError>;
    async fn get_object(&self, key: &str) -> Result<Vec<u8>, StorageError>;
    async fn delete_object(&self, key: &str) -> Result<(), StorageError>;
    fn public_url(&self, key: &str) -> String;
}