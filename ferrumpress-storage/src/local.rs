use async_trait::async_trait;
use ferrumpress_core::traits::StorageBackend;
use ferrumpress_core::error::StorageError;
use std::path::PathBuf;

pub struct LocalStorageBackend {
    base_path: PathBuf,
}

impl LocalStorageBackend {
    pub fn new(base_path: &str) -> Self {
        Self { base_path: PathBuf::from(base_path) }
    }
}

#[async_trait]
impl StorageBackend for LocalStorageBackend {
    async fn put(&self, key: &str, data: Vec<u8>, _content_type: &str) -> Result<(), StorageError> {
        let path = self.base_path.join(key);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| StorageError::UploadFailed(e.to_string()))?;
        }
        tokio::fs::write(&path, data)
            .await
            .map_err(|e| StorageError::UploadFailed(e.to_string()))
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>, StorageError> {
        tokio::fs::read(self.base_path.join(key))
            .await
            .map_err(|e| StorageError::NotFound(e.to_string()))
    }

    async fn delete(&self, key: &str) -> Result<(), StorageError> {
        tokio::fs::remove_file(self.base_path.join(key))
            .await
            .map_err(|e| StorageError::NotFound(e.to_string()))
    }

    fn public_url(&self, key: &str) -> Option<String> {
        Some(format!("/uploads/{}", key))
    }

    fn strategy_name(&self) -> &str { "local" }
}