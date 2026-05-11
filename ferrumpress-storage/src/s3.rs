use async_trait::async_trait;
use rust_s3::Bucket;
use rust_s3::Credentials;
use ferrumpress_core::error::StorageError;
use ferrumpress_core::traits::StorageBackend;

pub struct S3Backend {
    bucket: std::sync::Arc<Bucket>,
}

impl S3Backend {
    pub fn new(
        bucket: &str,
        region: &str,
        endpoint: &str,
        access_key: &str,
        secret_key: &str,
    ) -> Result<Self, StorageError> {
        let credentials = Credentials::new(
            Some(access_key),
            Some(secret_key),
            None,
            None,
            None,
        )
        .map_err(|e| StorageError::UploadFailed(e.to_string()))?;

        let bucket_obj = Bucket::new(
            bucket,
            rust_s3::Region::Custom {
                region: region.to_string(),
                endpoint: endpoint.to_string(),
            },
            credentials,
        )
        .map_err(|e| StorageError::UploadFailed(e.to_string()))?;

        Ok(Self {
            bucket: std::sync::Arc::new(bucket_obj),
        })
    }
}

#[async_trait]
impl StorageBackend for S3Backend {
    async fn put(&self, key: &str, data: Vec<u8>, content_type: &str) -> Result<(), StorageError> {
        let response = self.bucket
            .put_object_with_content_type(key, &data, content_type)
            .await
            .map_err(|e| StorageError::UploadFailed(e.to_string()))?;

        if response.status_code() != 200 {
            return Err(StorageError::UploadFailed(format!(
                "PUT failed with status: {}",
                response.status_code()
            )));
        }
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>, StorageError> {
        let response = self.bucket
            .get_object(key)
            .await
            .map_err(|e| StorageError::NotFound(e.to_string()))?;

        if response.status_code() != 200 {
            return Err(StorageError::NotFound(format!(
                "GET failed with status: {}",
                response.status_code()
            )));
        }
        Ok(response.to_vec())
    }

    async fn delete(&self, key: &str) -> Result<(), StorageError> {
        let response = self.bucket
            .delete_object(key)
            .await
            .map_err(|e| StorageError::NotFound(e.to_string()))?;

        if response.status_code() != 204 {
            return Err(StorageError::NotFound(format!(
                "DELETE failed with status: {}",
                response.status_code()
            )));
        }
        Ok(())
    }

    fn public_url(&self, key: &str) -> Option<String> {
        // For AWS return standard format, otherwise generic
        if self.bucket.region().map(|r| r.contains("amazonaws.com")).unwrap_or(false) {
            Some(format!(
                "https://{}.s3.{}.amazonaws.com/{}",
                self.bucket.name(),
                self.bucket.region().unwrap_or("us-east-1"),
                key
            ))
        } else {
            Some(format!("{}/{}/{}", self.bucket.endpoint(), self.bucket.name(), key))
        }
    }

    fn strategy_name(&self) -> &str { "s3" }
}
