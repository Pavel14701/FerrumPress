use async_trait::async_trait;
use ferrumpress_core::error::StorageError;
use ferrumpress_core::traits::StorageBackend;

pub struct S3Backend {
    bucket: String,
    region: String,
    endpoint: String,
    access_key: String,
    secret_key: String,
}

impl S3Backend {
    pub fn new(
        bucket: &str,
        region: &str,
        endpoint: &str,
        access_key: &str,
        secret_key: &str,
    ) -> Self {
        Self {
            bucket: bucket.to_string(),
            region: region.to_string(),
            endpoint: endpoint.to_string(),
            access_key: access_key.to_string(),
            secret_key: secret_key.to_string(),
        }
    }

    fn create_bucket(&self) -> Result<rust_s3::Bucket, StorageError> {
        rust_s3::Bucket::new(
            &self.bucket,
            rust_s3::Region::Custom {
                region: self.region.clone(),
                endpoint: self.endpoint.clone(),
            },
            rust_s3::Credentials::new(
                Some(&self.access_key),
                Some(&self.secret_key),
                None,
                None,
                None,
            )
            .map_err(|e| StorageError::UploadFailed(e.to_string()))?,
        )
        .map_err(|e| StorageError::UploadFailed(e.to_string()))
    }
}

#[async_trait]
impl StorageBackend for S3Backend {
    async fn put(&self, key: &str, data: Vec<u8>, content_type: &str) -> Result<(), StorageError> {
        let bucket = self.create_bucket()?;
        let response = bucket
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
        let bucket = self.create_bucket()?;
        let response = bucket
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
        let bucket = self.create_bucket()?;
        let response = bucket
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
        // Для AWS отдаём стандартный формат, иначе общий
        if self.endpoint.contains("amazonaws.com") {
            Some(format!(
                "https://{}.s3.{}.amazonaws.com/{}",
                self.bucket, self.region, key
            ))
        } else {
            Some(format!("{}/{}/{}", self.endpoint, self.bucket, key))
        }
    }

    fn strategy_name(&self) -> &str { "s3" }
}