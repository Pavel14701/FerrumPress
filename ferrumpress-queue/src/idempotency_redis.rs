use async_trait::async_trait;
use redis::{AsyncCommands, Client};
use ferrumpress_core::traits::IdempotencyStore;
use ferrumpress_core::error::QueueError;

pub struct RedisIdempotencyStore {
    client: Client,
    key_prefix: String,
}

impl RedisIdempotencyStore {
    pub fn new(redis_url: &str, prefix: &str) -> Result<Self, QueueError> {
        let client = Client::open(redis_url)
            .map_err(|e| QueueError::Internal(format!("redis: {}", e)))?;
        Ok(Self { client, key_prefix: prefix.to_string() })
    }
}

#[async_trait]
impl IdempotencyStore for RedisIdempotencyStore {
    async fn try_claim(&self, task_id: &str, ttl_secs: u64) -> Result<bool, QueueError> {
        let mut conn = self.client.get_tokio_connection()
            .await
            .map_err(|e| QueueError::Internal(format!("redis conn: {}", e)))?;
        let key = format!("{}:{}", self.key_prefix, task_id);
        let acquired: bool = redis::cmd("SET")
            .arg(&key)
            .arg("1")
            .arg("NX")
            .arg("EX")
            .arg(ttl_secs)
            .query_async(&mut conn)
            .await
            .map_err(|e| QueueError::Internal(format!("SET: {}", e)))?;
        Ok(acquired)
    }

    async fn release(&self, task_id: &str) -> Result<(), QueueError> {
        let mut conn = self.client.get_tokio_connection()
            .await
            .map_err(|e| QueueError::Internal(format!("redis conn: {}", e)))?;
        let key = format!("{}:{}", self.key_prefix, task_id);
        let _: () = conn.del(&key).await.map_err(|e| QueueError::Internal(format!("DEL: {}", e)))?;
        Ok(())
    }
}