use async_trait::async_trait;
use redis::aio::ConnectionManager;
use std::sync::Arc;
use tokio::sync::Mutex;
use ferrumpress_core::traits::IdempotencyStore;
use ferrumpress_core::error::QueueError;

pub struct RedisIdempotencyStore {
    conn: Arc<Mutex<ConnectionManager>>,
    key_prefix: String,
}

impl RedisIdempotencyStore {
    pub fn new(redis_url: &str, prefix: &str) -> Result<Self, QueueError> {
        let client = redis::Client::open(redis_url)
            .map_err(|e| QueueError::Unknown(format!("redis: {}", e)))?;
        let conn = client.get_tokio_connection_manager()
            .map_err(|e| QueueError::Unknown(format!("redis conn: {}", e)))?;
        Ok(Self { conn: Arc::new(Mutex::new(conn)), key_prefix: prefix.to_string() })
    }
}

#[async_trait]
impl IdempotencyStore for RedisIdempotencyStore {
    async fn try_claim(&self, task_id: &str, ttl_secs: u64) -> Result<bool, QueueError> {
        let key = format!("{}:{}", self.key_prefix, task_id);
        let mut conn = self.conn.lock().await;
        // SET NX EX returns "OK" on success, nil on failure
        let resp: Option<String> = redis::cmd("SET")
            .arg(&key)
            .arg("1")
            .arg("NX")
            .arg("EX")
            .arg(ttl_secs)
            .query_async(&mut *conn)
            .await
            .map_err(|e| QueueError::Unknown(format!("SET: {}", e)))?;
        Ok(resp.is_some())
    }

    async fn release(&self, task_id: &str) -> Result<(), QueueError> {
        let key = format!("{}:{}", self.key_prefix, task_id);
        let mut conn = self.conn.lock().await;
        let _: () = conn.del(&key).await
            .map_err(|e| QueueError::Unknown(format!("DEL: {}", e)))?;
        Ok(())
    }
}
