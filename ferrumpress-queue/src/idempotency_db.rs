use async_trait::async_trait;
use sqlx::AnyPool;
use chrono::{Utc, Duration};
use ferrumpress_core::traits::IdempotencyStore;
use ferrumpress_core::error::QueueError;

pub struct DatabaseIdempotencyStore {
    pool: AnyPool,
}

impl DatabaseIdempotencyStore {
    pub fn new(pool: AnyPool) -> Self { Self { pool } }
}

#[async_trait]
impl IdempotencyStore for DatabaseIdempotencyStore {
    async fn try_claim(&self, task_id: &str, ttl_secs: u64) -> Result<bool, QueueError> {
        let expires_at = Utc::now() + Duration::seconds(ttl_secs as i64);
        let result = sqlx::query::<sqlx::Any>(
            "INSERT INTO task_locks (task_id, expires_at) VALUES ($1, $2)
             ON CONFLICT (task_id) DO NOTHING"
        )
        .bind(task_id)
        .bind(expires_at.to_rfc3339())
        .execute(&self.pool)
        .await
        .map_err(|e| QueueError::Internal(format!("DB claim: {}", e)))?;
        Ok(result.rows_affected() > 0)
    }

    async fn release(&self, task_id: &str) -> Result<(), QueueError> {
        sqlx::query::<sqlx::Any>("DELETE FROM task_locks WHERE task_id = $1")
            .bind(task_id)
            .execute(&self.pool)
            .await
            .map_err(|e| QueueError::Internal(format!("DB release: {}", e)))?;
        Ok(())
    }
}