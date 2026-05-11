use async_trait::async_trait;
use redis::aio::ConnectionManager;
use std::sync::Arc;
use tokio::sync::Mutex;
use ferrumpress_core::error::QueueError;
use crate::{DeliverySemantics, Task, TaskQueue};

pub struct RedisQueue {
    conn: Arc<Mutex<ConnectionManager>>,
    source_queue: String,
    processing_queue: String,
    semantics: DeliverySemantics,
}

impl RedisQueue {
    pub async fn new(
        redis_url: &str,
        source: &str,
        processing: &str,
        semantics: DeliverySemantics,
    ) -> Result<Self, QueueError> {
        let client = redis::Client::open(redis_url)
            .map_err(|e| QueueError::Unknown(format!("redis: {}", e)))?;
        let conn = client.get_tokio_connection_manager()
            .map_err(|e| QueueError::Connection)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            source_queue: source.to_string(),
            processing_queue: processing.to_string(),
            semantics,
        })
    }
}

#[async_trait]
impl TaskQueue for RedisQueue {
    async fn push(&self, task: Task) -> Result<(), QueueError> {
        let payload = serde_json::to_string(&task)
            .map_err(|e| QueueError::Serialization(e.to_string()))?;
        let mut conn = self.conn.lock().await;
        let _: () = conn.lpush(&self.source_queue, payload)
            .await
            .map_err(|e| QueueError::Unknown(e.to_string()))?;
        Ok(())
    }

    async fn pop(&self, timeout_secs: u64) -> Result<Option<Task>, QueueError> {
        let mut conn = self.conn.lock().await;

        let result: Option<String> = if self.semantics == DeliverySemantics::AtMostOnce {
            redis::cmd("RPOP")
                .arg(&self.source_queue)
                .query_async(&mut *conn)
                .await
                .map_err(|e| QueueError::Unknown(e.to_string()))?
        } else {
            redis::cmd("BRPOPLPUSH")
                .arg(&self.source_queue)
                .arg(&self.processing_queue)
                .arg(timeout_secs as f64)
                .query_async(&mut *conn)
                .await
                .map_err(|e| QueueError::Unknown(e.to_string()))?
        };

        match result {
            Some(payload) => {
                let task: Task = serde_json::from_str(&payload)
                    .map_err(|e| QueueError::Serialization(e.to_string()))?;
                Ok(Some(task))
            }
            None => Ok(None),
        }
    }

    async fn ack(&self, task_id: &str) -> Result<(), QueueError> {
        if self.semantics == DeliverySemantics::AtMostOnce {
            return Ok(());
        }
        let mut conn = self.conn.lock().await;
        // Remove from processing queue by the serialized payload
        // We need to search by task_id which is in the JSON payload
        // Build the task object to get the exact payload
        let task = Task {
            id: task_id.to_string(),
            kind: String::new(),
            payload: Vec::new(),
            priority: 0,
            created_at: chrono::Utc::now(),
        };
        let payload = serde_json::to_string(&task)
            .map_err(|e| QueueError::Serialization(e.to_string()))?;
        let _: () = conn.lrem(&self.processing_queue, 0, payload)
            .await
            .map_err(|e| QueueError::Unknown(e.to_string()))?;
        Ok(())
    }

    async fn nack(&self, task_id: &str) -> Result<(), QueueError> {
        if self.semantics == DeliverySemantics::AtMostOnce {
            return Ok(());
        }
        let mut conn = self.conn.lock().await;
        let task = Task {
            id: task_id.to_string(),
            kind: String::new(),
            payload: Vec::new(),
            priority: 0,
            created_at: chrono::Utc::now(),
        };
        let payload = serde_json::to_string(&task)
            .map_err(|e| QueueError::Serialization(e.to_string()))?;
        let _: () = conn.lrem(&self.processing_queue, 0, payload)
            .await
            .map_err(|e| QueueError::Unknown(e.to_string()))?;
        // Also push back to source queue
        let _: () = conn.lpush(&self.source_queue, payload)
            .await
            .map_err(|e| QueueError::Unknown(e.to_string()))?;
        Ok(())
    }
}
