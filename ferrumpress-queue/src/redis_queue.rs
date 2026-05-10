use async_trait::async_trait;
use redis::{AsyncCommands, Client};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use ferrumpress_core::error::QueueError;
use crate::{DeliverySemantics, Task, TaskQueue};

pub struct RedisQueue {
    client: Client,
    source_queue: String,
    processing_queue: String,
    semantics: DeliverySemantics,
    // Хранит task_id -> сырой JSON (для точного удаления из processing_queue)
    pending: Arc<Mutex<HashMap<String, String>>>,
}

impl RedisQueue {
    pub async fn new(
        redis_url: &str,
        source: &str,
        processing: &str,
        semantics: DeliverySemantics,
    ) -> Result<Self, QueueError> {
        let client = Client::open(redis_url)
            .map_err(|e| QueueError::Internal(format!("redis: {}", e)))?;
        Ok(Self {
            client,
            source_queue: source.to_string(),
            processing_queue: processing.to_string(),
            semantics,
            pending: Arc::new(Mutex::new(HashMap::new())),
        })
    }
}

#[async_trait]
impl TaskQueue for RedisQueue {
    async fn push(&self, task: Task) -> Result<(), QueueError> {
        let payload = serde_json::to_string(&task)
            .map_err(|e| QueueError::Serialization(e.to_string()))?;
        let mut conn = self.client.get_tokio_connection()
            .await
            .map_err(|e| QueueError::Internal(format!("redis conn: {}", e)))?;
        let _: () = conn.lpush(&self.source_queue, payload)
            .await
            .map_err(|e| QueueError::Internal(e.to_string()))?;
        Ok(())
    }

    async fn pop(&self, timeout_secs: u64) -> Result<Option<Task>, QueueError> {
        let mut conn = self.client.get_tokio_connection()
            .await
            .map_err(|e| QueueError::Internal(format!("redis conn: {}", e)))?;

        let result: Option<String> = if self.semantics == DeliverySemantics::AtMostOnce {
            redis::cmd("RPOP")
                .arg(&self.source_queue)
                .query_async(&mut conn)
                .await
                .map_err(|e| QueueError::Internal(e.to_string()))?
        } else {
            redis::cmd("BRPOPLPUSH")
                .arg(&self.source_queue)
                .arg(&self.processing_queue)
                .arg(timeout_secs as f64)
                .query_async(&mut conn)
                .await
                .map_err(|e| QueueError::Internal(e.to_string()))?
        };

        match result {
            Some(payload) => {
                let task: Task = serde_json::from_str(&payload)
                    .map_err(|e| QueueError::Serialization(e.to_string()))?;
                if self.semantics != DeliverySemantics::AtMostOnce {
                    let mut pending = self.pending.lock().await;
                    pending.insert(task.id.clone(), payload);
                }
                Ok(Some(task))
            }
            None => Ok(None),
        }
    }

    async fn ack(&self, task_id: &str) -> Result<(), QueueError> {
        if self.semantics == DeliverySemantics::AtMostOnce {
            return Ok(());
        }
        let payload = {
            let mut pending = self.pending.lock().await;
            pending.remove(task_id)
        };
        if let Some(payload) = payload {
            let mut conn = self.client.get_tokio_connection()
                .await
                .map_err(|e| QueueError::Internal(format!("redis conn: {}", e)))?;
            // Удаляем точное значение из processing_queue
            let _: () = conn.lrem(&self.processing_queue, 1, payload)
                .await
                .map_err(|e| QueueError::Internal(e.to_string()))?;
        }
        Ok(())
    }

    async fn nack(&self, task_id: &str) -> Result<(), QueueError> {
        if self.semantics == DeliverySemantics::AtMostOnce {
            return Ok(());
        }
        let payload = {
            let mut pending = self.pending.lock().await;
            pending.remove(task_id)
        };
        if let Some(payload) = payload {
            let mut conn = self.client.get_tokio_connection()
                .await
                .map_err(|e| QueueError::Internal(format!("redis conn: {}", e)))?;
            // Возвращаем задачу в source_queue и удаляем из processing
            let _: () = conn.lpush(&self.source_queue, &payload)
                .await
                .map_err(|e| QueueError::Internal(e.to_string()))?;
            let _: () = conn.lrem(&self.processing_queue, 1, &payload)
                .await
                .map_err(|e| QueueError::Internal(e.to_string()))?;
        }
        Ok(())
    }
}