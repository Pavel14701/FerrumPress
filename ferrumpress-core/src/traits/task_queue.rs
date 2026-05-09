// traits/task_queue.rs
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use crate::error::QueueError;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub id: String,
    pub kind: String,
    pub payload: Vec<u8>,
    pub priority: u8,
    pub created_at: DateTime<Utc>,
}

#[async_trait]
pub trait TaskQueue: Send + Sync {
    async fn push(&self, task: Task) -> Result<(), QueueError>;
    async fn pop(&self, timeout_secs: u64) -> Result<Option<Task>, QueueError>;
    async fn ack(&self, task_id: &str) -> Result<(), QueueError>;
    async fn nack(&self, task_id: &str) -> Result<(), QueueError>;
}