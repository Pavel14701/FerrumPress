use async_trait::async_trait;
use crate::error::QueueError;
use crate::models::Task;

#[async_trait]
pub trait TaskQueue: Send + Sync {
    async fn push(&self, task: Task) -> Result<(), QueueError>;
    async fn pop(&self, timeout_secs: u64) -> Result<Option<Task>, QueueError>;
    async fn ack(&self, task_id: &str) -> Result<(), QueueError>;
    async fn nack(&self, task_id: &str) -> Result<(), QueueError>;
}

#[async_trait]
pub trait IdempotencyStore: Send + Sync {
    async fn try_claim(&self, task_id: &str, ttl_secs: u64) -> Result<bool, QueueError>;
    async fn release(&self, task_id: &str) -> Result<(), QueueError>;
}

#[async_trait]
pub trait TaskHandler: Send + Sync {
    async fn handle(&self, task: &Task) -> Result<(), QueueError>;
    fn is_idempotent(&self) -> bool { false }
}