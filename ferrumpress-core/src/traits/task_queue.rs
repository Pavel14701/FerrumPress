use std::pin::Pin;
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
    /// Пытается занять ключ (task_id) на время обработки.
    /// Возвращает `true`, если задача ещё не обрабатывалась, иначе `false`.
    async fn try_claim(&self, task_id: &str, ttl_secs: u64) -> Result<bool, QueueError>;

    /// Снимает занятый ключ (например, при ошибке обработки).
    async fn release(&self, task_id: &str) -> Result<(), QueueError>;
}

pub trait TaskHandler: Send + Sync {
    /// Обработать задачу. Возвращает `Ok(())` при успехе, иначе ошибку.
    fn handle(&self, task: &Task) -> Pin<Box<dyn Future<Output = Result<(), QueueError>> + Send + '_>>;

    /// Должна ли вызываться проверка идемпотентности перед `handle`.
    /// По умолчанию `false` – обработчик не требует защиты от дубликатов.
    fn is_idempotent(&self) -> bool { false }
}