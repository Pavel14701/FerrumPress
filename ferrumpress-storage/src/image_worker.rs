use std::pin::Pin;
use std::sync::Arc;
use async_trait::async_trait;
use sqlx::{AnyPool, Row};
use ferrumpress_core::traits::{
    TaskQueue, IdempotencyStore, TaskHandler, Task,
    StorageBackend, ImageVariant, ProcessMediaTask, ImageProcessor
};
use ferrumpress_core::error::QueueError;
use std::collections::HashMap;

// ----- Вспомогательные функции для ack/nack -----
async fn ack_task(queue: &dyn TaskQueue, task_id: &str) {
    let _ = queue.ack(task_id).await;
}

async fn nack_task(queue: &dyn TaskQueue, task_id: &str) {
    let _ = queue.nack(task_id).await;
}

// ----- Структура с метаданными из БД -----
struct MediaRow {
    id: uuid::Uuid,
    storage_strategy: String,
    storage_key: String,
    mime_type: String,
}

/// Загрузка строки media по ID. Если нет – возвращает None.
async fn fetch_media_row(pool: &AnyPool, media_id: uuid::Uuid) -> Option<MediaRow> {
    let row = sqlx::query::<sqlx::Any>("SELECT * FROM media WHERE id = $1")
        .bind(media_id.to_string())
        .fetch_optional(pool)
        .await
        .ok()??;

    let id: uuid::Uuid = {
        let s: String = row.try_get("id").unwrap_or_default();
        uuid::Uuid::parse_str(&s).ok()?
    };
    Some(MediaRow {
        id,
        storage_strategy: row.try_get("storage_strategy").unwrap_or_default(),
        storage_key: row.try_get("storage_key").unwrap_or_default(),
        mime_type: row.try_get("mime_type").unwrap_or_default(),
    })
}

/// Загрузить оригинальный файл из хранилища. В случае ошибки обновляет статус на "error" и возвращает None.
async fn fetch_original_data(
    backends: &HashMap<String, Arc<dyn StorageBackend>>,
    pool: &AnyPool,
    media: &MediaRow,
) -> Option<Vec<u8>> {
    let backend = backends.get(&media.storage_strategy)?;

    match backend.get(&media.storage_key).await {
        Ok(data) => Some(data),
        Err(_) => {
            let _ = sqlx::query::<sqlx::Any>("UPDATE media SET status = $1 WHERE id = $2")
                .bind("error")
                .bind(media.id.to_string())
                .execute(pool)
                .await;
            None
        }
    }
}

/// Обработка изображения: конвертация, сохранение вариантов, обновление БД.
async fn process_and_update_media(
    processor: &dyn ImageProcessor,
    backend: &dyn StorageBackend,
    pool: &AnyPool,
    media: &MediaRow,
    original_data: Vec<u8>,
) -> Result<(), ()> {
    let variants = processor.process_image(original_data.clone(), &media.mime_type).await.map_err(|_| ())?;
    let mut variant_records = Vec::new();

    for mut v in variants {
        let variant_key = format!("{}/variant_{}", media.id, v.format);
        if backend.put(&variant_key, original_data.clone(), &media.mime_type).await.is_ok() {
            v.key = variant_key;
            variant_records.push(v);
        }
    }

    let variants_json = serde_json::to_string(&variant_records).unwrap_or_default();
    let now = chrono::Utc::now().to_rfc3339();
    let _ = sqlx::query::<sqlx::Any>(
        "UPDATE media SET status = $1, variants = $2, updated_at = $3 WHERE id = $4"
    )
    .bind("ready")
    .bind(&variants_json)
    .bind(now)
    .bind(media.id.to_string())
    .execute(pool)
    .await;

    Ok(())
}

// ----- Основной обработчик задачи -----
pub struct MediaTaskWorker {
    pool: AnyPool,
    backends: HashMap<String, Arc<dyn StorageBackend>>,
    processor: Arc<dyn ImageProcessor>,
}

impl MediaTaskWorker {
    pub fn new(
        pool: AnyPool,
        backends: HashMap<String, Arc<dyn StorageBackend>>,
        processor: Arc<dyn ImageProcessor>,
    ) -> Self {
        Self { pool, backends, processor }
    }
}

#[async_trait]
impl TaskHandler for MediaTaskWorker {
    fn handle(&self, task: &Task) -> Pin<Box<dyn Future<Output = Result<(), QueueError>> + Send + '_>> {
        let pool = self.pool.clone();
        let backends = self.backends.clone();
        let processor = self.processor.clone();
        let task = task.clone();

        Box::pin(async move {
            let payload: ProcessMediaTask = serde_json::from_slice(&task.payload)
                .map_err(|e| QueueError::Serialization(e.to_string()))?;
            let media_id = payload.media_id;

            // 1. Получить метаданные
            let media = fetch_media_row(&pool, media_id).await
                .ok_or_else(|| QueueError::Internal("media not found".into()))?;

            // 2. Получить бэкенд
            let backend = backends.get(&media.storage_strategy)
                .ok_or_else(|| QueueError::Internal("unknown storage strategy".into()))?;

            // 3. Получить оригинальные данные
            let original = fetch_original_data(&backends, &pool, &media).await
                .ok_or_else(|| QueueError::Internal("failed to fetch original data".into()))?;

            // 4. Обработать и обновить
            process_and_update_media(
                self.processor.as_ref(),
                (*backend).as_ref(),
                &pool,
                &media,
                original,
            ).await.map_err(|_| QueueError::Internal("image processing failed".into()))?;

            Ok(())
        })
    }

    fn is_idempotent(&self) -> bool { false } // идемпотентность управляется снаружи
}

// ----- Основной цикл -----
pub async fn run_media_worker(
    queue: Arc<dyn TaskQueue>,
    pool: AnyPool,
    backends: HashMap<String, Arc<dyn StorageBackend>>,
    processor: Arc<dyn ImageProcessor>,
    idempotency: Option<Arc<dyn IdempotencyStore>>,
) {
    let handler = Arc::new(MediaTaskWorker::new(pool, backends, processor));

    loop {
        match queue.pop(5).await {
            Ok(Some(task)) => {
                if task.kind != "process_media" {
                    nack_task(queue.as_ref(), &task.id).await;
                    continue;
                }

                // Идемпотентность (для Redis/RabbitMQ, для Kafka передают None)
                if let Some(ref idem) = idempotency {
                    match idem.try_claim(&task.id, 300).await {
                        Ok(false) => {
                            // Уже выполняется – подтверждаем и пропускаем
                            ack_task(queue.as_ref(), &task.id).await;
                            continue;
                        }
                        Err(_) => {
                            nack_task(queue.as_ref(), &task.id).await;
                            continue;
                        }
                        _ => {} // true – продолжаем
                    }
                }

                // Выполняем задачу
                match handler.handle(&task).await {
                    Ok(()) => {
                        ack_task(queue.as_ref(), &task.id).await;
                        // Освобождаем идемпотентный ключ (если был)
                        if let Some(ref idem) = idempotency {
                            let _ = idem.release(&task.id).await;
                        }
                    }
                    Err(_) => {
                        nack_task(queue.as_ref(), &task.id).await;
                        // Освобождаем ключ, чтобы задачу можно было повторить
                        if let Some(ref idem) = idempotency {
                            let _ = idem.release(&task.id).await;
                        }
                    }
                }
            }
            Ok(None) => {}
            Err(_) => break,
        }
    }
}