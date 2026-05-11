use std::pin::Pin;
use std::sync::Arc;
use async_trait::async_trait;
use sqlx::{AnyPool, Row};
use ferrumpress_core::traits::{
    TaskQueue, IdempotencyStore, TaskHandler,
    StorageBackend, ImageVariant, ProcessMediaTask, ImageProcessor,
};
use ferrumpress_core::models::Task;
use ferrumpress_core::error::QueueError;
use std::collections::HashMap;
use ferrumpress_core::traits::ProcessedVariant;
use tokio::time::{sleep, Duration};

// Exponential backoff configuration
const INITIAL_BACKOFF_SECS: u64 = 2;
const MAX_BACKOFF_SECS: u64 = 60;
const MAX_RETRIES: u32 = 5;

struct MediaRow {
    id: uuid::Uuid,
    storage_strategy: String,
    storage_key: String,
    mime_type: String,
}

async fn fetch_media_row(pool: &AnyPool, media_id: uuid::Uuid) -> Option<MediaRow> {
    let row = sqlx::query::<sqlx::Any>("SELECT * FROM media WHERE id = $1")
        .bind(media_id.to_string())
        .fetch_optional(pool)
        .await
        .ok()?;

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

async fn fetch_original_data(
    backends: &HashMap<String, Arc<dyn StorageBackend>>,
    pool: &AnyPool,
    media: &MediaRow,
) -> Option<Vec<u8>> {
    let backend = backends.get(&media.storage_strategy)?;
    match backend.get(&media.storage_key).await {
        Ok(data) => Some(data),
        Err(e) => {
            tracing::warn!("Failed to fetch original data: {}", e);
            let _ = sqlx::query::<sqlx::Any>("UPDATE media SET status = $1 WHERE id = $2")
                .bind("error")
                .bind(media.id.to_string())
                .execute(pool)
                .await;
            None
        }
    }
}

async fn process_and_update_media(
    processor: &dyn ImageProcessor,
    backend: &dyn StorageBackend,
    pool: &AnyPool,
    media: &MediaRow,
    original_data: Vec<u8>,
) -> Result<(), QueueError> {
    let variants = processor.process_image(original_data, &media.mime_type).await
        .map_err(|e| QueueError::Unknown(format!("image processing failed: {}", e)))?;
    let mut variant_records = Vec::new();

    for v in variants {
        let variant_key = format!("{}/variant_{}", media.id, v.meta.format);
        let put_result = backend.put(&variant_key, v.data, &format!("image/{}", v.meta.format)).await;
        if let Err(e) = put_result {
            tracing::error!("Failed to put variant {}: {}", variant_key, e);
            continue;
        }
        variant_records.push(ImageVariant {
            format: v.meta.format.clone(),
            key: variant_key,
            size: v.meta.size,
            width: v.meta.width,
            height: v.meta.height,
        });
    }

    let variants_json = serde_json::to_string(&variant_records)
        .map_err(|e| QueueError::Serialization(format!("failed to serialize variants: {}", e)))?;
    let now = chrono::Utc::now().to_rfc3339();
    sqlx::query::<sqlx::Any>(
        "UPDATE media SET status = $1, variants = $2, updated_at = $3 WHERE id = $4"
    )
    .bind("ready")
    .bind(&variants_json)
    .bind(now)
    .bind(media.id.to_string())
    .execute(pool)
    .await
    .map_err(|e| QueueError::Unknown(format!("DB update failed: {}", e)))?;

    Ok(())
}

// ----- Основной обработчик -----
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
    async fn handle(&self, task: &Task) -> Result<(), QueueError> {
        let payload: ProcessMediaTask = serde_json::from_slice(&task.payload)
            .map_err(|e| QueueError::Serialization(e.to_string()))?;
        let media_id = payload.media_id;

        let media = fetch_media_row(&self.pool, media_id)
            .ok_or_else(|| QueueError::Unknown("media not found".into()))?;

        let backend = self.backends.get(&media.storage_strategy)
            .ok_or_else(|| QueueError::Unknown("unknown storage strategy".into()))?;

        let original = fetch_original_data(&self.backends, &self.pool, &media)
            .ok_or_else(|| QueueError::Unknown("failed to fetch original data".into()))?;

        process_and_update_media(
            self.processor.as_ref(),
            backend.as_ref(),
            &self.pool,
            &media,
            original,
        )
        .await?;

        Ok(())
    }

    fn is_idempotent(&self) -> bool { false }
}

// ----- Основной цикл с exponential backoff -----
pub async fn run_media_worker(
    queue: Arc<dyn TaskQueue>,
    pool: AnyPool,
    backends: HashMap<String, Arc<dyn StorageBackend>>,
    processor: Arc<dyn ImageProcessor>,
    idempotency: Option<Arc<dyn IdempotencyStore>>,
) {
    let handler = Arc::new(MediaTaskWorker::new(pool, backends, processor));

    let mut retry_count = 0;

    loop {
        let result = queue.pop(5).await;
        match result {
            Ok(Some(task)) => {
                retry_count = 0;
                if task.kind != "process_media" {
                    let _ = queue.nack(&task.id).await;
                    continue;
                }

                if let Some(ref idem) = idempotency {
                    match idem.try_claim(&task.id, 300).await {
                        Ok(false) => {
                            let _ = queue.ack(&task.id).await;
                            continue;
                        }
                        Err(e) => {
                            tracing::error!("Idempotency claim failed: {}", e);
                            let _ = queue.nack(&task.id).await;
                            continue;
                        }
                        _ => {}
                    }
                }

                match handler.handle(&task).await {
                    Ok(()) => {
                        let _ = queue.ack(&task.id).await;
                        if let Some(ref idem) = idempotency {
                            let _ = idem.release(&task.id).await;
                        }
                    }
                    Err(e) => {
                        tracing::error!("Task handling failed: {}", e);
                        let _ = queue.nack(&task.id).await;
                        if let Some(ref idem) = idempotency {
                            let _ = idem.release(&task.id).await;
                        }
                    }
                }
            }
            Ok(None) => {
                // Queue is empty, brief pause before next poll
                sleep(Duration::from_millis(100)).await;
            }
            Err(e) => {
                tracing::error!("queue pop error: {}", e);
                retry_count += 1;
                let backoff = INITIAL_BACKOFF_SECS * 2_u64.pow(retry_count.min(MAX_RETRIES) as u32)
                    .min(MAX_BACKOFF_SECS);
                tracing::warn!("retrying in {}s (attempt {})", backoff, retry_count);
                sleep(Duration::from_secs(backoff)).await;
            }
        }
    }
}
