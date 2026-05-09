#[cfg(feature = "image-processing")]
use std::sync::Arc;
#[cfg(feature = "image-processing")]
use sqlx::{AnyPool, Row};
#[cfg(feature = "image-processing")]
use ferrumpress_core::traits::{TaskQueue, StorageBackend, ImageVariant, ProcessMediaTask, ImageProcessor};
#[cfg(feature = "image-processing")]
use std::collections::HashMap;
#[cfg(feature = "image-processing")]
use chrono::Utc;

#[cfg(feature = "image-processing")]
pub async fn run_media_worker(
    queue: Arc<dyn TaskQueue>,
    pool: AnyPool,
    backends: HashMap<String, Arc<dyn StorageBackend>>,
    processor: Arc<dyn ImageProcessor>,
) {
    loop {
        match queue.pop(5).await {
            Ok(Some(task)) => {
                if task.kind == "process_media" {
                    if let Ok(payload) = serde_json::from_slice::<ProcessMediaTask>(&task.payload) {
                        let media_id = payload.media_id;
                        process_single_media(&queue, &pool, &backends, &processor, task.id, media_id).await;
                    } else {
                        ack_task(queue.as_ref(), &task.id).await;
                    }
                } else {
                    nack_task(queue.as_ref(), &task.id).await;
                }
            }
            Ok(None) => {}
            Err(_) => break,
        }
    }
}

#[cfg(feature = "image-processing")]
async fn process_single_media(
    queue: &Arc<dyn TaskQueue>,
    pool: &AnyPool,
    backends: &HashMap<String, Arc<dyn StorageBackend>>,
    processor: &Arc<dyn ImageProcessor>,
    task_id: String,
    media_id: uuid::Uuid,
) {
    let row = sqlx::query::<sqlx::Any>("SELECT * FROM media WHERE id = $1")
        .bind(media_id.to_string())
        .fetch_optional(pool)
        .await;
    let row = match row {
        Ok(Some(r)) => r,
        _ => {
            ack_task(queue.as_ref(), &task_id).await;
            return;
        }
    };

    let storage_strategy: String = row.try_get("storage_strategy").unwrap_or_default();
    let storage_key: String = row.try_get("storage_key").unwrap_or_default();
    let mime_type: String = row.try_get("mime_type").unwrap_or_default();

    let backend = match backends.get(&storage_strategy) {
        Some(b) => b,
        None => {
            ack_task(queue.as_ref(), &task_id).await;
            return;
        }
    };

    let original_data = match backend.get(&storage_key).await {
        Ok(d) => d,
        Err(_) => {
            let _ = sqlx::query::<sqlx::Any>("UPDATE media SET status = $1 WHERE id = $2")
                .bind("error")
                .bind(media_id.to_string())
                .execute(pool)
                .await;
            ack_task(queue.as_ref(), &task_id).await;
            return;
        }
    };

    match processor.process_image(original_data.clone(), &mime_type).await {
        Ok(variants) => {
            let mut variant_records = Vec::new();
            for mut v in variants {
                let variant_key = format!("{}/variant_{}", media_id, v.format);
                if backend.put(&variant_key, original_data.clone(), &mime_type).await.is_err() {
                    continue;
                }
                v.key = variant_key;
                variant_records.push(v);
            }

            let variants_json = serde_json::to_string(&variant_records).unwrap_or_default();
            let _ = sqlx::query::<sqlx::Any>(
                "UPDATE media SET status = $1, variants = $2, updated_at = $3 WHERE id = $4"
            )
            .bind("ready")
            .bind(&variants_json)
            .bind(Utc::now().to_rfc3339())
            .bind(media_id.to_string())
            .execute(pool)
            .await;
        }
        Err(_) => {
            let _ = sqlx::query::<sqlx::Any>("UPDATE media SET status = $1 WHERE id = $2")
                .bind("error")
                .bind(media_id.to_string())
                .execute(pool)
                .await;
        }
    }
    ack_task(queue.as_ref(), &task_id).await;
}

#[cfg(feature = "image-processing")]
async fn ack_task(queue: &dyn TaskQueue, task_id: &str) {
    let _ = queue.ack(task_id).await;
}

#[cfg(feature = "image-processing")]
async fn nack_task(queue: &dyn TaskQueue, task_id: &str) {
    let _ = queue.nack(task_id).await;
}