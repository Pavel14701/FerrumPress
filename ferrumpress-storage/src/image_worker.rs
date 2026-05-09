#[cfg(feature = "image-processing")]
use std::sync::Arc;
#[cfg(feature = "image-processing")]
use sqlx::{AnyPool, Row};
#[cfg(feature = "image-processing")]
use ferrumpress_core::traits::{TaskQueue, StorageBackend, ImageVariant, ProcessMediaTask};
#[cfg(feature = "image-processing")]
use std::collections::HashMap;
#[cfg(feature = "image-processing")]
use crate::image_processor::ImageProcessor;

#[cfg(feature = "image-processing")]
pub async fn run_media_worker(
    queue: Arc<dyn TaskQueue>,
    pool: AnyPool,
    backends: HashMap<String, Arc<dyn StorageBackend>>,
    processor: Arc<dyn ImageProcessor>,
) {
    loop {
        match queue.pop(5).await {
            Ok(Some(task)) if task.kind == "process_media" => {
                if let Ok(payload) = serde_json::from_slice::<ProcessMediaTask>(&task.payload) {
                    let media_id = payload.media_id;

                    let row = sqlx::query("SELECT * FROM media WHERE id = $1")
                        .bind(media_id)
                        .fetch_optional(&pool)
                        .await;
                    let row = match row {
                        Ok(Some(r)) => r,
                        _ => {
                            queue.ack(&task.id).await.ok();
                            continue;
                        }
                    };

                    let storage_strategy: String = row.try_get("storage_strategy").unwrap_or_default();
                    let storage_key: String = row.try_get("storage_key").unwrap_or_default();
                    let mime_type: String = row.try_get("mime_type").unwrap_or_default();

                    let backend = match backends.get(&storage_strategy) {
                        Some(b) => b,
                        None => {
                            queue.ack(&task.id).await.ok();
                            continue;
                        }
                    };

                    let original_data = match backend.get(&storage_key).await {
                        Ok(d) => d,
                        Err(_) => {
                            let _ = sqlx::query("UPDATE media SET status = $1 WHERE id = $2")
                                .bind("error")
                                .bind(media_id)
                                .execute(&pool).await;
                            queue.ack(&task.id).await.ok();
                            continue;
                        }
                    };

                    match processor.process_image(original_data, &mime_type).await {
                        Ok(variants) => {
                            let mut variant_records = Vec::new();
                            for mut v in variants {
                                let variant_key = format!("{}/variant_{}", media_id, v.format);
                                // В реальности нужно сохранить сгенерированные данные
                                // Здесь для примера повторно загружаем оригинал (заглушка)
                                if let Err(_) = backend.put(&variant_key, original_data.clone(), &mime_type).await {
                                    continue;
                                }
                                v.key = variant_key;
                                variant_records.push(v);
                            }

                            let variants_json = serde_json::to_string(&variant_records).unwrap_or_default();
                            let _ = sqlx::query(
                                "UPDATE media SET status = $1, variants = $2, updated_at = $3 WHERE id = $4"
                            )
                            .bind("ready")
                            .bind(&variants_json)
                            .bind(chrono::Utc::now())
                            .bind(media_id)
                            .execute(&pool).await;
                        }
                        Err(_) => {
                            let _ = sqlx::query("UPDATE media SET status = $1 WHERE id = $2")
                                .bind("error")
                                .bind(media_id)
                                .execute(&pool).await;
                        }
                    }
                }
                queue.ack(&task.id).await.ok();
            }
            Ok(Some(_)) => {
                queue.nack(&task.id).await.ok();
            }
            Ok(None) => {}
            Err(_) => break,
        }
    }
}