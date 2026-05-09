use std::collections::HashMap;
use std::sync::Arc;
use async_trait::async_trait;
use sqlx::{AnyPool, Row};
use uuid::Uuid;
use chrono::{Utc, DateTime};
use ferrumpress_core::traits::{
    StorageBackend, MediaService,
    ImageVariant, ProcessMediaTask,
    CacheProvider, TaskQueue, CacheOptions,
    ImageProcessor        // задачи – из traits
};

use ferrumpress_core::models::{Media, Task};
use ferrumpress_core::error::{MediaError, StorageError};

pub struct MediaServiceImpl {
    pool: AnyPool,
    backends: HashMap<String, Arc<dyn StorageBackend>>,
    task_queue: Arc<dyn TaskQueue>,
    cache: Option<Arc<dyn CacheProvider>>,
    image_processor: Option<Arc<dyn ImageProcessor>>,
}

impl MediaServiceImpl {
    pub fn new(
        pool: AnyPool,
        backends: HashMap<String, Arc<dyn StorageBackend>>,
        task_queue: Arc<dyn TaskQueue>,
        cache: Option<Arc<dyn CacheProvider>>,
        image_processor: Option<Arc<dyn ImageProcessor>>,
    ) -> Self {
        Self { pool, backends, task_queue, cache, image_processor }
    }

    fn cache_key(id: Uuid) -> String { format!("media:{}", id) }

    fn is_image(mime: &str) -> bool { mime.starts_with("image/") }

    async fn cache_media(&self, media: &Media) {
        if let Some(cache) = &self.cache {
            if let Ok(json) = serde_json::to_string(media) {
                let opts = CacheOptions {
                    ttl: Some(std::time::Duration::from_secs(300)),
                    tags: vec!["media".into()],
                };
                cache.set(&Self::cache_key(media.id), json.into_bytes(), opts).await.ok();
            }
        }
    }

    /// Преобразование строки БД в Media с ручным парсингом UUID и DateTime,
    /// потому что AnyPool не гарантирует поддержку этих типов напрямую.
    fn row_to_media(row: &sqlx::any::AnyRow) -> Result<Media, MediaError> {
        let id_str: String = row.try_get("id").map_err(|e| MediaError::Database(e.to_string()))?;
        let id = Uuid::parse_str(&id_str).map_err(|e| MediaError::Database(e.to_string()))?;

        let original_name: String = row.try_get("original_name").map_err(|e| MediaError::Database(e.to_string()))?;
        let storage_strategy: String = row.try_get("storage_strategy").map_err(|e| MediaError::Database(e.to_string()))?;
        let storage_key: String = row.try_get("storage_key").map_err(|e| MediaError::Database(e.to_string()))?;
        let mime_type: String = row.try_get("mime_type").map_err(|e| MediaError::Database(e.to_string()))?;
        let size: i64 = row.try_get("size").map_err(|e| MediaError::Database(e.to_string()))?;
        let width: Option<i32> = row.try_get("width").map_err(|e| MediaError::Database(e.to_string()))?;
        let height: Option<i32> = row.try_get("height").map_err(|e| MediaError::Database(e.to_string()))?;
        let status: String = row.try_get("status").map_err(|e| MediaError::Database(e.to_string()))?;
        let variants: Option<String> = row.try_get("variants").map_err(|e| MediaError::Database(e.to_string()))?;

        let created_at_str: String = row.try_get("created_at").map_err(|e| MediaError::Database(e.to_string()))?;
        let created_at = DateTime::parse_from_rfc3339(&created_at_str)
            .map_err(|e| MediaError::Database(e.to_string()))?
            .with_timezone(&Utc);

        let updated_at_str: String = row.try_get("updated_at").map_err(|e| MediaError::Database(e.to_string()))?;
        let updated_at = DateTime::parse_from_rfc3339(&updated_at_str)
            .map_err(|e| MediaError::Database(e.to_string()))?
            .with_timezone(&Utc);

        Ok(Media {
            id,
            original_name,
            storage_strategy,
            storage_key,
            mime_type,
            size,
            width,
            height,
            status,
            variants,
            created_at,
            updated_at,
        })
    }
}

#[async_trait]
impl MediaService for MediaServiceImpl {
    async fn upload(
        &self,
        original_name: &str,
        data: Vec<u8>,
        mime_type: &str,
        strategy: &str,
        convert: bool,
    ) -> Result<Media, MediaError> {
        let backend = self.backends.get(strategy)
            .ok_or_else(|| MediaError::UnknownStrategy(strategy.into()))?;
        let id = Uuid::new_v4();
        let storage_key = format!("{}/{}", id, original_name);

        backend.put(&storage_key, data.clone(), mime_type).await?;

        let now = Utc::now();
        let will_convert = convert && Self::is_image(mime_type) && self.image_processor.is_some();
        let status = if will_convert { "processing" } else { "ready" };

        sqlx::query::<sqlx::Any>(
            "INSERT INTO media (id, original_name, storage_strategy, storage_key, mime_type, size, width, height, status, variants, created_at, updated_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)"
        )
        .bind(id.to_string())                   // UUID -> String
        .bind(original_name)
        .bind(strategy)
        .bind(&storage_key)
        .bind(mime_type)
        .bind(data.len() as i64)
        .bind(None::<i32>)
        .bind(None::<i32>)
        .bind(status)
        .bind(None::<String>)
        .bind(now.to_rfc3339())                // DateTime -> String
        .bind(now.to_rfc3339())
        .execute(&self.pool)
        .await
        .map_err(|e| MediaError::Database(e.to_string()))?;

        if will_convert {
            let task = Task {
                id: Uuid::new_v4().to_string(),
                kind: "process_media".into(),
                payload: serde_json::to_vec(&ProcessMediaTask { media_id: id }).unwrap(),
                priority: 5,
                created_at: Utc::now(),
            };
            self.task_queue.push(task).await
                .map_err(|e| MediaError::Database(e.to_string()))?;
        }

        let media = Media {
            id,
            original_name: original_name.into(),
            storage_strategy: strategy.into(),
            storage_key,
            mime_type: mime_type.into(),
            size: data.len() as i64,
            width: None,
            height: None,
            status: status.into(),
            variants: None,
            created_at: now,
            updated_at: now,
        };
        self.cache_media(&media).await;
        Ok(media)
    }

    async fn get_by_id(&self, id: Uuid) -> Result<Option<Media>, MediaError> {
        if let Some(cache) = &self.cache {
            if let Some(cached) = cache.get(&Self::cache_key(id)).await.ok().flatten() {
                if let Ok(media) = serde_json::from_slice::<Media>(&cached) {
                    return Ok(Some(media));
                }
            }
        }

        let row = sqlx::query::<sqlx::Any>("SELECT * FROM media WHERE id = $1")
            .bind(id.to_string())   // UUID -> String
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| MediaError::Database(e.to_string()))?;

        if let Some(row) = row {
            let media = Self::row_to_media(&row)?;
            self.cache_media(&media).await;
            Ok(Some(media))
        } else {
            Ok(None)
        }
    }

    async fn delete(&self, id: Uuid) -> Result<(), MediaError> {
        let media = self.get_by_id(id).await?
            .ok_or(MediaError::Storage(StorageError::NotFound("media not found".into())))?;

        let backend = self.backends.get(&media.storage_strategy)
            .ok_or_else(|| MediaError::UnknownStrategy(media.storage_strategy.clone()))?;
        backend.delete(&media.storage_key).await.ok();

        if let Some(variants_json) = &media.variants {
            if let Ok(variants) = serde_json::from_str::<Vec<ImageVariant>>(variants_json) {
                for v in variants {
                    backend.delete(&v.key).await.ok();
                }
            }
        }

        sqlx::query::<sqlx::Any>("DELETE FROM media WHERE id = $1")
            .bind(id.to_string())   // UUID -> String
            .execute(&self.pool)
            .await
            .map_err(|e| MediaError::Database(e.to_string()))?;

        if let Some(cache) = &self.cache {
            cache.delete(&Self::cache_key(id)).await.ok();
        }
        Ok(())
    }
}