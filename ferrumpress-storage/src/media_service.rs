use std::collections::HashMap;
use std::sync::Arc;
use async_trait::async_trait;
use sqlx::{AnyPool, FromRow};
use uuid::Uuid;
use chrono::{DateTime, Utc};
use ferrumpress_core::traits::{
    StorageBackend, MediaService,
    ImageVariant, ProcessMediaTask,
    CacheProvider, TaskQueue, CacheOptions,
    ImageProcessor
};
use ferrumpress_core::models::{Media, Task};
use ferrumpress_core::error::{MediaError, StorageError};

// -----------------------------------------------------------
// Структура, которая точно повторяет столбцы таблицы `media`
// -----------------------------------------------------------
#[derive(Debug, FromRow)]
struct DbMedia {
    id: String,
    original_name: String,
    storage_strategy: String,
    storage_key: String,
    mime_type: String,
    size: i64,
    width: Option<i32>,
    height: Option<i32>,
    status: String,
    variants: Option<String>,
    created_at: String,
    updated_at: String,
}

impl TryFrom<DbMedia> for Media {
    type Error = MediaError;

    fn try_from(db: DbMedia) -> Result<Self, Self::Error> {
        Ok(Media {
            id: Uuid::parse_str(&db.id)
                .map_err(|e| MediaError::Database(e.to_string()))?,
            original_name: db.original_name,
            storage_strategy: db.storage_strategy,
            storage_key: db.storage_key,
            mime_type: db.mime_type,
            size: db.size,
            width: db.width,
            height: db.height,
            status: db.status,
            variants: db.variants,
            created_at: DateTime::parse_from_rfc3339(&db.created_at)
                .map_err(|e| MediaError::Database(e.to_string()))?
                .with_timezone(&Utc),
            updated_at: DateTime::parse_from_rfc3339(&db.updated_at)
                .map_err(|e| MediaError::Database(e.to_string()))?
                .with_timezone(&Utc),
        })
    }
}

// -----------------------------------------------------------
// Хелпер для работы с кэшем
// -----------------------------------------------------------
struct MediaCache<'a> {
    cache: &'a dyn CacheProvider,
}

impl<'a> MediaCache<'a> {
    fn key(id: Uuid) -> String {
        format!("media:{}", id)
    }

    async fn set(&self, media: &Media) {
        if let Ok(json) = serde_json::to_vec(media) {
            let opts = CacheOptions {
                ttl: Some(std::time::Duration::from_secs(300)),
                tags: vec!["media".into()],
            };
            let _ = self.cache.set(&Self::key(media.id), json, opts).await;
        }
    }

    async fn get(&self, id: Uuid) -> Option<Media> {
        self.cache
            .get(&Self::key(id))
            .await
            .ok()
            .flatten()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
    }

    async fn delete(&self, id: Uuid) {
        let _ = self.cache.delete(&Self::key(id)).await;
    }
}

// -----------------------------------------------------------
// Сервис (логика осталась прежней, но стала компактнее)
// -----------------------------------------------------------
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

    fn is_image(mime: &str) -> bool { mime.starts_with("image/") }

    fn cache_helper(&self) -> Option<MediaCache<'_>> {
        self.cache.as_deref().map(|cache| MediaCache { cache })
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
        let size = data.len() as i64;

        backend.put(&storage_key, data, mime_type).await?;

        let now = Utc::now();
        let will_convert = convert && Self::is_image(mime_type) && self.image_processor.is_some();
        let status = if will_convert { "processing" } else { "ready" };

        sqlx::query::<sqlx::Any>(
            "INSERT INTO media (id, original_name, storage_strategy, storage_key, mime_type, size, width, height, status, variants, created_at, updated_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)"
        )
        .bind(id.to_string())
        .bind(original_name)
        .bind(strategy)
        .bind(&storage_key)
        .bind(mime_type)
        .bind(size)
        .bind(None::<i32>)
        .bind(None::<i32>)
        .bind(status)
        .bind(None::<String>)
        .bind(now.to_rfc3339())
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
            size,
            width: None,
            height: None,
            status: status.into(),
            variants: None,
            created_at: now,
            updated_at: now,
        };

        if let Some(cache) = self.cache_helper() {
            cache.set(&media).await;
        }
        Ok(media)
    }

    async fn get_by_id(&self, id: Uuid) -> Result<Option<Media>, MediaError> {
        // 1. кэш
        if let Some(cache) = self.cache_helper() {
            if let Some(media) = cache.get(id).await {
                return Ok(Some(media));
            }
        }

        // 2. БД
        let db_media: Option<DbMedia> = sqlx::query_as::<_, DbMedia>(
            "SELECT * FROM media WHERE id = $1"
        )
        .bind(id.to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| MediaError::Database(e.to_string()))?;

        if let Some(db) = db_media {
            let media = Media::try_from(db)?;
            if let Some(cache) = self.cache_helper() {
                cache.set(&media).await;
            }
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
            .bind(id.to_string())
            .execute(&self.pool)
            .await
            .map_err(|e| MediaError::Database(e.to_string()))?;

        if let Some(cache) = self.cache_helper() {
            cache.delete(id).await;
        }
        Ok(())
    }
}