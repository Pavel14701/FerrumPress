use async_trait::async_trait;
use uuid::Uuid;
use crate::{error::MediaError, models::Media};


/// Сервис управления медиафайлами
#[async_trait]
pub trait MediaService: Send + Sync {
    /// Загрузить файл. Если convert == true и это изображение, обработка пойдёт асинхронно.
    async fn upload(&self, original_name: &str, data: Vec<u8>, mime_type: &str, strategy: &str, convert: bool) -> Result<Media, MediaError>;
    /// Получить метаданные по ID
    async fn get_by_id(&self, id: Uuid) -> Result<Option<Media>, MediaError>;
    /// Удалить файл и все его варианты
    async fn delete(&self, id: Uuid) -> Result<(), MediaError>;
}