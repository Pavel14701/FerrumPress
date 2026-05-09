use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Метаданные файла
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Media {
    pub id: Uuid,
    pub original_name: String,
    pub storage_strategy: String,
    pub storage_key: String,
    pub mime_type: String,
    pub size: i64,
    pub width: Option<i32>,
    pub height: Option<i32>,
    pub status: String,
    pub variants: Option<String>,   // JSON-массив Vec<ImageVariant>
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}
