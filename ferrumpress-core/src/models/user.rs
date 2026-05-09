// ferrumpress-core/src/models/user.rs
use uuid::Uuid;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use super::role::{Role};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub id: Uuid,
    pub login: String,
    pub email: String,
    pub password_hash: Option<String>,          // None, если используется Dilithium
    pub dilithium_public_key: Option<String>,   // PEM-строка публичного ключа Dilithium
    pub role: Role,
    pub is_superuser: bool,
    pub created_at: DateTime<Utc>,
}