use async_trait::async_trait;
use uuid::Uuid;
use crate::models::RefreshTokenInfo;
use crate::error::SessionError;

#[async_trait]
pub trait SessionStore: Send + Sync {
    async fn save_refresh_token(&self, info: &RefreshTokenInfo) -> Result<(), SessionError>;
    async fn get_refresh_token(&self, jti: &str) -> Result<Option<RefreshTokenInfo>, SessionError>;
    async fn revoke_refresh_token(&self, jti: &str) -> Result<(), SessionError>;
    async fn revoke_all_for_user(&self, user_id: Uuid) -> Result<(), SessionError>;
    async fn cleanup_expired(&self) -> Result<u64, SessionError>;
}